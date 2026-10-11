use {
    super::{
        PlannerError,
        expr::{try_visit_expr, try_visit_window, visit_mut_expr},
    },
    crate::{
        plan::{
            AggregationInputPlan, DistinctInputPlan, DistinctPlan, ExprPlan, FilterInputPlan,
            FilterPlan, HashJoinInputPlan, HashJoinPlan, InnerJoinInputPlan, InnerJoinPlan,
            JoinConditionInputPlan, JoinConditionPlan, LeftOuterJoinInputPlan, LeftOuterJoinPlan,
            LimitInputPlan, LimitPlan, NestedLoopJoinInputPlan, NestedLoopJoinPlan,
            OffsetInputPlan, OffsetPlan, OrderByExprPlan, ProjectInputPlan, ProjectPlan,
            ProjectionPlan, QueryPlan, SelectItemPlan, SelectOrderByPlan, SourcePlan,
            StatementPlan, ValuesOrderByPlan, ValuesPlan, WindowExprPlan, WindowFunctionPlan,
            WindowInputPlan, WindowPlan,
        },
        result::Result,
    },
    std::collections::HashMap,
};

/// Binds window function slots and validates window function placement.
///
/// # Errors
///
/// Returns an error when a window function appears outside the `SELECT`
/// projection, is combined with `GROUP BY`/`HAVING`/`DISTINCT`, or is nested
/// inside an aggregate or another window function.
pub fn plan(statement: StatementPlan) -> Result<StatementPlan> {
    match statement {
        StatementPlan::Query(mut query) => {
            plan_query(&mut query)?;
            Ok(StatementPlan::Query(query))
        }
        StatementPlan::Insert {
            table_name,
            columns,
            mut source,
            table_columns,
        } => {
            plan_query(&mut source)?;
            Ok(StatementPlan::Insert {
                table_name,
                columns,
                source,
                table_columns,
            })
        }
        StatementPlan::CreateTable {
            if_not_exists,
            name,
            columns,
            mut source,
            engine,
            foreign_keys,
            comment,
        } => {
            if let Some(source) = source.as_mut() {
                plan_query(source)?;
            }

            Ok(StatementPlan::CreateTable {
                if_not_exists,
                name,
                columns,
                source,
                engine,
                foreign_keys,
                comment,
            })
        }
        StatementPlan::Update {
            table_name,
            mut assignments,
            mut selection,
        } => {
            for assignment in &mut assignments {
                plan_expr(&mut assignment.value)?;
                check_no_window(&assignment.value, "UPDATE SET")?;
            }

            if let Some(selection) = selection.as_mut() {
                plan_expr(selection)?;
                check_no_window(selection, "WHERE")?;
            }

            Ok(StatementPlan::Update {
                table_name,
                assignments,
                selection,
            })
        }
        StatementPlan::Delete {
            table_name,
            mut selection,
        } => {
            if let Some(selection) = selection.as_mut() {
                plan_expr(selection)?;
                check_no_window(selection, "WHERE")?;
            }

            Ok(StatementPlan::Delete {
                table_name,
                selection,
            })
        }
        other => Ok(other),
    }
}

fn plan_query(query: &mut QueryPlan) -> Result<()> {
    match query {
        QueryPlan::Project(project) => plan_project_query(project, &mut [], false)?,
        QueryPlan::Values(values) => plan_values(values)?,
        QueryPlan::SelectOrderBy(order_by) => plan_select_order_by(order_by, false)?,
        QueryPlan::ValuesOrderBy(order_by) => plan_values_order_by(order_by)?,
        QueryPlan::Distinct(distinct) => plan_distinct(distinct)?,
        QueryPlan::Offset(offset) => plan_offset(offset)?,
        QueryPlan::Limit(LimitPlan { input, count }) => {
            plan_limit_input(input)?;
            plan_expr(count)?;
            check_no_window(count, "LIMIT")?;
        }
    }

    Ok(())
}

fn plan_offset(OffsetPlan { input, count }: &mut OffsetPlan) -> Result<()> {
    plan_offset_input(input)?;
    plan_expr(count)?;
    check_no_window(count, "OFFSET")
}

fn plan_offset_input(input: &mut OffsetInputPlan) -> Result<()> {
    match input {
        OffsetInputPlan::Project(project) => plan_project_query(project, &mut [], false),
        OffsetInputPlan::Values(values) => plan_values(values),
        OffsetInputPlan::SelectOrderBy(order_by) => plan_select_order_by(order_by, false),
        OffsetInputPlan::ValuesOrderBy(order_by) => plan_values_order_by(order_by),
        OffsetInputPlan::Distinct(distinct) => plan_distinct(distinct),
    }
}

fn plan_limit_input(input: &mut LimitInputPlan) -> Result<()> {
    match input {
        LimitInputPlan::Project(project) => plan_project_query(project, &mut [], false),
        LimitInputPlan::Values(values) => plan_values(values),
        LimitInputPlan::SelectOrderBy(order_by) => plan_select_order_by(order_by, false),
        LimitInputPlan::ValuesOrderBy(order_by) => plan_values_order_by(order_by),
        LimitInputPlan::Distinct(distinct) => plan_distinct(distinct),
        LimitInputPlan::Offset(offset) => plan_offset(offset),
    }
}

fn plan_distinct(DistinctPlan { input }: &mut DistinctPlan) -> Result<()> {
    match input {
        DistinctInputPlan::Project(project) => plan_project_query(project, &mut [], true),
        DistinctInputPlan::SelectOrderBy(order_by) => plan_select_order_by(order_by, true),
    }
}

fn plan_select_order_by(
    SelectOrderByPlan { input, exprs }: &mut SelectOrderByPlan,
    is_distinct: bool,
) -> Result<()> {
    plan_project_query(input, exprs, is_distinct)
}

fn plan_values_order_by(ValuesOrderByPlan { input, exprs }: &mut ValuesOrderByPlan) -> Result<()> {
    plan_values(input)?;

    for order_by in exprs {
        plan_expr(&mut order_by.expr)?;
        check_no_window(&order_by.expr, "ORDER BY")?;
    }

    Ok(())
}

fn plan_values(ValuesPlan(exprs_list): &mut ValuesPlan) -> Result<()> {
    for exprs in exprs_list {
        for expr in exprs {
            plan_expr(expr)?;
            check_no_window(expr, "VALUES")?;
        }
    }

    Ok(())
}

/// Plans a `SELECT` projection, rejecting window functions found outside it,
/// nested window/aggregate placements, and window functions combined with
/// `GROUP BY`, `HAVING`, or `SELECT DISTINCT`. `order_by` carries the
/// query-level `ORDER BY` expressions of the enclosing `SelectOrderByPlan`,
/// if any (window functions are not allowed there either).
fn plan_project_query(
    project: &mut ProjectPlan,
    order_by: &mut [OrderByExprPlan],
    is_distinct: bool,
) -> Result<()> {
    plan_project_input(&mut project.input)?;

    if let ProjectionPlan::SelectItems(items) = &mut project.projection {
        for item in items {
            if let SelectItemPlan::Expr { expr, .. } = item {
                plan_expr(expr)?;
                check_nesting(expr)?;
            }
        }
    }

    for order_by_expr in order_by.iter_mut() {
        plan_expr(&mut order_by_expr.expr)?;
        check_no_window(&order_by_expr.expr, "ORDER BY")?;
    }

    let has_window = has_window_in_projection(&project.projection);
    let has_group_by_or_having = matches!(
        project.input,
        ProjectInputPlan::Aggregation(_) | ProjectInputPlan::Having(_)
    );
    let has_bare_aggregate = has_aggregate_in_projection(&project.projection);

    if has_window && (is_distinct || has_group_by_or_having || has_bare_aggregate) {
        return Err(PlannerError::WindowWithGroupByHavingOrDistinctNotSupported.into());
    }

    if has_window {
        bind_window(project);
    }

    Ok(())
}

fn has_window_in_projection(projection: &ProjectionPlan) -> bool {
    match projection {
        ProjectionPlan::SelectItems(items) => items.iter().any(|item| match item {
            SelectItemPlan::Expr { expr, .. } => find_window(expr),
            SelectItemPlan::QualifiedWildcard(_) | SelectItemPlan::Wildcard => false,
        }),
        ProjectionPlan::SchemalessMap => false,
    }
}

/// Whether the projection contains a plain (non-window) aggregate function
/// call anywhere, e.g. `COUNT(*)` in `SELECT COUNT(*), ROW_NUMBER() OVER ()
/// FROM T`. An `AggregateExprPlan` embedded inside a window function's own
/// `OVER (...)` (as `WindowFunctionPlan::Aggregate`) does not count here —
/// `try_visit_window` only walks its *argument* expression, never presenting
/// the aggregate itself as an `ExprPlan::Aggregate` node.
fn has_aggregate_in_projection(projection: &ProjectionPlan) -> bool {
    match projection {
        ProjectionPlan::SelectItems(items) => items.iter().any(|item| match item {
            SelectItemPlan::Expr { expr, .. } => find_aggregate(expr),
            SelectItemPlan::QualifiedWildcard(_) | SelectItemPlan::Wildcard => false,
        }),
        ProjectionPlan::SchemalessMap => false,
    }
}

fn find_aggregate(expr: &ExprPlan) -> bool {
    let mut found = false;

    let _ = try_visit_expr(expr, &mut |expr| {
        if matches!(expr, ExprPlan::Aggregate(_)) {
            found = true;
        }

        Ok(())
    });

    found
}

fn plan_project_input(input: &mut ProjectInputPlan) -> Result<()> {
    match input {
        ProjectInputPlan::Source(relation) => plan_source(relation),
        ProjectInputPlan::InnerJoin(join) => plan_inner_join(join),
        ProjectInputPlan::LeftOuterJoin(join) => plan_left_outer_join(join),
        ProjectInputPlan::Filter(filter) => plan_filter(filter),
        ProjectInputPlan::Aggregation(aggregation) => {
            plan_aggregation_input(&mut aggregation.input)?;

            for group_by in &mut aggregation.group_by {
                plan_expr(group_by)?;
                check_no_window(group_by, "GROUP BY")?;
            }

            Ok(())
        }
        ProjectInputPlan::Having(having) => {
            plan_aggregation_input(&mut having.input.input)?;

            for group_by in &mut having.input.group_by {
                plan_expr(group_by)?;
                check_no_window(group_by, "GROUP BY")?;
            }

            plan_expr(&mut having.expr)?;
            check_no_window(&having.expr, "HAVING")
        }
        // Unreachable when this pass runs (window functions have not been
        // bound into a `WindowPlan` yet at this point), but handled the same
        // way as `Aggregation`/`Having` for defense in depth.
        ProjectInputPlan::Window(window) => plan_window_input(&mut window.input),
    }
}

fn plan_window_input(input: &mut WindowInputPlan) -> Result<()> {
    match input {
        WindowInputPlan::Source(relation) => plan_source(relation),
        WindowInputPlan::InnerJoin(join) => plan_inner_join(join),
        WindowInputPlan::LeftOuterJoin(join) => plan_left_outer_join(join),
        WindowInputPlan::Filter(filter) => plan_filter(filter),
    }
}

fn plan_filter(FilterPlan { input, expr }: &mut FilterPlan) -> Result<()> {
    match input {
        FilterInputPlan::Source(relation) => plan_source(relation)?,
        FilterInputPlan::InnerJoin(join) => plan_inner_join(join)?,
        FilterInputPlan::LeftOuterJoin(join) => plan_left_outer_join(join)?,
    }

    plan_expr(expr)?;
    check_no_window(expr, "WHERE")
}

fn plan_aggregation_input(input: &mut AggregationInputPlan) -> Result<()> {
    match input {
        AggregationInputPlan::Source(relation) => plan_source(relation),
        AggregationInputPlan::InnerJoin(join) => plan_inner_join(join),
        AggregationInputPlan::LeftOuterJoin(join) => plan_left_outer_join(join),
        AggregationInputPlan::Filter(filter) => plan_filter(filter),
    }
}

fn plan_inner_join(join: &mut InnerJoinPlan) -> Result<()> {
    match &mut join.input {
        InnerJoinInputPlan::NestedLoop(join) => plan_nested_loop_join(join),
        InnerJoinInputPlan::Hash(join) => plan_hash_join(join),
        InnerJoinInputPlan::Condition(condition) => plan_join_condition(condition),
    }
}

fn plan_left_outer_join(join: &mut LeftOuterJoinPlan) -> Result<()> {
    match &mut join.input {
        LeftOuterJoinInputPlan::NestedLoop(join) => plan_nested_loop_join(join),
        LeftOuterJoinInputPlan::Hash(join) => plan_hash_join(join),
        LeftOuterJoinInputPlan::Condition(condition) => plan_join_condition(condition),
    }
}

fn plan_join_condition(condition: &mut JoinConditionPlan) -> Result<()> {
    match &mut condition.input {
        JoinConditionInputPlan::NestedLoop(join) => plan_nested_loop_join(join)?,
        JoinConditionInputPlan::Hash(join) => plan_hash_join(join)?,
    }

    plan_expr(&mut condition.expr)?;
    check_no_window(&condition.expr, "JOIN ON")
}

fn plan_nested_loop_join(join: &mut NestedLoopJoinPlan) -> Result<()> {
    match &mut join.input {
        NestedLoopJoinInputPlan::Source(source) => plan_source(source)?,
        NestedLoopJoinInputPlan::InnerJoin(join) => plan_inner_join(join)?,
        NestedLoopJoinInputPlan::LeftOuterJoin(join) => plan_left_outer_join(join)?,
    }

    plan_source(&mut join.right)
}

fn plan_hash_join(join: &mut HashJoinPlan) -> Result<()> {
    match &mut join.input {
        HashJoinInputPlan::Source(source) => plan_source(source)?,
        HashJoinInputPlan::InnerJoin(join) => plan_inner_join(join)?,
        HashJoinInputPlan::LeftOuterJoin(join) => plan_left_outer_join(join)?,
    }

    plan_source(&mut join.right)?;
    plan_expr(&mut join.input_key)?;
    check_no_window(&join.input_key, "JOIN")?;
    plan_expr(&mut join.right_key)?;
    check_no_window(&join.right_key, "JOIN")?;

    if let Some(right_filter) = &mut join.right_filter {
        plan_expr(right_filter)?;
        check_no_window(right_filter, "JOIN")?;
    }

    Ok(())
}

fn plan_source(source: &mut SourcePlan) -> Result<()> {
    match source {
        SourcePlan::Table(_) | SourcePlan::Dictionary(_) => Ok(()),
        SourcePlan::Derived(derived) => plan_query(&mut derived.query),
        SourcePlan::Series(series) => {
            plan_expr(&mut series.size)?;
            check_no_window(&series.size, "FROM")
        }
    }
}

/// Plans subqueries reachable from `expr`, without descending into window
/// function internals (those are handled separately by placement checks).
fn plan_expr(expr: &mut ExprPlan) -> Result<()> {
    let mut result = Ok(());

    visit_mut_expr(expr, &mut |expr| {
        if result.is_err() {
            return;
        }

        if let ExprPlan::Subquery(subquery)
        | ExprPlan::Exists { subquery, .. }
        | ExprPlan::InSubquery { subquery, .. } = expr
        {
            result = plan_query(subquery);
        }
    });

    result
}

fn check_no_window(expr: &ExprPlan, clause: &'static str) -> Result<()> {
    if find_window(expr) {
        Err(PlannerError::WindowFunctionNotAllowedInClause(clause).into())
    } else {
        Ok(())
    }
}

fn find_window(expr: &ExprPlan) -> bool {
    let mut found = false;

    let _ = try_visit_expr(expr, &mut |expr| {
        if matches!(expr, ExprPlan::Window(_)) {
            found = true;
        }

        Ok(())
    });

    found
}

/// Rejects a window function nested inside an aggregate, and both a window
/// function and a plain aggregate found nested inside another window
/// function's `OVER` clause or function arguments (e.g. `SUM(v) OVER
/// (PARTITION BY OTHER() OVER ())` or `... OVER (PARTITION BY SUM(v))`).
fn check_nesting(expr: &ExprPlan) -> Result<()> {
    try_visit_expr(expr, &mut |expr| match expr {
        ExprPlan::Aggregate(aggregate) => {
            if aggregate.as_expr().is_some_and(find_window) {
                Err(PlannerError::WindowNestedInAggregate)
            } else {
                Ok(())
            }
        }
        ExprPlan::Window(window) => check_window_children(window),
        _ => Ok(()),
    })
    .map_err(Into::into)
}

fn check_window_children(window: &WindowExprPlan) -> Result<(), PlannerError> {
    try_visit_window(window, &mut |expr| match expr {
        ExprPlan::Window(_) => Err(PlannerError::WindowNestedInWindow),
        ExprPlan::Aggregate(_) => Err(PlannerError::AggregateNestedInWindow),
        _ => Ok(()),
    })
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct WindowKey {
    func: WindowFunctionPlan,
    partition_by: Vec<ExprPlan>,
    order_by: Vec<OrderByExprPlan>,
}

/// Dedups window expressions in the projection by `(func, partition_by,
/// order_by)`, assigns each a slot, and wraps `project.input` in a
/// `WindowPlan`.
///
/// Only called once `plan_project_query` has confirmed the projection
/// contains a window function and that it is not combined with `GROUP BY`,
/// `HAVING`, or `DISTINCT` — so `project.input` is guaranteed at this point
/// to be `Source`, `InnerJoin`, `LeftOuterJoin`, or `Filter`.
fn bind_window(project: &mut ProjectPlan) {
    let mut slots = HashMap::new();
    let mut windows = Vec::new();

    if let ProjectionPlan::SelectItems(items) = &mut project.projection {
        for item in items {
            if let SelectItemPlan::Expr { expr, .. } = item {
                visit_mut_expr(expr, &mut |expr| {
                    if let ExprPlan::Window(window) = expr {
                        let key = WindowKey {
                            func: window.func.clone(),
                            partition_by: window.over.partition_by.clone(),
                            order_by: window.over.order_by.clone(),
                        };

                        let slot = *slots.entry(key).or_insert_with(|| {
                            let slot = windows.len();
                            let mut window = window.as_ref().clone();
                            window.slot = Some(slot);
                            windows.push(window);
                            slot
                        });

                        window.slot = Some(slot);
                    }
                });
            }
        }
    }

    match &mut project.input {
        ProjectInputPlan::Source(relation) => {
            project.input = ProjectInputPlan::Window(WindowPlan {
                input: WindowInputPlan::Source(relation.clone()),
                window_slots: windows,
            });
        }
        ProjectInputPlan::InnerJoin(join) => {
            project.input = ProjectInputPlan::Window(WindowPlan {
                input: WindowInputPlan::InnerJoin(join.clone()),
                window_slots: windows,
            });
        }
        ProjectInputPlan::LeftOuterJoin(join) => {
            project.input = ProjectInputPlan::Window(WindowPlan {
                input: WindowInputPlan::LeftOuterJoin(join.clone()),
                window_slots: windows,
            });
        }
        ProjectInputPlan::Filter(filter) => {
            project.input = ProjectInputPlan::Window(WindowPlan {
                input: WindowInputPlan::Filter(filter.clone()),
                window_slots: windows,
            });
        }
        ProjectInputPlan::Aggregation(_)
        | ProjectInputPlan::Having(_)
        | ProjectInputPlan::Window(_) => {
            unreachable!("window functions combined with GROUP BY/HAVING are rejected earlier")
        }
    }
}
