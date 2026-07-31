use {
    crate::{
        plan::{
            ExprPlan, JoinConstraintPlan, JoinExecutorPlan, JoinOperatorPlan, JoinPlan,
            OrderByExprPlan, PlanError, ProjectionPlan, QueryPlan, SelectItemPlan, SelectPlan,
            SetExprPlan, StatementPlan, TableFactorPlan, TableWithJoinsPlan, ValuesPlan,
            WindowFunctionPlan, WindowPlan,
            expr::{try_visit_expr, visit_mut_expr},
        },
        result::Result,
    },
    std::collections::HashMap,
};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct WindowKey {
    func: WindowFunctionPlan,
    partition_by: Vec<ExprPlan>,
    order_by: Vec<OrderByExprPlan>,
}

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
        } => {
            plan_query(&mut source)?;
            Ok(StatementPlan::Insert {
                table_name,
                columns,
                source,
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
    match &mut query.body {
        SetExprPlan::Select(select) => {
            plan_select(select)?;

            for order_by in &mut query.order_by {
                plan_expr(&mut order_by.expr)?;
                check_no_window(&order_by.expr, "ORDER BY")?;
            }

            bind_select(select);
        }
        SetExprPlan::Values(ValuesPlan(exprs_list)) => {
            for exprs in exprs_list {
                for expr in exprs {
                    plan_expr(expr)?;
                    check_no_window(expr, "VALUES")?;
                }
            }
        }
    }

    if let Some(limit) = query.limit.as_mut() {
        plan_expr(limit)?;
        check_no_window(limit, "LIMIT")?;
    }

    if let Some(offset) = query.offset.as_mut() {
        plan_expr(offset)?;
        check_no_window(offset, "OFFSET")?;
    }

    Ok(())
}

fn plan_select(select: &mut SelectPlan) -> Result<()> {
    plan_table_with_joins(&mut select.from)?;

    if let ProjectionPlan::SelectItems(items) = &mut select.projection {
        for item in items {
            if let SelectItemPlan::Expr { expr, .. } = item {
                plan_expr(expr)?;
                check_nesting(expr)?;
            }
        }
    }

    if let Some(selection) = select.selection.as_mut() {
        plan_expr(selection)?;
        check_no_window(selection, "WHERE")?;
    }

    for group_by in &mut select.group_by {
        plan_expr(group_by)?;
        check_no_window(group_by, "GROUP BY")?;
    }

    if let Some(having) = select.having.as_mut() {
        plan_expr(having)?;
        check_no_window(having, "HAVING")?;
    }

    let has_window = match &select.projection {
        ProjectionPlan::SelectItems(items) => items.iter().any(|item| match item {
            SelectItemPlan::Expr { expr, .. } => find_window(expr),
            SelectItemPlan::QualifiedWildcard(_) | SelectItemPlan::Wildcard => false,
        }),
        ProjectionPlan::SchemalessMap => false,
    };

    if has_window && (select.distinct || !select.group_by.is_empty() || select.having.is_some()) {
        return Err(PlanError::WindowWithGroupByHavingOrDistinctNotSupported.into());
    }

    Ok(())
}

fn plan_table_with_joins(table_with_joins: &mut TableWithJoinsPlan) -> Result<()> {
    plan_table_factor(&mut table_with_joins.relation)?;

    for join in &mut table_with_joins.joins {
        plan_join(join)?;
    }

    Ok(())
}

fn plan_table_factor(table_factor: &mut TableFactorPlan) -> Result<()> {
    match table_factor {
        TableFactorPlan::Table { .. } | TableFactorPlan::Dictionary { .. } => Ok(()),
        TableFactorPlan::Derived { subquery, .. } => plan_query(subquery),
        TableFactorPlan::Series { size, .. } => {
            plan_expr(size)?;
            check_no_window(size, "FROM")
        }
    }
}

fn plan_join(join: &mut JoinPlan) -> Result<()> {
    plan_table_factor(&mut join.relation)?;

    match &mut join.join_operator {
        JoinOperatorPlan::Inner(JoinConstraintPlan::On(expr))
        | JoinOperatorPlan::LeftOuter(JoinConstraintPlan::On(expr)) => {
            plan_expr(expr)?;
            check_no_window(expr, "JOIN ON")?;
        }
        JoinOperatorPlan::Inner(JoinConstraintPlan::None)
        | JoinOperatorPlan::LeftOuter(JoinConstraintPlan::None) => {}
    }

    if let JoinExecutorPlan::Hash {
        key_expr,
        value_expr,
        where_clause,
    } = &mut join.join_executor
    {
        plan_expr(key_expr)?;
        check_no_window(key_expr, "JOIN")?;
        plan_expr(value_expr)?;
        check_no_window(value_expr, "JOIN")?;

        if let Some(where_clause) = where_clause {
            plan_expr(where_clause)?;
            check_no_window(where_clause, "JOIN")?;
        }
    }

    Ok(())
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
        Err(PlanError::WindowFunctionNotAllowedInClause(clause).into())
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

fn check_nesting(expr: &ExprPlan) -> Result<()> {
    try_visit_expr(expr, &mut |expr| match expr {
        ExprPlan::Aggregate(aggregate) => {
            if aggregate.as_expr().is_some_and(find_window) {
                Err(PlanError::WindowNestedInAggregate)
            } else {
                Ok(())
            }
        }
        ExprPlan::Window(window) => {
            if window_contains_window(window) {
                Err(PlanError::WindowNestedInWindow)
            } else {
                Ok(())
            }
        }
        _ => Ok(()),
    })
    .map_err(Into::into)
}

fn window_contains_window(window: &WindowPlan) -> bool {
    window.over.partition_by.iter().any(find_window)
        || window
            .over
            .order_by
            .iter()
            .any(|order_by| find_window(&order_by.expr))
        || match &window.func {
            WindowFunctionPlan::RowNumber
            | WindowFunctionPlan::Rank
            | WindowFunctionPlan::DenseRank => false,
            WindowFunctionPlan::Lag {
                expr,
                offset,
                default,
            }
            | WindowFunctionPlan::Lead {
                expr,
                offset,
                default,
            } => {
                find_window(expr)
                    || find_window(offset)
                    || default.as_ref().is_some_and(find_window)
            }
            WindowFunctionPlan::Aggregate(aggregate) => {
                aggregate.as_expr().is_some_and(find_window)
            }
        }
}

fn bind_select(select: &mut SelectPlan) {
    let mut slots = HashMap::new();
    let mut windows = Vec::new();

    if let ProjectionPlan::SelectItems(items) = &mut select.projection {
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

    select.window_slots = (!windows.is_empty()).then_some(windows);
}
