use {
    super::expr::visit_mut_expr,
    crate::{
        ast::Dictionary,
        data::{SCHEMALESS_DOC_COLUMN, Schema},
        plan::{
            AggregationInputPlan, DistinctInputPlan, ExprPlan, FilterInputPlan, HashJoinInputPlan,
            HashJoinPlan, InnerJoinInputPlan, InnerJoinPlan, JoinConditionInputPlan,
            LeftOuterJoinInputPlan, LeftOuterJoinPlan, LimitInputPlan, NestedLoopJoinInputPlan,
            NestedLoopJoinPlan, OffsetInputPlan, ProjectInputPlan, ProjectPlan, ProjectionPlan,
            QueryPlan, SelectItemPlan, SourcePlan, StatementPlan, ValuesPlan,
        },
        planner::PlannerError,
        result::Result,
    },
    std::{collections::HashMap, hash::BuildHasher, rc::Rc},
};

pub fn plan<S: BuildHasher>(
    schema_map: &HashMap<String, Schema, S>,
    mut statement: StatementPlan,
) -> Result<StatementPlan> {
    match &mut statement {
        StatementPlan::Query(query) => plan_query(schema_map, query, None)?,
        StatementPlan::Insert { source, .. } => plan_query(schema_map, source, None)?,
        StatementPlan::CreateTable {
            source: Some(source),
            ..
        } => plan_query(schema_map, source, None)?,
        StatementPlan::Update {
            table_name,
            assignments,
            selection,
        } => {
            let context = source_context(schema_map, table_name, None);
            for assignment in assignments {
                plan_expr(schema_map, &context, &mut assignment.value)?;
            }
            if let Some(selection) = selection {
                plan_expr(schema_map, &context, selection)?;
            }
        }
        StatementPlan::Delete {
            table_name,
            selection: Some(selection),
        } => {
            let context = source_context(schema_map, table_name, None);
            plan_expr(schema_map, &context, selection)?;
        }
        _ => {}
    }
    Ok(statement)
}

pub fn plan_scalar(alias: &str, expr: &mut ExprPlan) {
    visit_mut_expr(expr, &mut |expr| {
        if let ExprPlan::UnplannedReference { qualifier, name } = expr
            && qualifier
                .as_deref()
                .is_none_or(|qualifier| qualifier == alias)
        {
            *expr = ExprPlan::ResolvedColumn {
                alias: alias.to_owned(),
                column: name.clone(),
            };
        }
    });
}

fn plan_query<S: BuildHasher>(
    schema_map: &HashMap<String, Schema, S>,
    query: &mut QueryPlan,
    outer: Option<Rc<Context>>,
) -> Result<()> {
    prepare_sources(schema_map, query, outer.as_ref())?;
    let context = query_context(schema_map, query, outer);
    visit_query_exprs(schema_map, query, &context)
}

fn prepare_sources<S: BuildHasher>(
    schema_map: &HashMap<String, Schema, S>,
    query: &mut QueryPlan,
    outer: Option<&Rc<Context>>,
) -> Result<()> {
    let Some(project) = query.project_mut() else {
        return Ok(());
    };
    prepare_input_sources(schema_map, &mut project.input, outer)
}

fn prepare_input_sources<S: BuildHasher>(
    schema_map: &HashMap<String, Schema, S>,
    input: &mut ProjectInputPlan,
    outer: Option<&Rc<Context>>,
) -> Result<()> {
    match input {
        ProjectInputPlan::Source(source) => prepare_source(schema_map, source, outer),
        ProjectInputPlan::InnerJoin(join) => prepare_inner(schema_map, join, outer),
        ProjectInputPlan::LeftOuterJoin(join) => prepare_left(schema_map, join, outer),
        ProjectInputPlan::Filter(filter) => prepare_filter(schema_map, &mut filter.input, outer),
        ProjectInputPlan::Aggregation(aggregation) => {
            prepare_aggregation(schema_map, &mut aggregation.input, outer)
        }
        ProjectInputPlan::Having(having) => {
            prepare_aggregation(schema_map, &mut having.input.input, outer)
        }
    }
}

fn prepare_source<S: BuildHasher>(
    schema_map: &HashMap<String, Schema, S>,
    source: &mut SourcePlan,
    outer: Option<&Rc<Context>>,
) -> Result<()> {
    if let SourcePlan::Derived(source) = source {
        plan_query(schema_map, &mut source.query, outer.cloned())?;
    }
    Ok(())
}

fn prepare_filter<S: BuildHasher>(
    schema_map: &HashMap<String, Schema, S>,
    input: &mut FilterInputPlan,
    outer: Option<&Rc<Context>>,
) -> Result<()> {
    match input {
        FilterInputPlan::Source(source) => prepare_source(schema_map, source, outer),
        FilterInputPlan::InnerJoin(join) => prepare_inner(schema_map, join, outer),
        FilterInputPlan::LeftOuterJoin(join) => prepare_left(schema_map, join, outer),
    }
}

fn prepare_aggregation<S: BuildHasher>(
    schema_map: &HashMap<String, Schema, S>,
    input: &mut AggregationInputPlan,
    outer: Option<&Rc<Context>>,
) -> Result<()> {
    match input {
        AggregationInputPlan::Source(source) => prepare_source(schema_map, source, outer),
        AggregationInputPlan::InnerJoin(join) => prepare_inner(schema_map, join, outer),
        AggregationInputPlan::LeftOuterJoin(join) => prepare_left(schema_map, join, outer),
        AggregationInputPlan::Filter(filter) => {
            prepare_filter(schema_map, &mut filter.input, outer)
        }
    }
}

fn prepare_inner<S: BuildHasher>(
    schema_map: &HashMap<String, Schema, S>,
    join: &mut InnerJoinPlan,
    outer: Option<&Rc<Context>>,
) -> Result<()> {
    match &mut join.input {
        InnerJoinInputPlan::NestedLoop(join) => prepare_nested(schema_map, join, outer),
        InnerJoinInputPlan::Condition(condition) => match &mut condition.input {
            JoinConditionInputPlan::NestedLoop(join) => prepare_nested(schema_map, join, outer),
            JoinConditionInputPlan::Hash(join) => prepare_hash(schema_map, join, outer),
        },
        InnerJoinInputPlan::Hash(join) => prepare_hash(schema_map, join, outer),
    }
}

fn prepare_left<S: BuildHasher>(
    schema_map: &HashMap<String, Schema, S>,
    join: &mut LeftOuterJoinPlan,
    outer: Option<&Rc<Context>>,
) -> Result<()> {
    match &mut join.input {
        LeftOuterJoinInputPlan::NestedLoop(join) => prepare_nested(schema_map, join, outer),
        LeftOuterJoinInputPlan::Condition(condition) => match &mut condition.input {
            JoinConditionInputPlan::NestedLoop(join) => prepare_nested(schema_map, join, outer),
            JoinConditionInputPlan::Hash(join) => prepare_hash(schema_map, join, outer),
        },
        LeftOuterJoinInputPlan::Hash(join) => prepare_hash(schema_map, join, outer),
    }
}

fn prepare_nested<S: BuildHasher>(
    schema_map: &HashMap<String, Schema, S>,
    join: &mut NestedLoopJoinPlan,
    outer: Option<&Rc<Context>>,
) -> Result<()> {
    match &mut join.input {
        NestedLoopJoinInputPlan::Source(source) => prepare_source(schema_map, source, outer)?,
        NestedLoopJoinInputPlan::InnerJoin(join) => prepare_inner(schema_map, join, outer)?,
        NestedLoopJoinInputPlan::LeftOuterJoin(join) => prepare_left(schema_map, join, outer)?,
    }
    prepare_source(schema_map, &mut join.right, outer)
}

fn prepare_hash<S: BuildHasher>(
    schema_map: &HashMap<String, Schema, S>,
    join: &mut HashJoinPlan,
    outer: Option<&Rc<Context>>,
) -> Result<()> {
    match &mut join.input {
        HashJoinInputPlan::Source(source) => prepare_source(schema_map, source, outer)?,
        HashJoinInputPlan::InnerJoin(join) => prepare_inner(schema_map, join, outer)?,
        HashJoinInputPlan::LeftOuterJoin(join) => prepare_left(schema_map, join, outer)?,
    }
    prepare_source(schema_map, &mut join.right, outer)
}

fn query_context<S: BuildHasher>(
    schema_map: &HashMap<String, Schema, S>,
    query: &QueryPlan,
    outer: Option<Rc<Context>>,
) -> Rc<Context> {
    let local = match query {
        QueryPlan::Project(project) => input_context(schema_map, &project.input),
        QueryPlan::SelectOrderBy(order_by) => input_context(schema_map, &order_by.input.input),
        QueryPlan::Distinct(distinct) => match &distinct.input {
            DistinctInputPlan::Project(project) => input_context(schema_map, &project.input),
            DistinctInputPlan::SelectOrderBy(order_by) => {
                input_context(schema_map, &order_by.input.input)
            }
        },
        QueryPlan::ValuesOrderBy(order_by) => values_context(&order_by.input),
        QueryPlan::Values(values) => values_context(values),
        QueryPlan::Offset(offset) => offset_input_context(schema_map, &offset.input),
        QueryPlan::Limit(limit) => limit_input_context(schema_map, &limit.input),
    };
    match outer {
        Some(outer) => Rc::new(Context::Scope { local, outer }),
        None => local,
    }
}

fn input_context<S: BuildHasher>(
    schema_map: &HashMap<String, Schema, S>,
    input: &ProjectInputPlan,
) -> Rc<Context> {
    let mut sources = vec![input.base_source()];
    sources.extend(input.joined_sources());
    sources
        .into_iter()
        .map(|source| source_context(schema_map, source.alias_name(), Some(source)))
        .reduce(|left, right| Rc::new(Context::Bridge { left, right }))
        .unwrap_or_else(|| Rc::new(Context::Barrier))
}

fn source_context<S: BuildHasher>(
    schema_map: &HashMap<String, Schema, S>,
    name: &str,
    source: Option<&SourcePlan>,
) -> Rc<Context> {
    let Some(source) = source else {
        return schema_map.get(name).map_or_else(
            || Rc::new(Context::Barrier),
            |schema| data_context(name.to_owned(), Some(schema_labels(schema))),
        );
    };
    match source {
        SourcePlan::Table(table) => schema_map.get(&table.name).map_or_else(
            || Rc::new(Context::Barrier),
            |schema| {
                let mut labels = schema_labels(schema);
                if let Some(alias) = &table.alias {
                    labels
                        .iter_mut()
                        .zip(&alias.columns)
                        .for_each(|(label, alias)| {
                            label.clone_from(alias);
                        });
                }
                data_context(source.alias_name().to_owned(), Some(labels))
            },
        ),
        SourcePlan::Derived(derived) => {
            let labels = (!derived.alias.columns.is_empty())
                .then(|| derived.alias.columns.clone())
                .or_else(|| query_output_labels(schema_map, &derived.query));
            data_context(derived.alias.name.clone(), labels)
        }
        SourcePlan::Series(series) => {
            data_context(series.alias.name.clone(), Some(vec!["N".to_owned()]))
        }
        SourcePlan::Dictionary(dictionary) => data_context(
            dictionary.alias.name.clone(),
            Some(dictionary_labels(&dictionary.dictionary)),
        ),
    }
}

fn data_context(alias: String, labels: Option<Vec<String>>) -> Rc<Context> {
    Rc::new(Context::Data { alias, labels })
}

fn values_context(values: &ValuesPlan) -> Rc<Context> {
    data_context(
        "VALUES".to_owned(),
        values.0.first().map(|row| {
            (1..=row.len())
                .map(|index| format!("column{index}"))
                .collect()
        }),
    )
}

fn output_context(projection: &ProjectionPlan, source: &Rc<Context>) -> Rc<Context> {
    Rc::new(Context::Scope {
        local: data_context("OUTPUT".to_owned(), projection_labels(projection)),
        outer: Rc::clone(source),
    })
}

fn limit_input_context<S: BuildHasher>(
    schema_map: &HashMap<String, Schema, S>,
    input: &LimitInputPlan,
) -> Rc<Context> {
    match input {
        LimitInputPlan::Project(project) => input_context(schema_map, &project.input),
        LimitInputPlan::Values(values) => values_context(values),
        LimitInputPlan::ValuesOrderBy(order_by) => values_context(&order_by.input),
        LimitInputPlan::SelectOrderBy(order_by) => input_context(schema_map, &order_by.input.input),
        LimitInputPlan::Distinct(distinct) => distinct_input_context(schema_map, &distinct.input),
        LimitInputPlan::Offset(offset) => offset_input_context(schema_map, &offset.input),
    }
}

fn offset_input_context<S: BuildHasher>(
    schema_map: &HashMap<String, Schema, S>,
    input: &OffsetInputPlan,
) -> Rc<Context> {
    match input {
        OffsetInputPlan::Project(project) => input_context(schema_map, &project.input),
        OffsetInputPlan::Values(values) => values_context(values),
        OffsetInputPlan::ValuesOrderBy(order_by) => values_context(&order_by.input),
        OffsetInputPlan::SelectOrderBy(order_by) => {
            input_context(schema_map, &order_by.input.input)
        }
        OffsetInputPlan::Distinct(distinct) => distinct_input_context(schema_map, &distinct.input),
    }
}

fn distinct_input_context<S: BuildHasher>(
    schema_map: &HashMap<String, Schema, S>,
    input: &DistinctInputPlan,
) -> Rc<Context> {
    match input {
        DistinctInputPlan::Project(project) => input_context(schema_map, &project.input),
        DistinctInputPlan::SelectOrderBy(order_by) => {
            input_context(schema_map, &order_by.input.input)
        }
    }
}

fn schema_labels(schema: &Schema) -> Vec<String> {
    match schema.column_defs.as_ref() {
        Some(columns) if !columns.is_empty() => {
            columns.iter().map(|column| column.name.clone()).collect()
        }
        _ => vec![SCHEMALESS_DOC_COLUMN.to_owned()],
    }
}

fn projection_labels(projection: &ProjectionPlan) -> Option<Vec<String>> {
    match projection {
        ProjectionPlan::SchemalessMap => Some(vec![SCHEMALESS_DOC_COLUMN.to_owned()]),
        ProjectionPlan::SelectItems(items) => items
            .iter()
            .map(|item| match item {
                SelectItemPlan::Expr { label, .. } => Some(label.clone()),
                SelectItemPlan::Wildcard | SelectItemPlan::QualifiedWildcard(_) => None,
            })
            .collect(),
    }
}

fn query_output_labels<S: BuildHasher>(
    schema_map: &HashMap<String, Schema, S>,
    query: &QueryPlan,
) -> Option<Vec<String>> {
    match query {
        QueryPlan::Values(values) => values.0.first().map(|row| {
            (1..=row.len())
                .map(|index| format!("column{index}"))
                .collect()
        }),
        _ => query.project().and_then(|project| {
            projection_labels(&project.projection)
                .or_else(|| input_context(schema_map, &project.input).all_labels())
        }),
    }
}

fn dictionary_labels(dictionary: &Dictionary) -> Vec<String> {
    match dictionary {
        Dictionary::GlueObjects => vec!["OBJECT_NAME", "OBJECT_TYPE", "CREATED"],
        Dictionary::GlueTables => vec!["TABLE_NAME", "COMMENT"],
        Dictionary::GlueTableColumns => vec![
            "TABLE_NAME",
            "COLUMN_NAME",
            "COLUMN_ID",
            "NULLABLE",
            "KEY",
            "DEFAULT",
            "COMMENT",
        ],
        Dictionary::GlueIndexes => vec![
            "TABLE_NAME",
            "INDEX_NAME",
            "ORDER",
            "EXPRESSION",
            "UNIQUENESS",
        ],
    }
    .into_iter()
    .map(str::to_owned)
    .collect()
}

fn visit_query_exprs<S: BuildHasher>(
    schema_map: &HashMap<String, Schema, S>,
    query: &mut QueryPlan,
    context: &Rc<Context>,
) -> Result<()> {
    match query {
        QueryPlan::Project(project) => visit_project_exprs(schema_map, project, context),
        QueryPlan::Values(values) => {
            for expr in values.0.iter_mut().flatten() {
                plan_expr(schema_map, context, expr)?;
            }
            Ok(())
        }
        QueryPlan::SelectOrderBy(order_by) => {
            visit_project_exprs(schema_map, &mut order_by.input, context)?;
            let order_context = output_context(&order_by.input.projection, context);
            for expr in &mut order_by.exprs {
                plan_expr(schema_map, &order_context, &mut expr.expr)?;
            }
            Ok(())
        }
        QueryPlan::ValuesOrderBy(order_by) => {
            for expr in order_by.input.0.iter_mut().flatten() {
                plan_expr(schema_map, context, expr)?;
            }
            let order_context = values_context(&order_by.input);
            for expr in &mut order_by.exprs {
                plan_expr(schema_map, &order_context, &mut expr.expr)?;
            }
            Ok(())
        }
        QueryPlan::Distinct(distinct) => match &mut distinct.input {
            DistinctInputPlan::Project(project) => {
                visit_project_exprs(schema_map, project, context)
            }
            DistinctInputPlan::SelectOrderBy(order_by) => {
                visit_project_exprs(schema_map, &mut order_by.input, context)?;
                let order_context = output_context(&order_by.input.projection, context);
                for expr in &mut order_by.exprs {
                    plan_expr(schema_map, &order_context, &mut expr.expr)?;
                }
                Ok(())
            }
        },
        QueryPlan::Offset(offset) => {
            visit_offset_input_exprs(schema_map, &mut offset.input, context)?;
            plan_expr(schema_map, context, &mut offset.count)
        }
        QueryPlan::Limit(limit) => {
            visit_query_input_exprs(schema_map, &mut limit.input, context)?;
            plan_expr(schema_map, context, &mut limit.count)
        }
    }
}

fn visit_query_input_exprs<S: BuildHasher>(
    schema_map: &HashMap<String, Schema, S>,
    input: &mut LimitInputPlan,
    context: &Rc<Context>,
) -> Result<()> {
    match input {
        LimitInputPlan::Project(project) => visit_project_exprs(schema_map, project, context),
        LimitInputPlan::Values(values) => {
            for expr in values.0.iter_mut().flatten() {
                plan_expr(schema_map, context, expr)?;
            }
            Ok(())
        }
        LimitInputPlan::SelectOrderBy(order_by) => {
            visit_project_exprs(schema_map, &mut order_by.input, context)?;
            let order_context = output_context(&order_by.input.projection, context);
            for expr in &mut order_by.exprs {
                plan_expr(schema_map, &order_context, &mut expr.expr)?;
            }
            Ok(())
        }
        LimitInputPlan::ValuesOrderBy(order_by) => {
            for expr in order_by.input.0.iter_mut().flatten() {
                plan_expr(schema_map, context, expr)?;
            }
            let order_context = values_context(&order_by.input);
            for expr in &mut order_by.exprs {
                plan_expr(schema_map, &order_context, &mut expr.expr)?;
            }
            Ok(())
        }
        LimitInputPlan::Distinct(distinct) => match &mut distinct.input {
            DistinctInputPlan::Project(project) => {
                visit_project_exprs(schema_map, project, context)
            }
            DistinctInputPlan::SelectOrderBy(order_by) => {
                visit_project_exprs(schema_map, &mut order_by.input, context)?;
                let order_context = output_context(&order_by.input.projection, context);
                for expr in &mut order_by.exprs {
                    plan_expr(schema_map, &order_context, &mut expr.expr)?;
                }
                Ok(())
            }
        },
        LimitInputPlan::Offset(offset) => {
            visit_offset_input_exprs(schema_map, &mut offset.input, context)?;
            plan_expr(schema_map, context, &mut offset.count)
        }
    }
}

fn visit_offset_input_exprs<S: BuildHasher>(
    schema_map: &HashMap<String, Schema, S>,
    input: &mut OffsetInputPlan,
    context: &Rc<Context>,
) -> Result<()> {
    match input {
        OffsetInputPlan::Project(project) => visit_project_exprs(schema_map, project, context),
        OffsetInputPlan::SelectOrderBy(order_by) => {
            visit_project_exprs(schema_map, &mut order_by.input, context)?;
            let order_context = output_context(&order_by.input.projection, context);
            for expr in &mut order_by.exprs {
                plan_expr(schema_map, &order_context, &mut expr.expr)?;
            }
            Ok(())
        }
        OffsetInputPlan::Distinct(distinct) => match &mut distinct.input {
            DistinctInputPlan::Project(project) => {
                visit_project_exprs(schema_map, project, context)
            }
            DistinctInputPlan::SelectOrderBy(order_by) => {
                visit_project_exprs(schema_map, &mut order_by.input, context)?;
                let order_context = output_context(&order_by.input.projection, context);
                for expr in &mut order_by.exprs {
                    plan_expr(schema_map, &order_context, &mut expr.expr)?;
                }
                Ok(())
            }
        },
        OffsetInputPlan::Values(values) => {
            for expr in values.0.iter_mut().flatten() {
                plan_expr(schema_map, context, expr)?;
            }
            Ok(())
        }
        OffsetInputPlan::ValuesOrderBy(order_by) => {
            for expr in order_by.input.0.iter_mut().flatten() {
                plan_expr(schema_map, context, expr)?;
            }
            let order_context = values_context(&order_by.input);
            for expr in &mut order_by.exprs {
                plan_expr(schema_map, &order_context, &mut expr.expr)?;
            }
            Ok(())
        }
    }
}

fn visit_project_exprs<S: BuildHasher>(
    schema_map: &HashMap<String, Schema, S>,
    project: &mut ProjectPlan,
    context: &Rc<Context>,
) -> Result<()> {
    visit_input_exprs(schema_map, &mut project.input, context)?;
    if let ProjectionPlan::SelectItems(items) = &mut project.projection {
        for item in items {
            if let SelectItemPlan::Expr { expr, .. } = item {
                plan_expr(schema_map, context, expr)?;
            }
        }
    }
    Ok(())
}

fn visit_input_exprs<S: BuildHasher>(
    schema_map: &HashMap<String, Schema, S>,
    input: &mut ProjectInputPlan,
    context: &Rc<Context>,
) -> Result<()> {
    match input {
        ProjectInputPlan::Source(_) => Ok(()),
        ProjectInputPlan::InnerJoin(join) => visit_inner_exprs(schema_map, join, context),
        ProjectInputPlan::LeftOuterJoin(join) => visit_left_exprs(schema_map, join, context),
        ProjectInputPlan::Filter(filter) => {
            visit_filter_exprs(schema_map, &mut filter.input, context)?;
            plan_expr(schema_map, context, &mut filter.expr)
        }
        ProjectInputPlan::Aggregation(aggregation) => {
            visit_aggregation_exprs(schema_map, &mut aggregation.input, context)?;
            for expr in &mut aggregation.group_by {
                plan_expr(schema_map, context, expr)?;
            }
            Ok(())
        }
        ProjectInputPlan::Having(having) => {
            visit_aggregation_exprs(schema_map, &mut having.input.input, context)?;
            for expr in &mut having.input.group_by {
                plan_expr(schema_map, context, expr)?;
            }
            plan_expr(schema_map, context, &mut having.expr)
        }
    }
}

fn visit_filter_exprs<S: BuildHasher>(
    schema_map: &HashMap<String, Schema, S>,
    input: &mut FilterInputPlan,
    context: &Rc<Context>,
) -> Result<()> {
    match input {
        FilterInputPlan::Source(_) => Ok(()),
        FilterInputPlan::InnerJoin(join) => visit_inner_exprs(schema_map, join, context),
        FilterInputPlan::LeftOuterJoin(join) => visit_left_exprs(schema_map, join, context),
    }
}

fn visit_aggregation_exprs<S: BuildHasher>(
    schema_map: &HashMap<String, Schema, S>,
    input: &mut AggregationInputPlan,
    context: &Rc<Context>,
) -> Result<()> {
    match input {
        AggregationInputPlan::Source(_) => Ok(()),
        AggregationInputPlan::InnerJoin(join) => visit_inner_exprs(schema_map, join, context),
        AggregationInputPlan::LeftOuterJoin(join) => visit_left_exprs(schema_map, join, context),
        AggregationInputPlan::Filter(filter) => {
            visit_filter_exprs(schema_map, &mut filter.input, context)?;
            plan_expr(schema_map, context, &mut filter.expr)
        }
    }
}

fn visit_inner_exprs<S: BuildHasher>(
    schema_map: &HashMap<String, Schema, S>,
    join: &mut InnerJoinPlan,
    context: &Rc<Context>,
) -> Result<()> {
    match &mut join.input {
        InnerJoinInputPlan::NestedLoop(join) => visit_nested_exprs(schema_map, join, context),
        InnerJoinInputPlan::Condition(condition) => {
            visit_condition_exprs(schema_map, &mut condition.input, context)?;
            plan_expr(schema_map, context, &mut condition.expr)
        }
        InnerJoinInputPlan::Hash(join) => visit_hash_exprs(schema_map, join, context),
    }
}

fn visit_left_exprs<S: BuildHasher>(
    schema_map: &HashMap<String, Schema, S>,
    join: &mut LeftOuterJoinPlan,
    context: &Rc<Context>,
) -> Result<()> {
    match &mut join.input {
        LeftOuterJoinInputPlan::NestedLoop(join) => visit_nested_exprs(schema_map, join, context),
        LeftOuterJoinInputPlan::Condition(condition) => {
            visit_condition_exprs(schema_map, &mut condition.input, context)?;
            plan_expr(schema_map, context, &mut condition.expr)
        }
        LeftOuterJoinInputPlan::Hash(join) => visit_hash_exprs(schema_map, join, context),
    }
}

fn visit_condition_exprs<S: BuildHasher>(
    schema_map: &HashMap<String, Schema, S>,
    input: &mut JoinConditionInputPlan,
    context: &Rc<Context>,
) -> Result<()> {
    match input {
        JoinConditionInputPlan::NestedLoop(join) => visit_nested_exprs(schema_map, join, context),
        JoinConditionInputPlan::Hash(join) => visit_hash_exprs(schema_map, join, context),
    }
}

fn visit_nested_exprs<S: BuildHasher>(
    schema_map: &HashMap<String, Schema, S>,
    join: &mut NestedLoopJoinPlan,
    context: &Rc<Context>,
) -> Result<()> {
    match &mut join.input {
        NestedLoopJoinInputPlan::Source(_) => Ok(()),
        NestedLoopJoinInputPlan::InnerJoin(join) => visit_inner_exprs(schema_map, join, context),
        NestedLoopJoinInputPlan::LeftOuterJoin(join) => visit_left_exprs(schema_map, join, context),
    }
}

fn visit_hash_exprs<S: BuildHasher>(
    schema_map: &HashMap<String, Schema, S>,
    join: &mut HashJoinPlan,
    context: &Rc<Context>,
) -> Result<()> {
    plan_expr(schema_map, context, &mut join.input_key)?;
    plan_expr(schema_map, context, &mut join.right_key)?;
    if let Some(filter) = &mut join.right_filter {
        plan_expr(schema_map, context, filter)?;
    }
    Ok(())
}

fn plan_expr<S: BuildHasher>(
    schema_map: &HashMap<String, Schema, S>,
    context: &Rc<Context>,
    expr: &mut ExprPlan,
) -> Result<()> {
    let mut error = None;
    visit_mut_expr(expr, &mut |expr| {
        if let ExprPlan::UnplannedReference { qualifier, name } = expr {
            match context.resolve(qualifier.as_deref(), name) {
                Resolution::Resolved(alias) => {
                    *expr = ExprPlan::ResolvedColumn {
                        alias,
                        column: name.clone(),
                    };
                }
                Resolution::Ambiguous => {
                    error = Some(PlannerError::ColumnReferenceAmbiguous(name.clone()));
                }
                Resolution::Unresolved => {}
            }
        }
    });
    if let Some(error) = error {
        return Err(error.into());
    }

    let mut result = Ok(());
    visit_mut_expr(expr, &mut |expr| match expr {
        ExprPlan::Subquery(query)
        | ExprPlan::Exists {
            subquery: query, ..
        } if result.is_ok() => {
            result = plan_query(schema_map, query, Some(Rc::clone(context)));
        }
        ExprPlan::InSubquery { subquery, .. } if result.is_ok() => {
            result = plan_query(schema_map, subquery, Some(Rc::clone(context)));
        }
        _ => {}
    });
    result
}

enum Context {
    Data {
        alias: String,
        labels: Option<Vec<String>>,
    },
    Bridge {
        left: Rc<Context>,
        right: Rc<Context>,
    },
    Scope {
        local: Rc<Context>,
        outer: Rc<Context>,
    },
    Barrier,
}

#[derive(Debug, PartialEq, Eq)]
enum Resolution {
    Unresolved,
    Resolved(String),
    Ambiguous,
}

impl Context {
    fn all_labels(&self) -> Option<Vec<String>> {
        match self {
            Self::Data { labels, .. } => labels.clone(),
            Self::Bridge { left, right } => Some(
                left.all_labels()?
                    .into_iter()
                    .chain(right.all_labels()?)
                    .collect(),
            ),
            Self::Scope { local, .. } => local.all_labels(),
            Self::Barrier => None,
        }
    }

    fn resolve(&self, qualifier: Option<&str>, name: &str) -> Resolution {
        match self {
            Self::Data { alias, labels } => {
                if let Some(qualifier) = qualifier {
                    if qualifier == alias
                        && labels
                            .as_ref()
                            .is_some_and(|labels| labels.iter().any(|label| label == name))
                    {
                        Resolution::Resolved(alias.clone())
                    } else {
                        Resolution::Unresolved
                    }
                } else {
                    labels.as_ref().map_or(Resolution::Unresolved, |labels| {
                        if labels.iter().any(|label| label == name) {
                            Resolution::Resolved(alias.clone())
                        } else {
                            Resolution::Unresolved
                        }
                    })
                }
            }
            Self::Bridge { left, right } => match (
                left.resolve(qualifier, name),
                right.resolve(qualifier, name),
            ) {
                (Resolution::Unresolved, resolution) | (resolution, Resolution::Unresolved) => {
                    resolution
                }
                (Resolution::Resolved(_), Resolution::Resolved(_))
                | (Resolution::Ambiguous, _)
                | (_, Resolution::Ambiguous) => Resolution::Ambiguous,
            },
            Self::Scope { local, outer } => match local.resolve(qualifier, name) {
                Resolution::Unresolved => outer.resolve(qualifier, name),
                resolution => resolution,
            },
            Self::Barrier => Resolution::Unresolved,
        }
    }
}

#[cfg(test)]
mod tests {
    use {
        super::{
            Context, Resolution, plan, prepare_hash, prepare_left, visit_hash_exprs,
            visit_offset_input_exprs,
        },
        crate::{
            mock::run,
            parse_sql::parse,
            plan::{
                DerivedSourcePlan, DistinctInputPlan, DistinctPlan, ExprPlan, HashJoinInputPlan,
                HashJoinPlan, InnerJoinInputPlan, InnerJoinPlan, JoinConditionInputPlan,
                JoinConditionPlan, LeftOuterJoinInputPlan, LeftOuterJoinPlan,
                NestedLoopJoinInputPlan, NestedLoopJoinPlan, OffsetInputPlan, ProjectInputPlan,
                ProjectPlan, ProjectionPlan, QueryPlan, SelectItemPlan, SourcePlan, StatementPlan,
                TableAccessPlan, TableAliasPlan, TableSourcePlan,
            },
            planner::{PlannerError, fetch_schema_map},
            translate::translate,
        },
    };

    fn planned(sql: &str) -> StatementPlan {
        plan_result(sql).unwrap()
    }

    fn plan_result(sql: &str) -> crate::result::Result<StatementPlan> {
        let storage = run(
            "CREATE TABLE Users (id INTEGER, name TEXT); CREATE TABLE Teams (id INTEGER, team_id INTEGER, title TEXT); CREATE TABLE Logs;",
        );
        let statement = StatementPlan::from(translate(&parse(sql).unwrap().remove(0)).unwrap());
        let schema_map = fetch_schema_map(&storage, &statement).unwrap();
        plan(&schema_map, statement)
    }

    fn assert_ambiguous_reference(sql: &str) {
        assert!(matches!(
            plan_result(sql),
            Err(crate::result::Error::Planner(PlannerError::ColumnReferenceAmbiguous(name))) if name == "id"
        ));
    }

    fn first_projection_expr(projection: &ProjectionPlan) -> Option<&ExprPlan> {
        let ProjectionPlan::SelectItems(items) = projection else {
            return None;
        };
        let SelectItemPlan::Expr { expr, .. } = items.first()? else {
            return None;
        };
        Some(expr)
    }

    #[test]
    fn first_projection_expr_ignores_non_expression_items() {
        assert_eq!(first_projection_expr(&ProjectionPlan::SchemalessMap), None);
        assert_eq!(
            first_projection_expr(&ProjectionPlan::SelectItems(Vec::new())),
            None
        );
        assert_eq!(
            first_projection_expr(&ProjectionPlan::SelectItems(vec![SelectItemPlan::Wildcard])),
            None
        );
    }

    #[test]
    fn resolves_schemaful_references_across_query_stages() {
        assert!(matches!(
            planned("SELECT name FROM Users WHERE id = 1 GROUP BY name ORDER BY name"),
            StatementPlan::Query(QueryPlan::SelectOrderBy(order_by))
                if matches!(first_projection_expr(&order_by.input.projection), Some(ExprPlan::ResolvedColumn { alias, column }) if alias == "Users" && column == "name")
        ));

        assert!(matches!(
            planned("SELECT U.id FROM Users U JOIN Teams T ON U.id = T.team_id"),
            StatementPlan::Query(QueryPlan::Project(project))
                if matches!(first_projection_expr(&project.projection), Some(ExprPlan::ResolvedColumn { alias, column }) if alias == "U" && column == "id")
        ));

        assert!(matches!(
            planned("SELECT column1 FROM (VALUES (1)) AS V ORDER BY column1"),
            StatementPlan::Query(QueryPlan::SelectOrderBy(order_by))
                if matches!(first_projection_expr(&order_by.input.projection), Some(ExprPlan::ResolvedColumn { alias, column }) if alias == "V" && column == "column1")
        ));
    }

    fn table(name: &str) -> SourcePlan {
        SourcePlan::Table(TableSourcePlan {
            name: name.to_owned(),
            alias: None,
            access: TableAccessPlan::FullScan,
        })
    }

    #[test]
    fn visits_hash_and_distinct_inputs() {
        let schema_map = std::collections::HashMap::new();
        let context = std::rc::Rc::new(Context::Data {
            alias: "Users".to_owned(),
            labels: Some(vec!["id".to_owned()]),
        });
        let mut left = crate::plan::LeftOuterJoinPlan {
            input: crate::plan::LeftOuterJoinInputPlan::Condition(JoinConditionPlan {
                input: JoinConditionInputPlan::Hash(HashJoinPlan {
                    input: HashJoinInputPlan::Source(table("Users")),
                    right: table("Teams"),
                    input_key: ExprPlan::UnplannedReference {
                        qualifier: None,
                        name: "id".to_owned(),
                    },
                    right_key: ExprPlan::Literal(crate::ast::Literal::Number(1.into())),
                    right_filter: None,
                }),
                expr: ExprPlan::Value(crate::data::Value::Bool(true)),
            }),
        };
        prepare_left(&schema_map, &mut left, None).unwrap();

        for input in [
            HashJoinInputPlan::InnerJoin(Box::new(InnerJoinPlan {
                input: InnerJoinInputPlan::NestedLoop(NestedLoopJoinPlan {
                    input: NestedLoopJoinInputPlan::Source(table("Users")),
                    right: table("Teams"),
                }),
            })),
            HashJoinInputPlan::LeftOuterJoin(Box::new(LeftOuterJoinPlan {
                input: LeftOuterJoinInputPlan::NestedLoop(NestedLoopJoinPlan {
                    input: NestedLoopJoinInputPlan::Source(table("Users")),
                    right: table("Teams"),
                }),
            })),
        ] {
            let mut hash = HashJoinPlan {
                input,
                right: table("Teams"),
                input_key: ExprPlan::Literal(crate::ast::Literal::Number(1.into())),
                right_key: ExprPlan::Literal(crate::ast::Literal::Number(1.into())),
                right_filter: None,
            };
            prepare_hash(&schema_map, &mut hash, None).unwrap();
        }

        let mut hash = HashJoinPlan {
            input: HashJoinInputPlan::Source(table("Users")),
            right: table("Teams"),
            input_key: ExprPlan::Literal(crate::ast::Literal::Number(1.into())),
            right_key: ExprPlan::Literal(crate::ast::Literal::Number(1.into())),
            right_filter: Some(ExprPlan::UnplannedReference {
                qualifier: None,
                name: "id".to_owned(),
            }),
        };
        visit_hash_exprs(&schema_map, &mut hash, &context).unwrap();
        assert!(matches!(
            hash.right_filter,
            Some(ExprPlan::ResolvedColumn { .. })
        ));

        let mut input = OffsetInputPlan::Distinct(DistinctPlan {
            input: DistinctInputPlan::Project(ProjectPlan {
                input: ProjectInputPlan::Source(table("Users")),
                projection: ProjectionPlan::SelectItems(Vec::new()),
            }),
        });
        visit_offset_input_exprs(&schema_map, &mut input, &context).unwrap();
    }

    #[test]
    fn plans_derived_sources_in_explicit_hash_joins() {
        let storage = run("CREATE TABLE Users (id INTEGER); CREATE TABLE Teams (team_id INTEGER);");
        let StatementPlan::Query(derived_query) = StatementPlan::from(
            translate(&parse("SELECT id FROM Users").unwrap().remove(0)).unwrap(),
        ) else {
            unreachable!()
        };
        let statement = StatementPlan::Query(QueryPlan::Project(ProjectPlan {
            input: ProjectInputPlan::InnerJoin(Box::new(InnerJoinPlan {
                input: InnerJoinInputPlan::Hash(HashJoinPlan {
                    input: HashJoinInputPlan::Source(SourcePlan::Derived(DerivedSourcePlan {
                        query: Box::new(derived_query),
                        alias: TableAliasPlan {
                            name: "Derived".to_owned(),
                            columns: Vec::new(),
                        },
                    })),
                    right: table("Teams"),
                    input_key: ExprPlan::UnplannedReference {
                        qualifier: Some("Derived".to_owned()),
                        name: "id".to_owned(),
                    },
                    right_key: ExprPlan::UnplannedReference {
                        qualifier: Some("Teams".to_owned()),
                        name: "team_id".to_owned(),
                    },
                    right_filter: None,
                }),
            })),
            projection: ProjectionPlan::SelectItems(Vec::new()),
        }));
        let schema_map = fetch_schema_map(&storage, &statement).unwrap();
        let statement = plan(&schema_map, statement).unwrap();

        assert!(matches!(
            statement,
            StatementPlan::Query(QueryPlan::Project(ProjectPlan {
                input: ProjectInputPlan::InnerJoin(join),
                ..
            })) if matches!(
                &join.input,
                InnerJoinInputPlan::Hash(HashJoinPlan {
                    input: HashJoinInputPlan::Source(SourcePlan::Derived(derived)),
                    ..
                }) if matches!(
                    first_projection_expr(&derived.query.project().unwrap().projection),
                    Some(ExprPlan::ResolvedColumn { alias, column }) if alias == "Users" && column == "id"
                )
            )
        ));
    }

    #[test]
    fn scope_context_keeps_local_labels() {
        let local = std::rc::Rc::new(Context::Data {
            alias: "Users".to_owned(),
            labels: Some(vec!["id".to_owned()]),
        });
        let outer = std::rc::Rc::new(Context::Barrier);
        assert_eq!(
            Context::Scope { local, outer }.all_labels(),
            Some(vec!["id".to_owned()])
        );
    }

    #[test]
    fn bridge_and_barrier_contexts_have_no_implicit_columns() {
        let bridge = Context::Bridge {
            left: std::rc::Rc::new(Context::Data {
                alias: "Users".to_owned(),
                labels: Some(vec!["id".to_owned()]),
            }),
            right: std::rc::Rc::new(Context::Data {
                alias: "Teams".to_owned(),
                labels: Some(vec!["id".to_owned(), "team_id".to_owned()]),
            }),
        };

        assert_eq!(
            bridge.all_labels(),
            Some(vec!["id".to_owned(), "id".to_owned(), "team_id".to_owned()])
        );
        assert_eq!(bridge.resolve(None, "missing"), Resolution::Unresolved);
        assert_eq!(bridge.resolve(None, "id"), Resolution::Ambiguous);
        assert_eq!(Context::Barrier.all_labels(), None);
        assert_eq!(Context::Barrier.resolve(None, "id"), Resolution::Unresolved);
    }

    #[test]
    fn rejects_ambiguous_join_predicates_during_planning() {
        assert_ambiguous_reference(
            "SELECT Users.name FROM Users JOIN Teams ON Users.id = Teams.team_id WHERE id = 1",
        );
    }

    #[test]
    fn rejects_ambiguous_references_in_every_query_stage() {
        for sql in [
            "SELECT Users.name FROM Users JOIN Teams ON id = id",
            "SELECT Users.name FROM Users JOIN Teams ON Users.id = Teams.team_id GROUP BY id",
            "SELECT Users.name FROM Users JOIN Teams ON Users.id = Teams.team_id GROUP BY Users.name HAVING id = 1",
            "SELECT Users.name FROM Users JOIN Teams ON Users.id = Teams.team_id ORDER BY id",
            "SELECT id + 1 FROM Users JOIN Teams ON Users.id = Teams.team_id",
        ] {
            assert_ambiguous_reference(sql);
        }
    }

    #[test]
    fn resolves_correlated_references_from_outer_scope() {
        assert!(matches!(
            planned(
                "SELECT U.id FROM Users U WHERE EXISTS (SELECT 1 FROM Teams T WHERE T.team_id = U.id)",
            ),
            StatementPlan::Query(_)
        ));
    }

    #[test]
    fn preserves_unknown_references_for_evaluator_error() {
        let statement = format!("{:?}", planned("SELECT missing FROM Users"));
        assert!(statement.contains("UnplannedReference"));
    }
}
