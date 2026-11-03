use {
    super::{
        Column, Context,
        expr::{self, ExprType},
        source::{self, Scope},
    },
    crate::{
        ast::DataType,
        plan::{
            DistinctInputPlan, DistinctPlan, ExprPlan, LimitInputPlan, OffsetInputPlan, OffsetPlan,
            ProjectPlan, ProjectionPlan, QueryPlan, SelectItemPlan, SourcePlan, ValuesPlan,
        },
        planner::{
            PlannerError,
            visit_mut::{
                VisitorMut, walk_expr_mut, walk_project_mut, walk_query_mut, walk_source_mut,
            },
        },
        result::{Error, Result},
    },
    std::hash::BuildHasher,
};

#[derive(Debug, PartialEq)]
pub(super) enum QueryColumns {
    Columns(Vec<Column>),
    Schemaless,
}

pub(super) fn plan_query<S: BuildHasher>(
    context: &mut Context<'_, S>,
    query: &mut QueryPlan,
    outer: Option<&Scope<'_>>,
    expected: Option<&[DataType]>,
) -> Result<QueryColumns> {
    let scope = {
        let mut sources = PrepareSources {
            context,
            scope: Scope {
                relations: Vec::new(),
                outer,
            },
        };
        sources.visit_query(query)?;
        sources.scope
    };
    match output_mut(query) {
        OutputMut::Project(project) => walk_project_mut(
            &mut PrepareExpressions {
                context,
                scope: &scope,
            },
            project,
        )?,
        OutputMut::Values(ValuesPlan(rows)) => {
            for expr in rows.iter_mut().flatten() {
                expr::prepare_expr(context, &scope, expr, None)?;
            }
        }
    }
    let output = infer_output(context, query, &scope)?;
    if let QueryColumns::Columns(columns) = &output {
        if let Some(expected) = expected {
            check_width(expected.len(), columns.len())?;
        }
        match output_mut(query) {
            OutputMut::Project(project) => {
                if let ProjectionPlan::SelectItems(items) = &mut project.projection {
                    let mut index = 0;
                    for item in items {
                        match item {
                            SelectItemPlan::Expr { expr, label } => {
                                let target = expected
                                    .and_then(|types| types.get(index))
                                    .cloned()
                                    .or_else(|| columns[index].expr_type.data_type());
                                if let Some(target) = target {
                                    expr::prepare_expr(
                                        context,
                                        &scope,
                                        expr,
                                        Some((&target, label)),
                                    )?;
                                }
                                index += 1;
                            }
                            SelectItemPlan::Wildcard => index += scope.wildcard(None)?.len(),
                            SelectItemPlan::QualifiedWildcard(alias) => {
                                index += scope.wildcard(Some(alias))?.len();
                            }
                        }
                    }
                }
            }
            OutputMut::Values(ValuesPlan(rows)) => {
                for row in rows {
                    for (index, value) in row.iter_mut().enumerate() {
                        let target = expected
                            .and_then(|types| types.get(index))
                            .cloned()
                            .or_else(|| columns[index].expr_type.data_type());
                        if let Some(target) = target {
                            expr::prepare_expr(
                                context,
                                &scope,
                                value,
                                Some((&target, &columns[index].name)),
                            )?;
                        }
                    }
                }
            }
        }
    }
    // ORDER BY can use output aliases. Its expressions do not determine CTAS
    // column types; only keep its aggregate copies aligned with execution slots.
    walk_query_mut(
        &mut PrepareAggregateCopies {
            context,
            scope: &scope,
        },
        query,
    )?;
    let output = infer_output(context, query, &scope)?;
    if let (Some(expected), QueryColumns::Columns(columns)) = (expected, &output) {
        for (expected, column) in expected.iter().zip(columns) {
            expr::check_type(&column.expr_type, expected, &column.name)?;
        }
    }
    Ok(output)
}

pub(super) fn infer_query<S: BuildHasher>(
    context: &mut Context<'_, S>,
    query: &QueryPlan,
    outer: Option<&Scope<'_>>,
) -> Result<QueryColumns> {
    let scope = source::infer_scope(context, query, outer)?;
    infer_output(context, query, &scope)
}

fn infer_output<S: BuildHasher>(
    context: &mut Context<'_, S>,
    query: &QueryPlan,
    scope: &Scope<'_>,
) -> Result<QueryColumns> {
    if let Some(project) = query.project() {
        let ProjectionPlan::SelectItems(items) = &project.projection else {
            return Ok(QueryColumns::Schemaless);
        };
        let mut columns = Vec::new();
        for item in items {
            match item {
                SelectItemPlan::Expr { expr, label } => columns.push(Column {
                    name: label.clone(),
                    expr_type: expr::infer_type(context, scope, expr)?,
                }),
                SelectItemPlan::Wildcard => columns.extend(scope.wildcard(None)?),
                SelectItemPlan::QualifiedWildcard(alias) => {
                    columns.extend(scope.wildcard(Some(alias))?);
                }
            }
        }
        return Ok(QueryColumns::Columns(columns));
    }
    let ValuesPlan(rows) = values(query).ok_or(PlannerError::MalformedPlanEmptyCtasValues)?;
    let first = rows
        .first()
        .ok_or(PlannerError::MalformedPlanEmptyCtasValues)?;
    let mut columns: Vec<_> = (0..first.len())
        .map(|index| Column {
            name: format!("column{}", index + 1),
            expr_type: ExprType::Null,
        })
        .collect();
    for (index, row) in rows.iter().enumerate() {
        if row.len() != first.len() {
            return Err(PlannerError::CtasValuesLengthMismatch {
                row: index + 1,
                expected: first.len(),
                found: row.len(),
            }
            .into());
        }
        for (column, expr) in columns.iter_mut().zip(row) {
            if column.expr_type == ExprType::Null {
                column.expr_type = expr::infer_type(context, scope, expr)?;
            }
        }
    }
    Ok(QueryColumns::Columns(columns))
}

pub(super) fn check_width(expected: usize, found: usize) -> Result<()> {
    if expected == found {
        Ok(())
    } else {
        Err(PlannerError::CtasColumnCountMismatch { expected, found }.into())
    }
}

struct PrepareSources<'a, 'b, 'c, S> {
    context: &'a mut Context<'b, S>,
    scope: Scope<'c>,
}

impl<S: BuildHasher> VisitorMut for PrepareSources<'_, '_, '_, S> {
    type Error = Error;

    fn visit_source(&mut self, source: &mut SourcePlan) -> Result<()> {
        if let SourcePlan::Derived(derived) = source {
            plan_query(self.context, &mut derived.query, Some(&self.scope), None)?;
        }
        let relation = source::infer_source(self.context, source, Some(&self.scope))?;
        self.scope.relations.push(relation);
        Ok(())
    }

    fn visit_expr(&mut self, _: &mut ExprPlan) -> Result<()> {
        Ok(())
    }
}

struct PrepareExpressions<'a, 'b, 'c, S> {
    context: &'a mut Context<'b, S>,
    scope: &'c Scope<'c>,
}

struct PrepareAggregateCopies<'a, 'b, 'c, S> {
    context: &'a mut Context<'b, S>,
    scope: &'c Scope<'c>,
}

impl<S: BuildHasher> VisitorMut for PrepareAggregateCopies<'_, '_, '_, S> {
    type Error = Error;

    fn visit_expr(&mut self, expr: &mut ExprPlan) -> Result<()> {
        match expr {
            ExprPlan::Aggregate(_) => expr::prepare_expr(self.context, self.scope, expr, None),
            _ => walk_expr_mut(self, expr),
        }
    }

    fn visit_query(&mut self, _: &mut QueryPlan) -> Result<()> {
        Ok(())
    }

    fn visit_source(&mut self, _: &mut SourcePlan) -> Result<()> {
        Ok(())
    }
}

impl<S: BuildHasher> VisitorMut for PrepareExpressions<'_, '_, '_, S> {
    type Error = Error;

    fn visit_expr(&mut self, expr: &mut ExprPlan) -> Result<()> {
        expr::prepare_expr(self.context, self.scope, expr, None)
    }

    fn visit_source(&mut self, source: &mut SourcePlan) -> Result<()> {
        match source {
            SourcePlan::Derived(_) => Ok(()),
            _ => walk_source_mut(self, source),
        }
    }
}

enum OutputMut<'a> {
    Project(&'a mut ProjectPlan),
    Values(&'a mut ValuesPlan),
}

fn output_mut(query: &mut QueryPlan) -> OutputMut<'_> {
    match query {
        QueryPlan::Project(project) => OutputMut::Project(project),
        QueryPlan::Values(values) => OutputMut::Values(values),
        QueryPlan::SelectOrderBy(order) => OutputMut::Project(&mut order.input),
        QueryPlan::ValuesOrderBy(order) => OutputMut::Values(&mut order.input),
        QueryPlan::Distinct(distinct) => distinct_output_mut(distinct),
        QueryPlan::Offset(offset) => offset_output_mut(offset),
        QueryPlan::Limit(limit) => match &mut limit.input {
            LimitInputPlan::Project(project) => OutputMut::Project(project),
            LimitInputPlan::Values(values) => OutputMut::Values(values),
            LimitInputPlan::SelectOrderBy(order) => OutputMut::Project(&mut order.input),
            LimitInputPlan::ValuesOrderBy(order) => OutputMut::Values(&mut order.input),
            LimitInputPlan::Distinct(distinct) => distinct_output_mut(distinct),
            LimitInputPlan::Offset(offset) => offset_output_mut(offset),
        },
    }
}

fn offset_output_mut(offset: &mut OffsetPlan) -> OutputMut<'_> {
    match &mut offset.input {
        OffsetInputPlan::Project(project) => OutputMut::Project(project),
        OffsetInputPlan::Values(values) => OutputMut::Values(values),
        OffsetInputPlan::SelectOrderBy(order) => OutputMut::Project(&mut order.input),
        OffsetInputPlan::ValuesOrderBy(order) => OutputMut::Values(&mut order.input),
        OffsetInputPlan::Distinct(distinct) => distinct_output_mut(distinct),
    }
}

fn distinct_output_mut(distinct: &mut DistinctPlan) -> OutputMut<'_> {
    match &mut distinct.input {
        DistinctInputPlan::Project(project) => OutputMut::Project(project),
        DistinctInputPlan::SelectOrderBy(order) => OutputMut::Project(&mut order.input),
    }
}

fn values(query: &QueryPlan) -> Option<&ValuesPlan> {
    match query {
        QueryPlan::Values(values) => Some(values),
        QueryPlan::ValuesOrderBy(order) => Some(&order.input),
        QueryPlan::Offset(offset) => offset_values(offset),
        QueryPlan::Limit(limit) => match &limit.input {
            LimitInputPlan::Values(values) => Some(values),
            LimitInputPlan::ValuesOrderBy(order) => Some(&order.input),
            LimitInputPlan::Offset(offset) => offset_values(offset),
            _ => None,
        },
        _ => None,
    }
}

fn offset_values(offset: &OffsetPlan) -> Option<&ValuesPlan> {
    match &offset.input {
        OffsetInputPlan::Values(values) => Some(values),
        OffsetInputPlan::ValuesOrderBy(order) => Some(&order.input),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use {
        super::{Context, QueryColumns, infer_query, plan_query},
        crate::{
            ast::DataType,
            data::Value,
            mock::run,
            parse_sql::parse,
            plan::{
                AggregateExprPlan, AggregateFunctionPlan, ExprPlan, FunctionPlan, ProjectInputPlan,
                ProjectionPlan, QueryPlan, SelectItemPlan, StatementPlanKind, ValuesPlan,
            },
            planner::{
                PlannerError,
                create_table_columns::{Column, expr::ExprType},
                fetch_schema_map, plan_aggregate,
            },
            result::Error,
            translate::translate,
        },
    };

    fn query(sql: &str) -> QueryPlan {
        let statement = plan_aggregate(translate(&parse(sql).expect(sql)[0]).expect(sql).into());
        let StatementPlanKind::Query(query) = statement.kind else {
            panic!("{sql}")
        };
        query
    }

    #[test]
    fn terminal_outputs() {
        let storage = run("CREATE TABLE src (id INT)");
        let schemas = fetch_schema_map(
            &storage,
            &StatementPlanKind::Query(query("SELECT * FROM src")).into(),
        )
        .unwrap();
        let mut context = Context {
            schemas: &schemas,
            functions: &[],
            visiting_functions: Vec::new(),
        };
        let mut test = |sql: &str, expected: &str| {
            let mut actual = query(sql);
            let output = plan_query(&mut context, &mut actual, None, None);
            let columns = QueryColumns::Columns(vec![Column::new("id", DataType::Int)]);
            assert_eq!(output, Ok(columns), "{sql}");
            assert_eq!(actual, query(expected), "source operators: {sql}");
        };
        test("SELECT id FROM src", "SELECT id FROM src");
        test(
            "SELECT id FROM src ORDER BY id",
            "SELECT id FROM src ORDER BY id",
        );
        test("SELECT DISTINCT id FROM src", "SELECT DISTINCT id FROM src");
        test(
            "SELECT DISTINCT id FROM src ORDER BY id",
            "SELECT DISTINCT id FROM src ORDER BY id",
        );
        test("SELECT id FROM src OFFSET 1", "SELECT id FROM src OFFSET 1");
        test(
            "SELECT id FROM src ORDER BY id OFFSET 1",
            "SELECT id FROM src ORDER BY id OFFSET 1",
        );
        test(
            "SELECT DISTINCT id FROM src OFFSET 1",
            "SELECT DISTINCT id FROM src OFFSET 1",
        );
        test("SELECT id FROM src LIMIT 1", "SELECT id FROM src LIMIT 1");
        test(
            "SELECT id FROM src ORDER BY id LIMIT 1",
            "SELECT id FROM src ORDER BY id LIMIT 1",
        );
        test(
            "SELECT DISTINCT id FROM src LIMIT 1",
            "SELECT DISTINCT id FROM src LIMIT 1",
        );
        test(
            "SELECT DISTINCT id FROM src ORDER BY id LIMIT 1 OFFSET 1",
            "SELECT DISTINCT id FROM src ORDER BY id LIMIT 1 OFFSET 1",
        );

        let sql = "SELECT id AS renamed FROM src ORDER BY COALESCE(renamed, 0)";
        let mut actual = query(sql);
        let columns = QueryColumns::Columns(vec![Column::new("renamed", DataType::Int)]);
        assert_eq!(
            plan_query(&mut context, &mut actual, None, None),
            Ok(columns)
        );
        assert_eq!(
            actual,
            query(sql),
            "ORDER BY aliases are not source columns"
        );
    }

    #[test]
    fn values_output() {
        let schemas = std::collections::HashMap::new();
        let mut context = Context {
            schemas: &schemas,
            functions: &[],
            visiting_functions: Vec::new(),
        };
        let mut actual = query("VALUES (CAST(NULL AS FLOAT)), (2)");
        let expected = QueryPlan::Values(ValuesPlan(vec![
            vec![ExprPlan::Function(Box::new(FunctionPlan::Cast {
                expr: ExprPlan::Value(Value::Null),
                data_type: DataType::Float,
            }))],
            vec![ExprPlan::Value(Value::F64(2.0))],
        ]));
        let columns = QueryColumns::Columns(vec![Column::new("column1", DataType::Float)]);
        assert_eq!(
            plan_query(&mut context, &mut actual, None, None),
            Ok(columns)
        );
        assert_eq!(
            actual, expected,
            "typed NULL supplies the type before execution"
        );

        let mut actual = query("VALUES (NULL), (2)");
        let expected = QueryPlan::Values(ValuesPlan(vec![
            vec![ExprPlan::Value(Value::Null)],
            vec![ExprPlan::Value(Value::I64(2))],
        ]));
        let columns = QueryColumns::Columns(vec![Column::new("column1", DataType::Int)]);
        assert_eq!(
            plan_query(&mut context, &mut actual, None, None),
            Ok(columns)
        );
        assert_eq!(actual, expected, "untyped NULL is skipped");

        let mut actual = query("VALUES (NULL), (NULL)");
        let columns = QueryColumns::Columns(vec![Column {
            name: "column1".to_owned(),
            expr_type: ExprType::Null,
        }]);
        assert_eq!(
            plan_query(&mut context, &mut actual, None, None),
            Ok(columns),
            "nested untyped output remains available to an outer CAST"
        );

        let mut actual = query("VALUES ([1][1]), (2)");
        let columns = QueryColumns::Columns(vec![Column {
            name: "column1".to_owned(),
            expr_type: ExprType::Unknown,
        }]);
        assert_eq!(
            plan_query(&mut context, &mut actual, None, None),
            Ok(columns),
            "unknown is not skipped to select a later type"
        );

        let mut actual = query("VALUES (2), (CAST(1 AS FLOAT))");
        let expected = Err(Error::Planner(PlannerError::CtasColumnTypeMismatch {
            column: "column1".to_owned(),
            expected: DataType::Int,
            found: DataType::Float,
        }));
        assert_eq!(
            plan_query(&mut context, &mut actual, None, None),
            expected,
            "later typed values do not widen the first type"
        );

        let mut actual = query("VALUES (1), (2, 3)");
        let expected = Err(Error::Planner(PlannerError::CtasValuesLengthMismatch {
            row: 2,
            expected: 1,
            found: 2,
        }));
        assert_eq!(
            plan_query(&mut context, &mut actual, None, None),
            expected,
            "every row width is checked"
        );

        let mut actual = QueryPlan::Values(ValuesPlan(Vec::new()));
        assert_eq!(
            plan_query(&mut context, &mut actual, None, None),
            Err(Error::Planner(PlannerError::MalformedPlanEmptyCtasValues))
        );
    }

    #[test]
    fn values_terminals() {
        let schemas = std::collections::HashMap::new();
        let mut context = Context {
            schemas: &schemas,
            functions: &[],
            visiting_functions: Vec::new(),
        };
        let mut test = |sql: &str| {
            let mut actual = query(sql);
            let expected = query(sql);
            let columns = QueryColumns::Columns(vec![Column::new("column1", DataType::Int)]);
            assert_eq!(
                plan_query(&mut context, &mut actual, None, None),
                Ok(columns),
                "{sql}"
            );
            assert_eq!(
                actual, expected,
                "typed VALUES preserve terminal expressions: {sql}"
            );
        };
        test("VALUES (CAST(1 AS INT))");
        test("VALUES (CAST(1 AS INT)) ORDER BY column1");
        test("VALUES (CAST(1 AS INT)) OFFSET 1");
        test("VALUES (CAST(1 AS INT)) ORDER BY column1 OFFSET 1");
        test("VALUES (CAST(1 AS INT)) LIMIT 1");
        test("VALUES (CAST(1 AS INT)) ORDER BY column1 LIMIT 1");
        test("VALUES (CAST(1 AS INT)) LIMIT 1 OFFSET 1");
    }

    #[test]
    fn aggregate_slots() {
        let storage = run("CREATE TABLE src (flag BOOLEAN, amount FLOAT)");
        let mut actual = query(
            "
            SELECT SUM(CASE WHEN flag THEN 0 ELSE amount END) AS total FROM src
            HAVING SUM(CASE WHEN flag THEN 0 ELSE amount END) > 0
            ORDER BY SUM(CASE WHEN flag THEN 0 ELSE amount END)
        ",
        );
        let schemas =
            fetch_schema_map(&storage, &StatementPlanKind::Query(actual.clone()).into()).unwrap();
        let mut context = Context {
            schemas: &schemas,
            functions: &[],
            visiting_functions: Vec::new(),
        };
        let expected_expr = ExprPlan::Case {
            operand: None,
            when_then: vec![(
                ExprPlan::Identifier("flag".to_owned()),
                ExprPlan::Value(Value::F64(0.0)),
            )],
            else_result: Some(Box::new(ExprPlan::Identifier("amount".to_owned()))),
        };
        let expected_aggregate = AggregateExprPlan {
            func: AggregateFunctionPlan::Sum(expected_expr),
            distinct: false,
            slot: Some(0),
        };
        let mut expected = query(
            "
            SELECT SUM(amount) AS total FROM src
            HAVING SUM(amount) > 0 ORDER BY SUM(amount)
        ",
        );
        let QueryPlan::SelectOrderBy(order) = &mut expected else {
            panic!("select order by")
        };
        let ProjectInputPlan::Having(having) = &mut order.input.input else {
            panic!("having")
        };
        having.input.aggregate_slots[0] = expected_aggregate.clone();
        let ExprPlan::BinaryOp { left, .. } = &mut having.expr else {
            panic!("having predicate")
        };
        **left = ExprPlan::Aggregate(Box::new(expected_aggregate.clone()));
        order.input.projection = ProjectionPlan::SelectItems(vec![SelectItemPlan::Expr {
            label: "total".to_owned(),
            expr: ExprPlan::Aggregate(Box::new(expected_aggregate.clone())),
        }]);
        order.exprs[0].expr = ExprPlan::Aggregate(Box::new(expected_aggregate));
        let columns = QueryColumns::Columns(vec![Column::new("total", DataType::Float)]);
        assert_eq!(
            plan_query(&mut context, &mut actual, None, None),
            Ok(columns)
        );
        assert_eq!(
            actual, expected,
            "the execution slot and projection/HAVING/ORDER BY copies are prepared together"
        );
    }

    #[test]
    fn derived_query() {
        let schemas = std::collections::HashMap::new();
        let mut context = Context {
            schemas: &schemas,
            functions: &[],
            visiting_functions: Vec::new(),
        };
        let mut actual = query("SELECT v.x FROM (VALUES (CAST(NULL AS FLOAT)), (2)) v(x)");
        let columns = QueryColumns::Columns(vec![Column::new("x", DataType::Float)]);
        assert_eq!(
            plan_query(&mut context, &mut actual, None, None),
            Ok(columns)
        );
        assert_eq!(
            infer_query(&mut context, &actual, None),
            Ok(QueryColumns::Columns(vec![Column::new(
                "x",
                DataType::Float
            )]))
        );

        let mut actual = query(
            "SELECT d.amount FROM (VALUES (CAST(2 AS FLOAT))) v(amount) JOIN (SELECT COALESCE(v.amount, 0) AS amount) d",
        );
        let columns = QueryColumns::Columns(vec![Column::new("amount", DataType::Float)]);
        assert_eq!(
            plan_query(&mut context, &mut actual, None, None),
            Ok(columns),
            "a derived source can use a preceding source's type"
        );
    }
}
