use {
    super::expr::PlanExpr,
    crate::{
        data::Schema,
        plan::{
            AggregationInputPlan, DistinctInputPlan, DistinctPlan, ExprPlan, FilterInputPlan,
            FilterPlan, HashJoinInputPlan, HashJoinPlan, InnerJoinInputPlan, InnerJoinPlan,
            JoinConditionInputPlan, JoinConditionPlan, LeftOuterJoinInputPlan, LeftOuterJoinPlan,
            LimitInputPlan, LimitPlan, NestedLoopJoinInputPlan, NestedLoopJoinPlan,
            OffsetInputPlan, OffsetPlan, OrderByExprPlan, ProjectInputPlan, ProjectPlan,
            ProjectionPlan, QueryPlan, SelectItemPlan, SelectOrderByPlan, SourcePlan,
            StatementPlan, ValuesOrderByPlan,
        },
        result::Result,
        store::Store,
    },
    std::collections::HashMap,
};

pub fn fetch_schema_map<T: Store + ?Sized>(
    storage: &T,
    statement: &StatementPlan,
) -> Result<HashMap<String, Schema>> {
    match statement {
        StatementPlan::Query(query) => scan_query(storage, query),
        StatementPlan::Insert {
            table_name, source, ..
        } => {
            let table_schema = storage
                .fetch_schema(table_name)?
                .map_or_else(HashMap::new, |schema| {
                    HashMap::from([(table_name.to_owned(), schema)])
                });
            let source_schema_list = scan_query(storage, source)?;
            let schema_list = table_schema.into_iter().chain(source_schema_list).collect();

            Ok(schema_list)
        }
        StatementPlan::CreateTable { name, source, .. } => {
            let table_schema = storage
                .fetch_schema(name)?
                .map_or_else(HashMap::new, |schema| {
                    HashMap::from([(name.to_owned(), schema)])
                });
            let source_schema_list = match source {
                Some(source) => scan_query(storage, source)?,
                None => HashMap::new(),
            };
            let schema_list = table_schema.into_iter().chain(source_schema_list).collect();

            Ok(schema_list)
        }
        StatementPlan::DropTable { names, .. } => {
            let mut schema_map = HashMap::new();
            for table_name in names {
                if let Some(schema) = storage.fetch_schema(table_name)? {
                    schema_map.insert(table_name.clone(), schema);
                }
            }

            Ok(schema_map)
        }
        StatementPlan::Update {
            table_name,
            selection,
            ..
        } => {
            let table_schema = storage
                .fetch_schema(table_name)?
                .map_or_else(HashMap::new, |schema| {
                    HashMap::from([(table_name.to_owned(), schema)])
                });
            let selection_schema = match selection {
                Some(expr) => scan_expr(storage, expr)?,
                None => HashMap::new(),
            };
            Ok(table_schema.into_iter().chain(selection_schema).collect())
        }
        StatementPlan::Delete {
            table_name,
            selection,
        } => {
            let table_schema = storage
                .fetch_schema(table_name)?
                .map_or_else(HashMap::new, |schema| {
                    HashMap::from([(table_name.to_owned(), schema)])
                });
            let selection_schema = match selection {
                Some(expr) => scan_expr(storage, expr)?,
                None => HashMap::new(),
            };
            Ok(table_schema.into_iter().chain(selection_schema).collect())
        }
        _ => Ok(HashMap::new()),
    }
}

fn scan_query<T: Store + ?Sized>(
    storage: &T,
    query: &QueryPlan,
) -> Result<HashMap<String, Schema>> {
    match query {
        QueryPlan::Project(project) => scan_project(storage, project),
        QueryPlan::Values(_) => Ok(HashMap::new()),
        QueryPlan::SelectOrderBy(order_by) => scan_select_order_by(storage, order_by),
        QueryPlan::ValuesOrderBy(order_by) => scan_values_order_by(storage, order_by),
        QueryPlan::Distinct(distinct) => scan_distinct(storage, distinct),
        QueryPlan::Offset(offset) => scan_offset(storage, offset),
        QueryPlan::Limit(LimitPlan { input, count }) => {
            let schema_list = match input {
                LimitInputPlan::Project(project) => scan_project(storage, project)?,
                LimitInputPlan::Values(_) => HashMap::new(),
                LimitInputPlan::SelectOrderBy(order_by) => scan_select_order_by(storage, order_by)?,
                LimitInputPlan::ValuesOrderBy(order_by) => scan_values_order_by(storage, order_by)?,
                LimitInputPlan::Distinct(distinct) => scan_distinct(storage, distinct)?,
                LimitInputPlan::Offset(offset) => scan_offset(storage, offset)?,
            };

            Ok(schema_list
                .into_iter()
                .chain(scan_expr(storage, count)?)
                .collect())
        }
    }
}

fn scan_offset<T: Store + ?Sized>(
    storage: &T,
    OffsetPlan { input, count }: &OffsetPlan,
) -> Result<HashMap<String, Schema>> {
    let schema_list = match input {
        OffsetInputPlan::Project(project) => scan_project(storage, project)?,
        OffsetInputPlan::Values(_) => HashMap::new(),
        OffsetInputPlan::SelectOrderBy(order_by) => scan_select_order_by(storage, order_by)?,
        OffsetInputPlan::ValuesOrderBy(order_by) => scan_values_order_by(storage, order_by)?,
        OffsetInputPlan::Distinct(distinct) => scan_distinct(storage, distinct)?,
    };

    Ok(schema_list
        .into_iter()
        .chain(scan_expr(storage, count)?)
        .collect())
}

fn scan_distinct<T: Store + ?Sized>(
    storage: &T,
    DistinctPlan { input }: &DistinctPlan,
) -> Result<HashMap<String, Schema>> {
    match input {
        DistinctInputPlan::Project(project) => scan_project(storage, project),
        DistinctInputPlan::SelectOrderBy(order_by) => scan_select_order_by(storage, order_by),
    }
}

fn scan_select_order_by<T: Store + ?Sized>(
    storage: &T,
    SelectOrderByPlan { input, exprs }: &SelectOrderByPlan,
) -> Result<HashMap<String, Schema>> {
    let schema_list = scan_project(storage, input)?;

    scan_order_by_exprs(storage, schema_list, exprs)
}

fn scan_values_order_by<T: Store + ?Sized>(
    storage: &T,
    ValuesOrderByPlan { exprs, .. }: &ValuesOrderByPlan,
) -> Result<HashMap<String, Schema>> {
    scan_order_by_exprs(storage, HashMap::new(), exprs)
}

fn scan_order_by_exprs<T: Store + ?Sized>(
    storage: &T,
    schema_list: HashMap<String, Schema>,
    exprs: &[OrderByExprPlan],
) -> Result<HashMap<String, Schema>> {
    let order_by = exprs
        .iter()
        .map(|order_by| scan_expr(storage, &order_by.expr))
        .collect::<Result<Vec<_>>>()?
        .into_iter()
        .flatten();

    Ok(schema_list.into_iter().chain(order_by).collect())
}

fn scan_project<T: Store + ?Sized>(
    storage: &T,
    ProjectPlan { input, projection }: &ProjectPlan,
) -> Result<HashMap<String, Schema>> {
    let schema_list = match input {
        ProjectInputPlan::Source(relation) => scan_source(storage, relation)?,
        ProjectInputPlan::InnerJoin(join) => scan_inner_join(storage, join)?,
        ProjectInputPlan::LeftOuterJoin(join) => scan_left_outer_join(storage, join)?,
        ProjectInputPlan::Filter(filter) => scan_filter(storage, filter)?,
        ProjectInputPlan::Aggregation(aggregation) => {
            let schema_list = scan_aggregation_input(storage, &aggregation.input)?;
            let group_by = aggregation
                .group_by
                .iter()
                .map(|expr| scan_expr(storage, expr))
                .collect::<Result<Vec<_>>>()?
                .into_iter()
                .flatten();

            schema_list.into_iter().chain(group_by).collect()
        }
        ProjectInputPlan::Having(having) => {
            let schema_list = scan_aggregation_input(storage, &having.input.input)?;
            let aggregation = having
                .input
                .group_by
                .iter()
                .chain(std::iter::once(&having.expr))
                .map(|expr| scan_expr(storage, expr))
                .collect::<Result<Vec<_>>>()?
                .into_iter()
                .flatten();

            schema_list.into_iter().chain(aggregation).collect()
        }
    };
    let projection_items = match projection {
        ProjectionPlan::SelectItems(items) => items.as_slice(),
        ProjectionPlan::SchemalessMap => &[],
    };

    let projection = projection_items
        .iter()
        .map(|select_item| match select_item {
            SelectItemPlan::Expr { expr, .. } => scan_expr(storage, expr),
            SelectItemPlan::QualifiedWildcard(_) | SelectItemPlan::Wildcard => Ok(HashMap::new()),
        })
        .collect::<Result<Vec<HashMap<String, Schema>>>>()?
        .into_iter()
        .flatten();

    Ok(schema_list.into_iter().chain(projection).collect())
}

fn scan_filter<T: Store + ?Sized>(
    storage: &T,
    FilterPlan { input, expr }: &FilterPlan,
) -> Result<HashMap<String, Schema>> {
    let input = match input {
        FilterInputPlan::Source(relation) => scan_source(storage, relation)?,
        FilterInputPlan::InnerJoin(join) => scan_inner_join(storage, join)?,
        FilterInputPlan::LeftOuterJoin(join) => scan_left_outer_join(storage, join)?,
    };
    let expr = scan_expr(storage, expr)?;

    Ok(input.into_iter().chain(expr).collect())
}

fn scan_aggregation_input<T: Store + ?Sized>(
    storage: &T,
    input: &AggregationInputPlan,
) -> Result<HashMap<String, Schema>> {
    match input {
        AggregationInputPlan::Source(relation) => scan_source(storage, relation),
        AggregationInputPlan::InnerJoin(join) => scan_inner_join(storage, join),
        AggregationInputPlan::LeftOuterJoin(join) => scan_left_outer_join(storage, join),
        AggregationInputPlan::Filter(filter) => scan_filter(storage, filter),
    }
}

fn scan_inner_join<T: Store + ?Sized>(
    storage: &T,
    join: &InnerJoinPlan,
) -> Result<HashMap<String, Schema>> {
    match &join.input {
        InnerJoinInputPlan::NestedLoop(join) => scan_nested_loop(storage, join),
        InnerJoinInputPlan::Hash(join) => scan_hash(storage, join),
        InnerJoinInputPlan::Condition(condition) => scan_condition(storage, condition),
    }
}

fn scan_left_outer_join<T: Store + ?Sized>(
    storage: &T,
    join: &LeftOuterJoinPlan,
) -> Result<HashMap<String, Schema>> {
    match &join.input {
        LeftOuterJoinInputPlan::NestedLoop(join) => scan_nested_loop(storage, join),
        LeftOuterJoinInputPlan::Hash(join) => scan_hash(storage, join),
        LeftOuterJoinInputPlan::Condition(condition) => scan_condition(storage, condition),
    }
}

fn scan_condition<T: Store + ?Sized>(
    storage: &T,
    condition: &JoinConditionPlan,
) -> Result<HashMap<String, Schema>> {
    let input = match &condition.input {
        JoinConditionInputPlan::NestedLoop(join) => scan_nested_loop(storage, join)?,
        JoinConditionInputPlan::Hash(join) => scan_hash(storage, join)?,
    };
    let expr = scan_expr(storage, &condition.expr)?;

    Ok(input.into_iter().chain(expr).collect())
}

fn scan_nested_loop<T: Store + ?Sized>(
    storage: &T,
    join: &NestedLoopJoinPlan,
) -> Result<HashMap<String, Schema>> {
    let input = match &join.input {
        NestedLoopJoinInputPlan::Source(source) => scan_source(storage, source)?,
        NestedLoopJoinInputPlan::InnerJoin(join) => scan_inner_join(storage, join)?,
        NestedLoopJoinInputPlan::LeftOuterJoin(join) => scan_left_outer_join(storage, join)?,
    };
    let right = scan_source(storage, &join.right)?;

    Ok(input.into_iter().chain(right).collect())
}

fn scan_hash<T: Store + ?Sized>(
    storage: &T,
    join: &HashJoinPlan,
) -> Result<HashMap<String, Schema>> {
    let input = match &join.input {
        HashJoinInputPlan::Source(source) => scan_source(storage, source)?,
        HashJoinInputPlan::InnerJoin(join) => scan_inner_join(storage, join)?,
        HashJoinInputPlan::LeftOuterJoin(join) => scan_left_outer_join(storage, join)?,
    };
    let expressions = [&join.input_key, &join.right_key]
        .into_iter()
        .chain(join.right_filter.iter())
        .map(|expr| scan_expr(storage, expr))
        .collect::<Result<Vec<_>>>()?
        .into_iter()
        .flatten();
    let right = scan_source(storage, &join.right)?;

    Ok(input.into_iter().chain(right).chain(expressions).collect())
}

fn scan_source<T>(storage: &T, source: &SourcePlan) -> Result<HashMap<String, Schema>>
where
    T: Store + ?Sized,
{
    match source {
        SourcePlan::Table(table) => {
            let schema = storage.fetch_schema(&table.name)?;
            let schema_list: HashMap<String, Schema> = schema.map_or_else(HashMap::new, |schema| {
                HashMap::from([(table.name.clone(), schema)])
            });

            Ok(schema_list)
        }
        SourcePlan::Derived(derived) => scan_query(storage, &derived.query),
        SourcePlan::Series(_) | SourcePlan::Dictionary(_) => Ok(HashMap::new()),
    }
}

fn scan_expr<T>(storage: &T, expr: &ExprPlan) -> Result<HashMap<String, Schema>>
where
    T: Store + ?Sized,
{
    let schema_list = match expr.into() {
        PlanExpr::None | PlanExpr::Identifier(_) | PlanExpr::CompoundIdentifier { .. } => {
            HashMap::new()
        }
        PlanExpr::Expr(expr) => scan_expr(storage, expr)?,
        PlanExpr::TwoExprs(expr, expr2) => scan_expr(storage, expr)?
            .into_iter()
            .chain(scan_expr(storage, expr2)?)
            .collect(),
        PlanExpr::ThreeExprs(expr, expr2, expr3) => scan_expr(storage, expr)?
            .into_iter()
            .chain(scan_expr(storage, expr2)?)
            .chain(scan_expr(storage, expr3)?)
            .collect(),
        PlanExpr::MultiExprs(exprs) => exprs
            .iter()
            .map(|expr| scan_expr(storage, expr))
            .collect::<Result<Vec<HashMap<String, Schema>>>>()?
            .into_iter()
            .flatten()
            .collect(),
        PlanExpr::Query(query) => scan_query(storage, query)?,
        PlanExpr::QueryAndExpr { query, expr } => scan_query(storage, query)?
            .into_iter()
            .chain(scan_expr(storage, expr)?)
            .collect(),
    };

    Ok(schema_list)
}

#[cfg(test)]
mod tests {
    use {
        super::fetch_schema_map,
        crate::{
            mock::{MockStorage, run},
            parse_sql::parse,
            result::Result,
            translate::translate,
        },
    };

    fn plan(storage: &MockStorage, sql: &str) -> Result<Vec<String>> {
        let parsed = parse(sql).expect(sql).into_iter().next().unwrap();
        let statement = translate(&parsed).unwrap().into();
        let schema_map = fetch_schema_map(storage, &statement);

        let mut schema_names = schema_map?.into_keys().collect::<Vec<_>>();
        schema_names.sort();

        Ok(schema_names)
    }

    fn run_test(storage: &MockStorage, sql: &str, expected: &[&str]) {
        let actual = plan(storage, sql).unwrap();
        let actual = actual.as_slice();

        assert_eq!(actual, expected, "{sql}");
    }

    #[test]
    fn basic() {
        let storage = run("
            CREATE TABLE Foo (id INTEGER);
            CREATE TABLE Bar (name TEXT);
        ");

        let test = |sql, expected| run_test(&storage, sql, expected);

        test("SELECT * FROM Foo", &["Foo"]);
        test("INSERT INTO Foo VALUES (1), (2), (3);", &["Foo"]);
        test("DROP TABLE Foo, Bar;", &["Bar", "Foo"]);
        test("UPDATE Foo SET id = 1;", &["Foo"]);
        test("DELETE FROM Foo;", &["Foo"]);
    }

    #[test]
    fn expr() {
        let storage = run("
            CREATE TABLE Foo (id INTEGER);
            CREATE TABLE Bar (name TEXT);
        ");
        let test = |sql, expected| run_test(&storage, sql, expected);

        // PlanExpr::None
        test(
            r#"SELECT Foo.*, * FROM Foo WHERE id = DATE "2021-01-01";"#,
            &["Foo"],
        );

        // PlanExpr::Expr
        test(
            "
            SELECT * FROM Foo
            WHERE
                Foo.id IS NULL
                AND id IS NOT NULL
                OR (id IS NULL)
        ",
            &["Foo"],
        );

        // PlanExpr::TwoExprs
        test("SELECT * FROM Foo WHERE id = 1", &["Foo"]);

        // PlanExpr::ThreeExprs
        test("SELECT * FROM Foo WHERE id BETWEEN 1 AND 20", &["Foo"]);

        // PlanExpr::MultiExprs
        test("SELECT * FROM Foo WHERE id IN (1, 2, 3)", &["Foo"]);

        // PlanExpr::Query
        test(
            "
            SELECT * FROM Bar
            WHERE
                EXISTS(SELECT id FROM Foo)
                AND Bar.id = (SELECT id FROM Bar LIMIT 1);
        ",
            &["Bar", "Foo"],
        );

        // PlanExpr::QueryAndExpr
        test(
            "SELECT * FROM Foo WHERE Foo.id IN (SELECT 1 FROM Bar);",
            &["Bar", "Foo"],
        );
    }

    #[test]
    fn select() {
        let storage = run("
            CREATE TABLE Foo (id INTEGER);
            CREATE TABLE Bar (
                id INTEGER,
                foo_id INTEGER
            );
            CREATE TABLE Baz (flag BOOLEAN);
        ");

        let test = |sql, expected| run_test(&storage, sql, expected);

        test(
            "
            SELECT foo_id, COUNT(*)
            FROM Bar
            WHERE id IS NOT NULL
            GROUP BY foo_id
            HAVING foo_id > 10;
            ",
            &["Bar"],
        );
        test(
            "SELECT * FROM Foo JOIN Bar ORDER BY Foo.id",
            &["Bar", "Foo"],
        );
        test("SELECT * FROM Foo LEFT OUTER JOIN Bar", &["Bar", "Foo"]);
        test(
            "SELECT * FROM Foo LEFT JOIN Bar ON Bar.foo_id = Foo.id",
            &["Bar", "Foo"],
        );
        test(
            "
            SELECT * FROM Foo
            INNER JOIN Bar ON Bar.id = Foo.bar_id
            LEFT JOIN Baz ON False;
        ",
            &["Bar", "Baz", "Foo"],
        );
        test(
            "
            SELECT Bar.*, id, *
            FROM Foo
            JOIN Bar ON True
            LEFT JOIN Baz ON True
            WHERE Foo.id = 1
            LIMIT 1 OFFSET 1
            ",
            &["Bar", "Baz", "Foo"],
        );

        // ignore rather than returning error
        test("SELECT * FROM Railway", &[]);
        test("SELECT * FROM Foo WHERE Foo.id = Lab.foo_id", &["Foo"]);
    }

    #[test]
    fn storage_err() {
        let storage = run("
            CREATE TABLE Foo (id INTEGER);
            CREATE TABLE Bar (id INTEGER);
            CREATE TABLE Baz (flag BOOLEAN);
        ");

        let test = |sql| assert!(plan(&storage, sql).is_err(), "{sql}");

        test("SELECT * FROM __Err__");
        test("INSERT INTO __Err__ VALUES (1), (2)");
        test("DROP TABLE __Err__");

        test("SELECT * FROM Foo WHERE id = (SELECT foo_id FROM __Err__ LIMIT 1)");
        test("SELECT * FROM Foo WHERE (SELECT foo_id FROM __Err__ LIMIT 1) = id");
        test("SELECT * FROM Foo WHERE id BETWEEN (SELECT foo_id FROM __Err__ LIMIT 1) AND 100");
        test("SELECT * FROM Foo WHERE (SELECT id FROM __Err__ LIMIT 1) BETWEEN 20 AND 50");
        test("SELECT * FROM Foo WHERE id IN (1, 2, (SELECT foo_id FROM __Err__ LIMIT 1), 5)");
        test("SELECT * FROM Foo WHERE id IN (SELECT * FROM __Err__)");
        test("SELECT * FROM Foo LEFT JOIN Bar ON Bar.id = (SELECT id FROM __Err__ LIMIT 1)");
        test("SELECT id, (SELECT id FROM __Err__ LIMIT 1) AS cc FROM Foo;");
    }
}
