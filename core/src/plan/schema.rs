use {
    super::expr::PlanExpr,
    crate::{
        data::Schema,
        plan::{
            ExprPlan, JoinConstraintPlan, JoinOperatorPlan, JoinPlan, LimitInputPlan, LimitPlan,
            OffsetInputPlan, OffsetPlan, OrderByPlan, ProjectionPlan, QueryPlan, SelectItemPlan,
            SelectPlan, SetExprPlan, StatementPlan, TableFactorPlan, TableWithJoinsPlan,
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
        QueryPlan::Body(body) => scan_set_expr(storage, body),
        QueryPlan::OrderBy(order_by) => scan_order_by(storage, order_by),
        QueryPlan::Offset(offset) => scan_offset(storage, offset),
        QueryPlan::Limit(LimitPlan { input, count }) => {
            let schema_list = match input {
                LimitInputPlan::Body(body) => scan_set_expr(storage, body)?,
                LimitInputPlan::OrderBy(order_by) => scan_order_by(storage, order_by)?,
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
        OffsetInputPlan::Body(body) => scan_set_expr(storage, body)?,
        OffsetInputPlan::OrderBy(order_by) => scan_order_by(storage, order_by)?,
    };

    Ok(schema_list
        .into_iter()
        .chain(scan_expr(storage, count)?)
        .collect())
}

fn scan_order_by<T: Store + ?Sized>(
    storage: &T,
    OrderByPlan { input, exprs }: &OrderByPlan,
) -> Result<HashMap<String, Schema>> {
    let order_by = exprs
        .iter()
        .map(|order_by| scan_expr(storage, &order_by.expr))
        .collect::<Result<Vec<_>>>()?
        .into_iter()
        .flatten();

    Ok(scan_set_expr(storage, input)?
        .into_iter()
        .chain(order_by)
        .collect())
}

fn scan_set_expr<T: Store + ?Sized>(
    storage: &T,
    body: &SetExprPlan,
) -> Result<HashMap<String, Schema>> {
    let schema_list = match body {
        SetExprPlan::Select(select) => scan_select(storage, select)?,
        SetExprPlan::Values(_) => HashMap::new(),
    };

    Ok(schema_list)
}

fn scan_select<T: Store + ?Sized>(
    storage: &T,
    select: &SelectPlan,
) -> Result<HashMap<String, Schema>> {
    let SelectPlan {
        distinct: _,
        projection,
        from,
        selection,
        group_by,
        having,
        ..
    } = select;

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

    let from = scan_table_with_joins(storage, from)?;

    let exprs = selection.iter().chain(group_by.iter()).chain(having.iter());

    Ok(exprs
        .map(|expr| scan_expr(storage, expr))
        .collect::<Result<Vec<HashMap<String, Schema>>>>()?
        .into_iter()
        .flatten()
        .chain(projection)
        .chain(from)
        .collect())
}

fn scan_table_with_joins<T: Store + ?Sized>(
    storage: &T,
    table_with_joins: &TableWithJoinsPlan,
) -> Result<HashMap<String, Schema>> {
    let TableWithJoinsPlan { relation, joins } = table_with_joins;
    let schema_list = scan_table_factor(storage, relation)?;

    Ok(joins
        .iter()
        .map(|join| scan_join(storage, join))
        .collect::<Result<Vec<HashMap<String, Schema>>>>()?
        .into_iter()
        .flatten()
        .chain(schema_list)
        .collect())
}

fn scan_join<T: Store + ?Sized>(storage: &T, join: &JoinPlan) -> Result<HashMap<String, Schema>> {
    let JoinPlan {
        relation,
        join_operator,
        ..
    } = join;

    let schema_list = scan_table_factor(storage, relation)?;
    let schema_list = match join_operator {
        JoinOperatorPlan::Inner(JoinConstraintPlan::On(expr))
        | JoinOperatorPlan::LeftOuter(JoinConstraintPlan::On(expr)) => scan_expr(storage, expr)?
            .into_iter()
            .chain(schema_list)
            .collect(),
        JoinOperatorPlan::Inner(JoinConstraintPlan::None)
        | JoinOperatorPlan::LeftOuter(JoinConstraintPlan::None) => schema_list,
    };

    Ok(schema_list)
}

fn scan_table_factor<T>(
    storage: &T,
    table_factor: &TableFactorPlan,
) -> Result<HashMap<String, Schema>>
where
    T: Store + ?Sized,
{
    match table_factor {
        TableFactorPlan::Table { name, .. } => {
            let schema = storage.fetch_schema(name)?;
            let schema_list: HashMap<String, Schema> = schema.map_or_else(HashMap::new, |schema| {
                HashMap::from([(name.to_owned(), schema)])
            });

            Ok(schema_list)
        }
        TableFactorPlan::Derived { subquery, .. } => scan_query(storage, subquery),
        TableFactorPlan::Series { .. } | TableFactorPlan::Dictionary { .. } => Ok(HashMap::new()),
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
