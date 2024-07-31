use {
    super::expr::PlanExpr,
    crate::{
        ast::{
            Expr, Join, JoinConstraint, JoinOperator, Query, Select, SelectItem, SetExpr,
            Statement, TableFactor, TableWithJoins,
        },
        data::Schema,
        result::Result,
        store::Store,
    },
    async_recursion::async_recursion,
    futures::stream::{self, StreamExt, TryStreamExt},
    std::collections::HashMap,
};

pub async fn fetch_schema_map<T: Store>(
    storage: &T,
    statement: &Statement,
) -> Result<HashMap<String, Schema>> {
    match statement {
        Statement::Query(query) => scan_query(storage, query).await,
        Statement::Insert {
            table_name, source, ..
        } => {
            let table_schema = storage
                .fetch_schema(table_name)
                .await?
                .map(|schema| HashMap::from([(table_name.to_owned(), schema)]))
                .unwrap_or_else(HashMap::new);
            let source_schema_list = scan_query(storage, source).await?;
            let schema_list = table_schema.into_iter().chain(source_schema_list).collect();

            Ok(schema_list)
        }
        Statement::CreateTable { name, source, .. } => {
            let table_schema = storage
                .fetch_schema(name)
                .await?
                .map(|schema| HashMap::from([(name.to_owned(), schema)]))
                .unwrap_or_else(HashMap::new);
            let source_schema_list = match source {
                Some(source) => scan_query(storage, source).await?,
                None => HashMap::new(),
            };
            let schema_list = table_schema.into_iter().chain(source_schema_list).collect();

            Ok(schema_list)
        }
        Statement::DropTable { names, .. } => {
            stream::iter(names)
                .filter_map(|table_name| async {
                    storage
                        .fetch_schema(table_name)
                        .await
                        .map(|schema| Some((table_name.clone(), schema?)))
                        .transpose()
                })
                .try_collect()
                .await
        }
        _ => Ok(HashMap::new()),
    }
}

async fn scan_query<T: Store>(storage: &T, query: &Query) -> Result<HashMap<String, Schema>> {
    let Query {
        body,
        limit,
        offset,
        ..
    } = query;

    let schema_list = match body {
        SetExpr::Select(select) => scan_select(storage, select).await?,
        SetExpr::Values(_) => HashMap::new(),
    };

    let schema_list = match (limit, offset) {
        (Some(limit), Some(offset)) => schema_list
            .into_iter()
            .chain(scan_expr(storage, limit).await?)
            .chain(scan_expr(storage, offset).await?)
            .collect(),
        (Some(expr), None) | (None, Some(expr)) => schema_list
            .into_iter()
            .chain(scan_expr(storage, expr).await?)
            .collect(),
        (None, None) => schema_list,
    };

    Ok(schema_list)
}

async fn scan_select<T: Store>(storage: &T, select: &Select) -> Result<HashMap<String, Schema>> {
    let Select {
        projection,
        from,
        selection,
        group_by,
        having,
    } = select;

    let projection = stream::iter(projection)
        .then(|select_item| async move {
            match select_item {
                SelectItem::Expr { expr, .. } => scan_expr(storage, expr).await,
                SelectItem::QualifiedWildcard(_) | SelectItem::Wildcard => Ok(HashMap::new()),
            }
        })
        .try_collect::<Vec<HashMap<String, Schema>>>()
        .await?
        .into_iter()
        .flatten();

    let from = scan_table_with_joins(storage, from).await?;

    let exprs = selection.iter().chain(group_by.iter()).chain(having.iter());

    Ok(stream::iter(exprs)
        .then(|expr| scan_expr(storage, expr))
        .try_collect::<Vec<HashMap<String, Schema>>>()
        .await?
        .into_iter()
        .flatten()
        .chain(projection)
        .chain(from)
        .collect())
}

async fn scan_table_with_joins<T: Store>(
    storage: &T,
    table_with_joins: &TableWithJoins,
) -> Result<HashMap<String, Schema>> {
    let TableWithJoins { relation, joins } = table_with_joins;
    let schema_list = scan_table_factor(storage, relation).await?;

    Ok(stream::iter(joins)
        .then(|join| scan_join(storage, join))
        .try_collect::<Vec<HashMap<String, Schema>>>()
        .await?
        .into_iter()
        .flatten()
        .chain(schema_list)
        .collect())
}

async fn scan_join<T: Store>(storage: &T, join: &Join) -> Result<HashMap<String, Schema>> {
    let Join {
        relation,
        join_operator,
        ..
    } = join;

    let schema_list = scan_table_factor(storage, relation).await?;
    let schema_list = match join_operator {
        JoinOperator::Inner(JoinConstraint::On(expr))
        | JoinOperator::LeftOuter(JoinConstraint::On(expr)) => scan_expr(storage, expr)
            .await?
            .into_iter()
            .chain(schema_list)
            .collect(),
        JoinOperator::Inner(JoinConstraint::None)
        | JoinOperator::LeftOuter(JoinConstraint::None) => schema_list,
    };

    Ok(schema_list)
}

#[async_recursion(?Send)]
async fn scan_table_factor<T>(
    storage: &T,
    table_factor: &TableFactor,
) -> Result<HashMap<String, Schema>>
where
    T: Store,
{
    match table_factor {
        TableFactor::Table { name, .. } => {
            let schema = storage.fetch_schema(name).await?;
            let schema_list: HashMap<String, Schema> = schema.map_or_else(HashMap::new, |schema| {
                HashMap::from([(name.to_owned(), schema)])
            });

            Ok(schema_list)
        }
        TableFactor::Derived { subquery, .. } => scan_query(storage, subquery).await,
        TableFactor::Series { .. } | TableFactor::Dictionary { .. } => Ok(HashMap::new()),
    }
}

#[async_recursion(?Send)]
async fn scan_expr<T>(storage: &T, expr: &Expr) -> Result<HashMap<String, Schema>>
where
    T: Store,
{
    let schema_list = match expr.into() {
        PlanExpr::None | PlanExpr::Identifier(_) | PlanExpr::CompoundIdentifier { .. } => {
            HashMap::new()
        }
        PlanExpr::Expr(expr) => scan_expr(storage, expr).await?,
        PlanExpr::TwoExprs(expr, expr2) => scan_expr(storage, expr)
            .await?
            .into_iter()
            .chain(scan_expr(storage, expr2).await?)
            .collect(),
        PlanExpr::ThreeExprs(expr, expr2, expr3) => scan_expr(storage, expr)
            .await?
            .into_iter()
            .chain(scan_expr(storage, expr2).await?)
            .chain(scan_expr(storage, expr3).await?)
            .collect(),
        PlanExpr::MultiExprs(exprs) => stream::iter(exprs)
            .then(|expr| scan_expr(storage, expr))
            .try_collect::<Vec<HashMap<String, Schema>>>()
            .await?
            .into_iter()
            .flatten()
            .collect(),
        PlanExpr::Query(query) => scan_query(storage, query).await?,
        PlanExpr::QueryAndExpr { query, expr } => scan_query(storage, query)
            .await?
            .into_iter()
            .chain(scan_expr(storage, expr).await?)
            .collect(),
    };

    Ok(schema_list)
}

#[cfg(test)]
mod tests {
    use {
        super::fetch_schema_map,
        crate::{
            mock::{run, MockStorage},
            parse_sql::parse,
            result::Result,
            translate::translate,
        },
        futures::executor::block_on,
        utils::Vector,
    };

    fn plan(storage: &MockStorage, sql: &str) -> Result<Vec<String>> {
        let parsed = parse(sql).expect(sql).into_iter().next().unwrap();
        let statement = translate(&parsed).unwrap();
        let schema_map = block_on(fetch_schema_map(storage, &statement));

        Ok(schema_map?
            .into_keys()
            .collect::<Vector<String>>()
            .sort()
            .into())
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

        // Unimplemented
        test("DELETE FROM Foo;", &[]);
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
