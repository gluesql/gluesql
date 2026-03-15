use {
    self::{query::transform_query, validate::validate_statement},
    crate::{
        ast::{Expr, Literal, Statement},
        data::{SCHEMALESS_DOC_COLUMN, Schema},
        plan::expr::visit_mut_expr,
        result::Result,
    },
    std::{collections::HashMap, hash::BuildHasher},
};

mod query;
mod validate;

pub fn plan<S: BuildHasher>(
    schema_map: &HashMap<String, Schema, S>,
    statement: Statement,
) -> Result<Statement> {
    if !schema_map
        .values()
        .any(|schema| schema.column_defs.is_none())
    {
        return Ok(statement);
    }

    validate_statement(schema_map, &statement)?;
    Ok(transform_statement(schema_map, statement))
}

fn transform_statement<S: BuildHasher>(
    schema_map: &HashMap<String, Schema, S>,
    statement: Statement,
) -> Statement {
    match statement {
        Statement::Query(mut query) => {
            transform_query(schema_map, &mut query);
            Statement::Query(query)
        }
        Statement::Insert {
            table_name,
            columns,
            mut source,
        } => {
            transform_query(schema_map, &mut source);
            let columns = if is_schemaless_table(schema_map, &table_name) {
                vec![SCHEMALESS_DOC_COLUMN.to_owned()]
            } else {
                columns
            };

            Statement::Insert {
                table_name,
                columns,
                source,
            }
        }
        Statement::Update {
            table_name,
            assignments,
            selection,
        } => {
            let table_is_schemaless = is_schemaless_table(schema_map, &table_name);

            let mut assignments = assignments;
            for assignment in &mut assignments {
                transform_single_table_expr(
                    schema_map,
                    &mut assignment.value,
                    &table_name,
                    table_is_schemaless,
                );
            }

            let mut selection = selection;
            if let Some(selection) = selection.as_mut() {
                transform_single_table_expr(
                    schema_map,
                    selection,
                    &table_name,
                    table_is_schemaless,
                );
            }

            Statement::Update {
                table_name,
                assignments,
                selection,
            }
        }
        Statement::Delete {
            table_name,
            selection,
        } => {
            let table_is_schemaless = is_schemaless_table(schema_map, &table_name);

            let mut selection = selection;
            if let Some(selection) = selection.as_mut() {
                transform_single_table_expr(
                    schema_map,
                    selection,
                    &table_name,
                    table_is_schemaless,
                );
            }

            Statement::Delete {
                table_name,
                selection,
            }
        }
        _ => statement,
    }
}

fn transform_single_table_expr(
    schema_map: &HashMap<String, Schema, impl BuildHasher>,
    expr: &mut Expr,
    table_name: &str,
    table_is_schemaless: bool,
) {
    visit_mut_expr(expr, &mut |e| match e {
        Expr::Identifier(ident) => {
            if table_is_schemaless {
                *e = Expr::ArrayIndex {
                    obj: Box::new(Expr::Identifier(SCHEMALESS_DOC_COLUMN.to_owned())),
                    indexes: vec![Expr::Literal(Literal::QuotedString(ident.to_owned()))],
                };
            }
        }
        Expr::CompoundIdentifier { alias, ident } => {
            if table_is_schemaless && alias == table_name {
                *e = Expr::ArrayIndex {
                    obj: Box::new(Expr::CompoundIdentifier {
                        alias: alias.to_owned(),
                        ident: SCHEMALESS_DOC_COLUMN.to_owned(),
                    }),
                    indexes: vec![Expr::Literal(Literal::QuotedString(ident.to_owned()))],
                };
            }
        }
        Expr::Subquery(subquery)
        | Expr::Exists { subquery, .. }
        | Expr::InSubquery { subquery, .. } => {
            transform_query(schema_map, subquery.as_mut());
        }
        _ => {}
    });
}

fn is_schemaless_table(
    schema_map: &HashMap<String, Schema, impl BuildHasher>,
    table_name: &str,
) -> bool {
    schema_map
        .get(table_name)
        .is_some_and(|schema| schema.column_defs.is_none())
}

#[cfg(test)]
mod tests {
    use {
        super::plan as plan_schemaless,
        crate::{
            ast::{Projection, SetExpr, Statement},
            mock::{MockStorage, run},
            parse_sql::parse,
            plan::fetch_schema_map,
            translate::translate,
        },
        futures::executor::block_on,
    };

    fn setup_schemaless_storage() -> MockStorage {
        run("
            CREATE TABLE Player;
            CREATE TABLE Team;
            CREATE TABLE Item (id INTEGER);
        ")
    }

    #[test]
    fn plan_schemaless_transforms() {
        let storage = setup_schemaless_storage();
        let test = |actual: &str, expected: &str, name: &str| {
            let parsed = parse(actual).expect(actual).into_iter().next().unwrap();
            let statement = translate(&parsed).unwrap();
            let schema_map = block_on(fetch_schema_map(&storage, &statement)).unwrap();
            let result = plan_schemaless(&schema_map, statement).unwrap();

            let expected_parsed = parse(expected).expect(expected).into_iter().next().unwrap();
            let mut expected_stmt = translate(&expected_parsed).unwrap();
            if let (Statement::Query(actual_query), Statement::Query(expected_query)) =
                (&result, &mut expected_stmt)
                && let (SetExpr::Select(actual_select), SetExpr::Select(expected_select)) =
                    (&actual_query.body, &mut expected_query.body)
            {
                expected_select.projection = actual_select.projection.clone();
            }

            assert_eq!(
                result, expected_stmt,
                "\n[{name}]\nactual: {actual}\nexpected: {expected}"
            );
        };

        let actual = "SELECT id FROM Player";
        let expected = "SELECT _doc['id'] as id FROM Player";
        test(actual, expected, "single column");

        let actual = "SELECT id FROM Item";
        let expected = "SELECT id FROM Item";
        test(actual, expected, "schemaful root identifier");

        let actual = "SELECT id, name FROM Player";
        let expected = "SELECT _doc['id'] as id, _doc['name'] as name FROM Player";
        test(actual, expected, "multiple columns");

        let actual = "SELECT * FROM Player";
        let expected = "SELECT _doc as _doc FROM Player";
        test(actual, expected, "wildcard");

        let actual = "SELECT Player.* FROM Player";
        let expected = "SELECT Player._doc as _doc FROM Player";
        test(actual, expected, "qualified wildcard");

        let actual = "SELECT * FROM Player AS P";
        let expected = "SELECT _doc as _doc FROM Player AS P";
        test(actual, expected, "wildcard with root alias");

        let actual = "SELECT P.* FROM Player AS P";
        let expected = "SELECT P._doc as _doc FROM Player AS P";
        test(actual, expected, "qualified wildcard with root alias");

        let actual = r"
            SELECT Player.*, Team.*
            FROM Player
            JOIN Team
            WHERE Player.id = Team.id
        ";
        let expected = r"
            SELECT Player._doc as _doc, Team._doc as _doc
            FROM Player
            JOIN Team
            WHERE Player._doc['id'] = Team._doc['id']
        ";
        test(actual, expected, "qualified wildcard join");

        let actual = "SELECT P.*, T.* FROM Player AS P JOIN Team AS T WHERE P.id = T.id";
        let expected = "SELECT P._doc as _doc, T._doc as _doc FROM Player AS P JOIN Team AS T WHERE P._doc['id'] = T._doc['id']";
        test(actual, expected, "qualified wildcard join with aliases");

        let actual = "SELECT Item.* FROM Player JOIN Item WHERE Player.id = Item.id";
        let expected = "SELECT Item.* FROM Player JOIN Item WHERE Player._doc['id'] = Item.id";
        test(actual, expected, "schemaful qualified wildcard join no-op");

        let actual = "SELECT Player.id FROM Player";
        let expected = "SELECT Player._doc['id'] as id FROM Player";
        test(actual, expected, "compound identifier");

        let actual = "SELECT *, id FROM Player";
        let expected = "SELECT _doc as _doc, _doc['id'] as id FROM Player";
        test(actual, expected, "wildcard with extra projection");

        let actual = "SELECT * FROM Player WHERE id = 1";
        let expected = "SELECT _doc as _doc FROM Player WHERE _doc['id'] = 1";
        test(actual, expected, "where clause");

        let actual = "SELECT * FROM Player WHERE id = 1 AND name = 'foo'";
        let expected =
            "SELECT _doc as _doc FROM Player WHERE _doc['id'] = 1 AND _doc['name'] = 'foo'";
        test(actual, expected, "where clause complex");

        let actual = "SELECT * FROM Player WHERE name IS NULL";
        let expected = "SELECT _doc as _doc FROM Player WHERE _doc['name'] IS NULL";
        test(actual, expected, "is null");

        let actual = "SELECT * FROM Player WHERE name IS NOT NULL";
        let expected = "SELECT _doc as _doc FROM Player WHERE _doc['name'] IS NOT NULL";
        test(actual, expected, "is not null");

        let actual = "SELECT * FROM Player WHERE id IN (1, 2, 3)";
        let expected = "SELECT _doc as _doc FROM Player WHERE _doc['id'] IN (1, 2, 3)";
        test(actual, expected, "in list");

        let actual = "SELECT * FROM Player WHERE id BETWEEN 1 AND 10";
        let expected = "SELECT _doc as _doc FROM Player WHERE _doc['id'] BETWEEN 1 AND 10";
        test(actual, expected, "between");

        let actual = "SELECT * FROM Player WHERE name LIKE '%foo%'";
        let expected = "SELECT _doc as _doc FROM Player WHERE _doc['name'] LIKE '%foo%'";
        test(actual, expected, "like");

        let actual = "SELECT * FROM Player WHERE name ILIKE '%foo%'";
        let expected = "SELECT _doc as _doc FROM Player WHERE _doc['name'] ILIKE '%foo%'";
        test(actual, expected, "ilike");

        let actual = "SELECT * FROM Player WHERE (id = 1)";
        let expected = "SELECT _doc as _doc FROM Player WHERE (_doc['id'] = 1)";
        test(actual, expected, "nested");

        let actual = "SELECT -id as neg_id FROM Player";
        let expected = "SELECT -_doc['id'] as neg_id FROM Player";
        test(actual, expected, "unary op");

        let actual = "SELECT CASE WHEN id = 1 THEN 'one' ELSE 'other' END as label FROM Player";
        let expected =
            "SELECT CASE WHEN _doc['id'] = 1 THEN 'one' ELSE 'other' END as label FROM Player";
        test(actual, expected, "case expr");

        let actual = "SELECT CAST(id AS TEXT) as id_text FROM Player";
        let expected = "SELECT CAST(_doc['id'] AS TEXT) as id_text FROM Player";
        test(actual, expected, "cast function");

        let actual = "SELECT SUM(score) as total FROM Player";
        let expected = "SELECT SUM(_doc['score']) as total FROM Player";
        test(actual, expected, "aggregate sum");

        let actual = "SELECT COUNT(id) as cnt FROM Player";
        let expected = "SELECT COUNT(_doc['id']) as cnt FROM Player";
        test(actual, expected, "aggregate count");

        let actual = "SELECT COUNT(*) as cnt FROM Player";
        let expected = "SELECT COUNT(*) as cnt FROM Player";
        test(actual, expected, "aggregate count wildcard");

        let actual = "SELECT team, COUNT(*) as cnt FROM Player GROUP BY team";
        let expected =
            "SELECT _doc['team'] as team, COUNT(*) as cnt FROM Player GROUP BY _doc['team']";
        test(actual, expected, "group by");

        let actual = r"
            SELECT team, COUNT(*) as cnt
            FROM Player
            GROUP BY team
            HAVING COUNT(*) > 1
        ";
        let expected = r"
            SELECT _doc['team'] as team, COUNT(*) as cnt
            FROM Player
            GROUP BY _doc['team']
            HAVING COUNT(*) > 1
        ";
        test(actual, expected, "having");

        let actual = "SELECT id, name FROM Player ORDER BY score";
        let expected =
            "SELECT _doc['id'] as id, _doc['name'] as name FROM Player ORDER BY _doc['score']";
        test(actual, expected, "order by");

        let actual = "SELECT id FROM Player ORDER BY score DESC";
        let expected = "SELECT _doc['id'] as id FROM Player ORDER BY _doc['score'] DESC";
        test(actual, expected, "order by desc");

        let actual = "UPDATE Player SET score = 100 WHERE id = 1";
        let expected = "UPDATE Player SET score = 100 WHERE _doc['id'] = 1";
        test(actual, expected, "update where");

        let actual = "UPDATE Player SET score = score + 1 WHERE id = 1";
        let expected = "UPDATE Player SET score = _doc['score'] + 1 WHERE _doc['id'] = 1";
        test(actual, expected, "update expr");

        let actual = "DELETE FROM Player WHERE id = 1";
        let expected = "DELETE FROM Player WHERE _doc['id'] = 1";
        test(actual, expected, "delete where");

        let actual = "SELECT id FROM Player LIMIT num";
        let expected = "SELECT _doc['id'] as id FROM Player LIMIT _doc['num']";
        test(actual, expected, "limit expr");

        let actual = "SELECT id FROM Player OFFSET skip";
        let expected = "SELECT _doc['id'] as id FROM Player OFFSET _doc['skip']";
        test(actual, expected, "offset expr");

        let actual = "SELECT id FROM Player LIMIT num OFFSET skip";
        let expected = "SELECT _doc['id'] as id FROM Player LIMIT _doc['num'] OFFSET _doc['skip']";
        test(actual, expected, "limit offset expr");

        let actual = "SELECT * FROM (SELECT id FROM Player) AS sub";
        let expected = "SELECT * FROM (SELECT _doc['id'] as id FROM Player) AS sub";
        test(actual, expected, "derived subquery");

        let actual = r"
            SELECT
                (SELECT id FROM Player LIMIT 1) as first_id
            FROM Item
            WHERE
                EXISTS (SELECT id FROM Player WHERE id = 1)
                AND id IN (SELECT id FROM Player)
        ";
        let expected = r"
            SELECT
                (SELECT _doc['id'] as id FROM Player LIMIT 1) as first_id
            FROM Item
            WHERE
                EXISTS (SELECT _doc['id'] as id FROM Player WHERE _doc['id'] = 1)
                AND id IN (SELECT _doc['id'] as id FROM Player)
        ";
        test(actual, expected, "expr subquery recursion");

        let actual = "INSERT INTO Player VALUES ('{}')";
        let expected = "INSERT INTO Player (_doc) VALUES ('{}')";
        test(actual, expected, "insert schemaless");

        let actual = "INSERT INTO Player VALUES ('{}'), ('{}')";
        let expected = "INSERT INTO Player (_doc) VALUES ('{}'), ('{}')";
        test(actual, expected, "insert schemaless multiple");

        let actual = "INSERT INTO Item (id) SELECT id FROM Player";
        let expected = "INSERT INTO Item (id) SELECT _doc['id'] as id FROM Player";
        test(actual, expected, "insert schemaful columns unchanged");

        let actual = "UPDATE Player SET score = 100";
        let expected = "UPDATE Player SET score = 100";
        test(actual, expected, "update without selection");

        let actual = "UPDATE Player SET score = Player.score + 1 WHERE Player.id = 1";
        let expected =
            "UPDATE Player SET score = Player._doc['score'] + 1 WHERE Player._doc['id'] = 1";
        test(actual, expected, "update compound identifier");

        let actual = r"
            UPDATE Player
            SET score = (SELECT MAX(score) as max_score FROM Player)
            WHERE id = 1
        ";
        let expected = r"
            UPDATE Player
            SET score = (SELECT MAX(_doc['score']) as max_score FROM Player)
            WHERE _doc['id'] = 1
        ";
        test(actual, expected, "update subquery recursion");

        let actual = "UPDATE Item SET id = id + 1 WHERE id = 1";
        let expected = "UPDATE Item SET id = id + 1 WHERE id = 1";
        test(actual, expected, "update schemaful no-op");

        let actual = "DELETE FROM Player";
        let expected = "DELETE FROM Player";
        test(actual, expected, "delete without selection");

        let actual = "DELETE FROM Player WHERE Player.id = 1";
        let expected = "DELETE FROM Player WHERE Player._doc['id'] = 1";
        test(actual, expected, "delete compound identifier");

        let actual = r"
            DELETE FROM Player
            WHERE
                EXISTS (SELECT id FROM Player WHERE id = 1)
                AND id IN (SELECT id FROM Player)
        ";
        let expected = r"
            DELETE FROM Player
            WHERE
                EXISTS (SELECT _doc['id'] as id FROM Player WHERE _doc['id'] = 1)
                AND _doc['id'] IN (SELECT _doc['id'] as id FROM Player)
        ";
        test(actual, expected, "delete exists and in subquery recursion");

        let actual = "DELETE FROM Item WHERE Item.id = 1";
        let expected = "DELETE FROM Item WHERE Item.id = 1";
        test(actual, expected, "delete schemaful compound no-op");
    }

    #[test]
    fn plan_schemaless_projection_mode() {
        let storage = setup_schemaless_storage();
        let test = |sql: &str, expected_schemaless_map: bool| {
            let parsed = parse(sql).expect(sql).into_iter().next().unwrap();
            let statement = translate(&parsed).unwrap();
            let schema_map = block_on(fetch_schema_map(&storage, &statement)).unwrap();
            let planned = plan_schemaless(&schema_map, statement).unwrap();

            let crate::ast::Statement::Query(query) = planned else {
                panic!("expected query statement");
            };
            let SetExpr::Select(select) = query.body else {
                panic!("expected select query");
            };
            assert_eq!(
                matches!(select.projection, Projection::SchemalessMap),
                expected_schemaless_map,
                "{sql}"
            );
        };

        test("SELECT * FROM Player", true);
        test("SELECT * FROM Player AS P", true);
        test("SELECT P.* FROM Player AS P", true);
        test("SELECT id FROM Player", false);
        test(
            r"
                SELECT Player.*, Team.*
                FROM Player
                JOIN Team
                WHERE Player.id = Team.id
            ",
            false,
        );
        test(
            "SELECT P.*, T.* FROM Player AS P JOIN Team AS T WHERE P.id = T.id",
            false,
        );
    }

    #[test]
    fn schemaful_table_unchanged() {
        let storage = run("CREATE TABLE Item (id INTEGER, name TEXT);");
        let test = |actual: &str, expected: &str, name: &str| {
            let parsed = parse(actual).expect(actual).into_iter().next().unwrap();
            let statement = translate(&parsed).unwrap();
            let schema_map = block_on(fetch_schema_map(&storage, &statement)).unwrap();
            let result = plan_schemaless(&schema_map, statement).unwrap();

            let expected_parsed = parse(expected).expect(expected).into_iter().next().unwrap();
            let expected_stmt = translate(&expected_parsed).unwrap();

            assert_eq!(
                result, expected_stmt,
                "\n[{name}]\nactual: {actual}\nexpected: {expected}"
            );
        };

        let actual = "SELECT id FROM Item WHERE id = 1";
        let expected = "SELECT id FROM Item WHERE id = 1";
        test(actual, expected, "schemaful unchanged");
    }
}
