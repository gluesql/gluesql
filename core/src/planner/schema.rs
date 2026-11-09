use {
    super::visit::{Visitor, walk_source},
    crate::{
        plan::{
            AlterTableOperationPlan, SchemaMap, SchemaMapPlan, SourcePlan, StatementPlan,
            StatementPlanKind,
        },
        result::{Error, Result},
        store::Store,
    },
    std::collections::BTreeSet,
};

/// Prepares schema dependencies on the statement for subsequent planning passes.
/// An existing `Planned` map is preserved without storage reads or conversion.
pub fn plan_schema_map<T: Store + ?Sized>(
    storage: &T,
    mut statement: StatementPlan,
) -> Result<StatementPlan> {
    if matches!(&statement.schema_map, SchemaMapPlan::Planned(_)) {
        return Ok(statement);
    }

    let mut dependencies = SchemaDependencies::default();
    let mut referencing_targets = BTreeSet::new();

    match &statement.kind {
        StatementPlanKind::Query(query) => dependencies.visit_query(query)?,
        StatementPlanKind::Insert {
            table_name, source, ..
        } => {
            dependencies.names.insert(table_name.clone());
            dependencies.visit_query(source)?;
        }
        StatementPlanKind::Update {
            table_name,
            assignments,
            selection,
        } => {
            dependencies.names.insert(table_name.clone());

            for assignment in assignments {
                dependencies.visit_expr(&assignment.value)?;
            }

            if let Some(selection) = selection {
                dependencies.visit_expr(selection)?;
            }
        }
        StatementPlanKind::Delete {
            table_name,
            selection,
        } => {
            dependencies.names.insert(table_name.clone());
            referencing_targets.insert(table_name.clone());

            if let Some(selection) = selection {
                dependencies.visit_expr(selection)?;
            }
        }
        StatementPlanKind::CreateTable {
            name,
            source,
            foreign_keys,
            ..
        } => {
            dependencies.names.insert(name.clone());

            for foreign_key in foreign_keys {
                if &foreign_key.referenced_table_name != name {
                    dependencies
                        .names
                        .insert(foreign_key.referenced_table_name.clone());
                }
            }

            if let Some(source) = source {
                dependencies.visit_query(source)?;
            }
        }
        StatementPlanKind::AlterTable { name, operation } => {
            dependencies.names.insert(name.clone());

            if matches!(
                operation,
                AlterTableOperationPlan::RenameColumn { .. }
                    | AlterTableOperationPlan::DropColumn { .. }
            ) {
                referencing_targets.insert(name.clone());
            }
        }
        StatementPlanKind::DropTable { names, .. } => {
            dependencies.names.extend(names.iter().cloned());
            referencing_targets.extend(names.iter().cloned());
        }
        StatementPlanKind::CreateIndex { table_name, .. }
        | StatementPlanKind::ShowColumns { table_name } => {
            dependencies.names.insert(table_name.clone());
        }
        StatementPlanKind::CreateFunction { .. }
        | StatementPlanKind::DropFunction { .. }
        | StatementPlanKind::DropIndex { .. }
        | StatementPlanKind::StartTransaction
        | StatementPlanKind::Commit
        | StatementPlanKind::Rollback
        | StatementPlanKind::ShowVariable(_)
        | StatementPlanKind::ShowIndexes(_) => {}
    }

    for function in &statement.functions {
        dependencies.visit_expr(&function.body)?;

        for arg in &function.args {
            if let Some(default) = &arg.default {
                dependencies.visit_expr(default)?;
            }
        }
    }

    let schema_map = if referencing_targets.is_empty() {
        let mut schema_map = SchemaMap::new();

        for name in dependencies.names {
            if let Some(schema) = storage.fetch_schema(&name)? {
                schema_map.insert(name, schema.into());
            }
        }

        schema_map
    } else {
        storage
            .fetch_all_schemas()?
            .into_iter()
            .filter(|schema| {
                dependencies.names.contains(&schema.table_name)
                    || schema.foreign_keys.iter().any(|foreign_key| {
                        referencing_targets.contains(&foreign_key.referenced_table_name)
                    })
            })
            .map(|schema| (schema.table_name.clone(), schema.into()))
            .collect()
    };

    statement.schema_map = SchemaMapPlan::Planned(schema_map);

    Ok(statement)
}

#[derive(Default)]
struct SchemaDependencies {
    names: BTreeSet<String>,
}

impl Visitor for SchemaDependencies {
    type Error = Error;

    fn visit_source(&mut self, source: &SourcePlan) -> Result<()> {
        if let SourcePlan::Table(table) = source {
            self.names.insert(table.name.clone());
        }

        walk_source(self, source)
    }
}

#[cfg(test)]
mod tests {
    use {
        super::plan_schema_map,
        crate::{
            ast::DataType,
            mock::{MockStorage, run},
            parse_sql::{parse, parse_expr},
            plan::{
                CustomFunctionPlan, ExprPlan, OperateFunctionArgPlan, SchemaMap, SchemaMapPlan,
                StatementPlan,
            },
            query_builder::{Build, non_clustered, primary_key, table},
            result::{Error, Result},
            translate::{NO_PARAMS, translate, translate_expr},
        },
    };

    fn plan(storage: &MockStorage, sql: &str) -> Result<Vec<String>> {
        let parsed = parse(sql).expect(sql).into_iter().next().unwrap();
        let statement = translate(&parsed).unwrap().into();

        plan_statement(storage, statement)
    }

    fn plan_statement(storage: &MockStorage, statement: StatementPlan) -> Result<Vec<String>> {
        let statement = plan_schema_map(storage, statement)?;
        let SchemaMapPlan::Planned(schema_map) = statement.schema_map else {
            panic!("schema planning must produce a Planned map");
        };

        Ok(schema_map.into_keys().collect())
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
        test(
            "UPDATE Foo SET id = (SELECT name FROM Bar)",
            &["Bar", "Foo"],
        );
        test(
            "CREATE TABLE New (id INT, FOREIGN KEY (id) REFERENCES Foo(id))",
            &["Foo"],
        );
        test(
            "CREATE TABLE Foo (id INT, FOREIGN KEY (id) REFERENCES Foo(id))",
            &["Foo"],
        );
        test("SHOW COLUMNS FROM Foo", &["Foo"]);
        test("ALTER TABLE Foo ADD COLUMN n INT", &["Foo"]);
        test("ALTER TABLE Foo RENAME TO Renamed", &["Foo"]);
    }

    #[test]
    fn create_index() {
        let storage = run("CREATE TABLE Foo (id INTEGER)");
        let test = |sql: &str, expected: &[&str], name: &str| {
            let actual = plan(&storage, sql).expect(sql);
            assert_eq!(actual.as_slice(), expected, "{name}: {sql}");
        };

        let actual = "CREATE INDEX idx ON Foo (id)";
        let expected = &["Foo"];
        test(actual, expected, "index target schema");

        let actual = "CREATE INDEX idx ON Missing (id)";
        let expected = &[];
        test(actual, expected, "missing schema is left to validation");

        let actual = "CREATE INDEX idx ON Foo ((SELECT id FROM __Err__))";
        let expected = &["Foo"];
        test(
            actual,
            expected,
            "unsupported index subquery is not scanned",
        );

        let sql = "CREATE INDEX idx ON __Err__ (id)";
        let actual = plan(&storage, sql);
        let expected = Err(Error::StorageMsg(
            "[MockStorage] fetch_schema - user triggered error".to_owned(),
        ));
        assert_eq!(actual, expected, "target schema error propagates: {sql}");
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
        test(
            "
            SELECT *
            FROM Foo
            ORDER BY (SELECT id FROM Bar LIMIT 1);
        ",
            &["Bar", "Foo"],
        );
        test(
            "
            SELECT *
            FROM Foo
            ORDER BY id + 1, (SELECT id FROM Bar LIMIT 1);
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
    fn values() {
        let storage = run("
            CREATE TABLE Foo (id INTEGER);
            CREATE TABLE Bar (id INTEGER);
            CREATE TABLE Baz (id INTEGER);
        ");
        let test = |sql, expected| run_test(&storage, sql, expected);

        test("VALUES (1), (2)", &[]);
        test("VALUES ((SELECT id FROM Foo))", &["Foo"]);
        test(
            "
            VALUES
                ((SELECT id FROM Foo), (SELECT id FROM Bar)),
                ((SELECT id FROM Baz), (SELECT id FROM Foo))
            ",
            &["Bar", "Baz", "Foo"],
        );
        test("VALUES ((SELECT id FROM Foo)) ORDER BY column1", &["Foo"]);
        test("VALUES ((SELECT id FROM Foo)) LIMIT 1", &["Foo"]);
        test("VALUES ((SELECT id FROM Foo)) OFFSET 1", &["Foo"]);
        test(
            "
            VALUES ((SELECT id FROM Foo))
            ORDER BY column1
            LIMIT 1
            OFFSET 1
            ",
            &["Foo"],
        );
        test(
            "
            VALUES ((SELECT id FROM Foo))
            ORDER BY (SELECT id FROM Bar)
            LIMIT (SELECT id FROM Baz)
            ",
            &["Bar", "Baz", "Foo"],
        );
    }

    #[test]
    fn series() {
        let storage = run("CREATE TABLE Foo (id INTEGER)");
        let test = |sql, expected| run_test(&storage, sql, expected);

        test("SELECT * FROM SERIES(2)", &[]);
        test("SELECT * FROM SERIES((SELECT id FROM Foo))", &["Foo"]);
    }

    #[test]
    fn prepares_schema_map() {
        let ddl = "CREATE TABLE Item (id INT DEFAULT 7)";
        let storage = run(ddl);
        let input = table("Item").select().build().unwrap();
        let actual = plan_schema_map(&storage, input);
        let expected = table("Item").select().with_schema(ddl).build();
        assert_eq!(actual, expected);

        let input = table("Missing").select().build().unwrap();
        let expected = StatementPlan {
            schema_map: SchemaMapPlan::Planned(SchemaMap::new()),
            ..input.clone()
        };
        let actual = plan_schema_map(&storage, input);
        assert_eq!(actual, Ok(expected));
    }

    #[test]
    fn supplied_schema_map() {
        let storage = run("CREATE TABLE Item (id INT)");
        let input = table("Item")
            .select()
            .with_schema("CREATE TABLE Item")
            .build()
            .unwrap();
        let expected = Ok(input.clone());
        let actual = plan_schema_map(&storage, input);
        assert_eq!(actual, expected);

        let input = StatementPlan {
            schema_map: SchemaMapPlan::Planned(SchemaMap::new()),
            ..table("Item").select().build().unwrap()
        };
        let expected = Ok(input.clone());
        let actual = plan_schema_map(&storage, input);
        assert_eq!(actual, expected);
    }

    #[test]
    fn referencing() {
        let storage = run("
            CREATE TABLE Parent (id INT PRIMARY KEY);
            CREATE TABLE Child (
                id INT, parent_id INT,
                FOREIGN KEY (parent_id) REFERENCES Parent(id)
            );
            CREATE TABLE Other (id INT);
            CREATE TABLE Unrelated (id INT);
        ");
        let test = |sql, expected| run_test(&storage, sql, expected);

        test(
            "DELETE FROM Parent WHERE id IN (SELECT id FROM Other)",
            &["Child", "Other", "Parent"],
        );
        test("DROP TABLE Parent, Parent", &["Child", "Parent"]);
        test(
            "ALTER TABLE Parent RENAME COLUMN id TO new_id",
            &["Child", "Parent"],
        );
        test("ALTER TABLE Parent DROP COLUMN id", &["Child", "Parent"]);
    }

    #[test]
    fn table_access() {
        let storage = run("
            CREATE TABLE Item (id INT);
            CREATE TABLE Lookup (id INT);
        ");

        let input = table("Item")
            .index_by(primary_key().eq("(SELECT id FROM Lookup)"))
            .select()
            .build()
            .unwrap();
        let actual = plan_statement(&storage, input).unwrap();
        let expected = ["Item", "Lookup"];
        assert_eq!(actual, expected);

        let input = table("Item")
            .index_by(non_clustered("by_id".to_owned()).eq("(SELECT id FROM Lookup)"))
            .select()
            .build()
            .unwrap();
        let actual = plan_statement(&storage, input).unwrap();
        let expected = ["Item", "Lookup"];
        assert_eq!(actual, expected);
    }

    #[test]
    fn custom_function() {
        let storage = run("
            CREATE TABLE Item (id INT);
            CREATE TABLE Body (id INT);
            CREATE TABLE Arg (id INT);
        ");
        let expr =
            |sql| ExprPlan::from(translate_expr(&parse_expr(sql).unwrap(), NO_PARAMS).unwrap());
        let mut input = table("Item").select().build().unwrap();
        input.functions = vec![CustomFunctionPlan {
            name: "provided".to_owned(),
            body: expr("(SELECT id FROM Body)"),
            args: vec![OperateFunctionArgPlan {
                name: "arg".to_owned(),
                data_type: DataType::Int,
                default: Some(expr("(SELECT id FROM Arg)")),
            }],
        }];
        let expected = input
            .clone()
            .with_schema("CREATE TABLE Item (id INT)")
            .with_schema("CREATE TABLE Body (id INT)")
            .with_schema("CREATE TABLE Arg (id INT)")
            .build();
        let actual = plan_schema_map(&storage, input);
        assert_eq!(actual, expected);
    }

    #[test]
    fn hash_join() {
        let storage = run("
            CREATE TABLE A (id INT);
            CREATE TABLE B (id INT);
            CREATE TABLE InputKey (id INT);
            CREATE TABLE RightKey (id INT);
            CREATE TABLE Filter (id INT);
        ");
        let input = table("A")
            .select()
            .join("B")
            .hash_executor("(SELECT id FROM RightKey)", "(SELECT id FROM InputKey)")
            .hash_filter("B.id = (SELECT id FROM Filter)")
            .build()
            .unwrap();
        let actual = plan_statement(&storage, input).unwrap();
        let expected = ["A", "B", "Filter", "InputKey", "RightKey"];
        assert_eq!(actual, expected);
    }
}
