use {
    crate::{
        data::Schema,
        executor::InsertError,
        plan::{ColumnDefPlan, StatementPlan, TableColumnsPlan},
        result::Result,
    },
    std::{collections::HashMap, hash::BuildHasher},
};

pub fn plan<S: BuildHasher>(
    schema_map: &HashMap<String, Schema, S>,
    statement: StatementPlan,
) -> Result<StatementPlan> {
    match statement {
        StatementPlan::Insert {
            table_name,
            columns,
            source,
            table_columns: TableColumnsPlan::Unplanned,
        } => {
            let table_columns = table_columns_from_schema(schema_map, &table_name)?;

            Ok(StatementPlan::Insert {
                table_name,
                columns,
                source,
                table_columns,
            })
        }
        statement => Ok(statement),
    }
}

fn table_columns_from_schema<S: BuildHasher>(
    schema_map: &HashMap<String, Schema, S>,
    table_name: &str,
) -> Result<TableColumnsPlan> {
    let Some(schema) = schema_map.get(table_name) else {
        return Err(InsertError::TableNotFound(table_name.to_owned()).into());
    };

    Ok(match &schema.column_defs {
        Some(column_defs) => TableColumnsPlan::Columns(
            column_defs
                .iter()
                .cloned()
                .map(ColumnDefPlan::from)
                .collect(),
        ),
        None => TableColumnsPlan::Schemaless,
    })
}

#[cfg(test)]
mod tests {
    use {
        super::plan,
        crate::{
            executor::InsertError,
            mock::{MockStorage, run},
            parse_sql::parse,
            plan::StatementPlan,
            planner::fetch_schema_map,
            query_builder::{Build, table},
            result::{Error, Result},
            store::Planner,
            translate::translate,
        },
    };

    const ITEM: &str = "
        CREATE TABLE Item (
            id INTEGER NOT NULL DEFAULT 1,
            name TEXT NULL,
            note TEXT DEFAULT 'x'
        );
    ";
    const ITEM_COLUMNS: &[&str] = &[
        "id INTEGER NOT NULL DEFAULT 1",
        "name TEXT NULL",
        "note TEXT DEFAULT 'x'",
    ];

    fn statement(sql: &str) -> StatementPlan {
        let parsed = parse(sql).expect(sql).into_iter().next().unwrap();

        StatementPlan::from(translate(&parsed).unwrap())
    }

    fn plan_table_columns(storage: &MockStorage, sql: &str) -> Result<StatementPlan> {
        let statement = statement(sql);
        let schema_map = fetch_schema_map(storage, &statement).unwrap();

        plan(&schema_map, statement)
    }

    macro_rules! test {
        ($actual: expr, $expected: expr, $name: literal) => {
            assert_eq!($actual, $expected, $name);
        };
    }

    #[test]
    fn insert() {
        let storage = run(ITEM);

        let sql = "INSERT INTO Item (name) VALUES ('glue')";
        let actual = plan_table_columns(&storage, sql).unwrap();
        let expected = table("Item")
            .table_columns(ITEM_COLUMNS)
            .insert()
            .columns("name")
            .values(vec!["'glue'"])
            .build()
            .unwrap();
        test!(
            actual,
            expected,
            "schemaful insert closes catalog columns:\n{sql}"
        );

        let sql = "INSERT INTO Item SELECT id, name, note FROM Item";
        let actual = plan_table_columns(&storage, sql).unwrap();
        let expected = table("Item")
            .table_columns(ITEM_COLUMNS)
            .insert()
            .as_select(table("Item").select().project("id, name, note"))
            .build()
            .unwrap();
        test!(
            actual,
            expected,
            "insert source stays on the statement:\n{sql}"
        );

        let closed = table("Item")
            .schemaless()
            .insert()
            .as_select(table("Item").select().project("id, name, note"))
            .build()
            .unwrap();
        let schema_map = fetch_schema_map(&storage, &closed).unwrap();
        let actual = plan(&schema_map, closed.clone()).unwrap();
        test!(actual, closed, "closed insert is left unchanged:\n{sql}");

        let storage = run("CREATE TABLE Log;");
        let sql = "INSERT INTO Log VALUES ('{}')";
        let actual = plan_table_columns(&storage, sql).unwrap();
        let expected = table("Log")
            .schemaless()
            .insert()
            .values(vec!["'{}'"])
            .build()
            .unwrap();
        test!(actual, expected, "schemaless insert:\n{sql}");

        let storage = MockStorage::default();
        let sql = "INSERT INTO Missing VALUES (1)";
        let actual = plan_table_columns(&storage, sql).unwrap_err();
        let expected = Error::Insert(InsertError::TableNotFound("Missing".to_owned()));
        test!(actual, expected, "missing table:\n{sql}");

        let actual = storage.plan(statement(sql)).unwrap_err();
        test!(
            actual,
            expected,
            "planner chain reports the missing table:\n{sql}"
        );
    }

    #[test]
    fn leaves_statements_without_table_columns() {
        let storage = run(ITEM);

        let sql = "SELECT * FROM Item";
        let actual = plan_table_columns(&storage, sql).unwrap();
        test!(actual, statement(sql), "select is unchanged:\n{sql}");

        let sql = "DELETE FROM Item WHERE id = 1";
        let actual = plan_table_columns(&storage, sql).unwrap();
        test!(actual, statement(sql), "delete is unchanged:\n{sql}");

        let sql = "CREATE TABLE Other (id INTEGER DEFAULT 1)";
        let actual = plan_table_columns(&storage, sql).unwrap();
        test!(actual, statement(sql), "create table is unchanged:\n{sql}");
    }
}
