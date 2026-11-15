//! Rewrites single-table filters into native sources.
//! Conditions that cannot be delegated stay in a common Filter above the source.

mod condition;

use {
    crate::schema::{primary_key, quote, rowid_name},
    gluesql_core::{
        ast::DataType,
        plan::{
            ExprPlan, FilterInputPlan, FilterPlan, NativeColumnPlan, NativeSourcePlan,
            ProjectInputPlan, QueryPlan, SchemaMap, SourcePlan, TableAccessPlan,
        },
        planner::{VisitorMut, walk_query_mut},
    },
    std::{collections::BTreeSet, convert::Infallible},
};

pub(super) fn plan(query: &mut QueryPlan, schemas: &SchemaMap) {
    let Ok(()) = NativeFilterPlanner { schemas }.visit_query(query);
}

struct NativeFilterPlanner<'a> {
    schemas: &'a SchemaMap,
}

enum NativeFilterRewrite {
    Unchanged,
    Full {
        source: NativeSourcePlan,
    },
    Partial {
        source: NativeSourcePlan,
        remaining: ExprPlan,
    },
}

impl VisitorMut for NativeFilterPlanner<'_> {
    type Error = Infallible;

    fn visit_query(&mut self, query: &mut QueryPlan) -> Result<(), Self::Error> {
        // Visit children first so an unsupported parent does not skip eligible subqueries.
        walk_query_mut(self, query)?;

        if let Some(project) = query.project_mut()
            && let ProjectInputPlan::Filter(filter) = &project.input
        {
            match plan_filter(filter, self.schemas) {
                NativeFilterRewrite::Unchanged => {}
                NativeFilterRewrite::Full { source } => {
                    project.input = ProjectInputPlan::Source(SourcePlan::Native(source));
                }
                NativeFilterRewrite::Partial { source, remaining } => {
                    project.input = ProjectInputPlan::Filter(FilterPlan {
                        input: FilterInputPlan::Source(SourcePlan::Native(source)),
                        expr: remaining,
                    });
                }
            }
        }
        Ok(())
    }
}

fn plan_filter(filter: &FilterPlan, schemas: &SchemaMap) -> NativeFilterRewrite {
    let FilterInputPlan::Source(SourcePlan::Table(table)) = &filter.input else {
        return NativeFilterRewrite::Unchanged;
    };

    // PK lookups already use SQLite through fetch_data; keep the chosen access path.
    let selected_columns = match &table.access {
        TableAccessPlan::FullScan => None,
        TableAccessPlan::ProjectedScan { columns } => Some(columns),
        TableAccessPlan::PrimaryKey { .. } | TableAccessPlan::Index { .. } => {
            return NativeFilterRewrite::Unchanged;
        }
    };

    // Native row decoding requires fixed output types.
    let Some(schema) = schemas.get(&table.name) else {
        return NativeFilterRewrite::Unchanged;
    };

    let Some(columns) = schema.column_defs.as_ref() else {
        return NativeFilterRewrite::Unchanged;
    };

    let alias = table
        .alias
        .as_ref()
        .map_or(table.name.as_str(), |alias| alias.name.as_str());
    let column_aliases = table
        .alias
        .as_ref()
        .map_or(&[][..], |alias| alias.columns.as_slice());

    if column_aliases.len() > columns.len() {
        return NativeFilterRewrite::Unchanged;
    }

    let logical_names = columns
        .iter()
        .enumerate()
        .map(|(index, column)| column_aliases.get(index).unwrap_or(&column.name))
        .collect::<Vec<_>>();

    // Duplicate logical names make physical column lookup ambiguous.
    if logical_names.iter().collect::<BTreeSet<_>>().len() != logical_names.len() {
        return NativeFilterRewrite::Unchanged;
    }

    let storage_schema = schema.to_schema(&table.name);
    let Some(storage_columns) = storage_schema.column_defs.as_deref() else {
        return NativeFilterRewrite::Unchanged;
    };

    // Preserve scan_data's GlueSQL Key or rowid order.
    let order_by = if let Some(index) = primary_key(storage_columns) {
        let column = &columns[index];
        // TEXT encodings such as DECIMAL do not preserve numeric Key order.
        if !matches!(
            column.data_type,
            DataType::Int8
                | DataType::Int16
                | DataType::Int32
                | DataType::Int
                | DataType::Uint8
                | DataType::Uint16
                | DataType::Uint32
                | DataType::Boolean
                | DataType::Text
                | DataType::Bytea
        ) {
            return NativeFilterRewrite::Unchanged;
        }

        quote(&column.name)
    } else {
        // A shadowed rowid alias would sort by a user column instead.
        let Ok(rowid) = rowid_name(&storage_schema) else {
            return NativeFilterRewrite::Unchanged;
        };

        quote(rowid)
    };

    let Some(condition) = condition::plan(&filter.expr, alias, columns, column_aliases) else {
        return NativeFilterRewrite::Unchanged;
    };

    // SQL uses physical column names; output metadata keeps GlueSQL's logical names.
    // Both lists must preserve the common scan's column order.
    let mut projection = Vec::new();
    let mut output_columns = Vec::new();
    for (column, name) in columns.iter().zip(logical_names) {
        if selected_columns.is_some_and(|selected| !selected.contains(&column.name)) {
            continue;
        }

        projection.push(quote(&column.name));
        output_columns.push(NativeColumnPlan {
            name: name.clone(),
            data_type: column.data_type.clone(),
        });
    }
    // Never silently drop unknown or repeated requested column names.
    if selected_columns.is_some_and(|selected| selected.len() != output_columns.len()) {
        return NativeFilterRewrite::Unchanged;
    }

    // SQLite still needs a SELECT item when GlueSQL needs no values from each row.
    // query_native accepts this dummy column and returns an empty value list per row.
    if projection.is_empty() {
        projection.push("1".to_owned());
    }

    let source = NativeSourcePlan {
        query: format!(
            "SELECT {} FROM {} WHERE {} ORDER BY {} ASC",
            projection.join(", "),
            quote(&table.name),
            condition.sql,
            order_by
        ),
        params: condition.params,
        alias: alias.to_owned(),
        columns: output_columns,
    };

    match condition.remaining {
        Some(remaining) => NativeFilterRewrite::Partial { source, remaining },
        None => NativeFilterRewrite::Full { source },
    }
}

#[cfg(test)]
mod tests {
    use {
        super::plan as plan_native_filter,
        gluesql_core::{
            data::Schema,
            parse_sql::parse,
            plan::{
                FilterInputPlan, ProjectInputPlan, QueryPlan, SchemaMap, SchemaMapPlan, SourcePlan,
                StatementPlan, StatementPlanKind, TableAccessPlan,
            },
            planner::plan_scan_columns,
            query_builder::{
                Build, col, native_query, non_clustered, primary_key, subquery, table, value,
                values,
            },
            translate::translate,
        },
    };

    fn input(sql: &str, ddl: &str) -> (QueryPlan, SchemaMap) {
        let schema = Schema::from_ddl(ddl).unwrap();
        let schemas: SchemaMap = [(schema.table_name.clone(), schema.into())].into();
        let parsed = parse(sql).expect(sql).into_iter().next().expect(sql);
        let mut statement: StatementPlan = translate(&parsed).expect(sql).into();
        statement.schema_map = SchemaMapPlan::Planned(schemas.clone());
        let statement = plan_scan_columns(statement).unwrap();
        let StatementPlanKind::Query(query) = statement.kind else {
            panic!("{sql}");
        };
        (query, schemas)
    }

    fn plan(sql: &str, ddl: &str) -> QueryPlan {
        let (mut query, schemas) = input(sql, ddl);
        plan_native_filter(&mut query, &schemas);

        query
    }

    fn assert_query(actual: QueryPlan, expected: impl Build, ddl: &str) {
        let expected = expected.with_schema(ddl).build().unwrap();
        let expected = plan_scan_columns(expected).unwrap();

        assert_eq!(StatementPlanKind::Query(actual), expected.kind);
    }

    #[test]
    fn visit_query() {
        let ddl = "CREATE TABLE Item (id INT, qty INT)";

        // walk_query_mut visits subqueries before checking the parent input.
        let sql = "
            SELECT id
            FROM Item
            WHERE id IN (
                SELECT i.id
                FROM Item i
                WHERE i.qty > 10
            )
        ";
        let actual = plan(sql, ddl);
        let inner = native_query(concat!(
            r#"SELECT "id", "qty" "#,
            r#"FROM "Item" "#,
            r#"WHERE ("qty" > ?1) "#,
            r#"ORDER BY "_rowid_" ASC"#,
        ))
        .params([10_i64])
        .columns([("id", "INT"), ("qty", "INT")])
        .alias_as("i")
        .select()
        .project("i.id");
        let expected = table("Item")
            .select()
            .filter(col("id").in_list(inner.clone()))
            .project("id");
        assert_query(actual, expected, ddl);

        // Values expressions are walked too; Values is not itself a native source.
        let sql = "
            VALUES ((
                SELECT i.id
                FROM Item i
                WHERE i.qty > 10
            ))
        ";
        let actual = plan(sql, ddl);
        let expected = values(vec![vec![subquery(inner)]]);
        assert_query(actual, expected, ddl);

        // Correlated projection and residual conditions stay in the common plan.
        let sql = "
            SELECT (
                SELECT outer_row.id
                FROM Item i
                WHERE i.qty > 10
            ) AS outer_id
            FROM Item outer_row
        ";
        let actual = plan(sql, ddl);
        let inner = native_query(concat!(
            r#"SELECT "qty" "#,
            r#"FROM "Item" "#,
            r#"WHERE ("qty" > ?1) "#,
            r#"ORDER BY "_rowid_" ASC"#,
        ))
        .params([10_i64])
        .columns([("qty", "INT")])
        .alias_as("i")
        .select()
        .project("outer_row.id");
        let expected = table("Item")
            .alias_as("outer_row")
            .select()
            .project(subquery(inner).alias_as("outer_id"));
        assert_query(actual, expected, ddl);

        let sql = "
            SELECT (
                SELECT i.id
                FROM Item i
                WHERE i.qty > 10
                  AND i.id = outer_row.id
            ) AS matched_id
            FROM Item outer_row
        ";
        let actual = plan(sql, ddl);
        let inner = native_query(concat!(
            r#"SELECT "id", "qty" "#,
            r#"FROM "Item" "#,
            r#"WHERE ("qty" > ?1) "#,
            r#"ORDER BY "_rowid_" ASC"#,
        ))
        .params([10_i64])
        .columns([("id", "INT"), ("qty", "INT")])
        .alias_as("i")
        .select()
        .filter("i.id = outer_row.id")
        .project("i.id");
        let expected = table("Item")
            .alias_as("outer_row")
            .select()
            .project(subquery(inner).alias_as("matched_id"));
        assert_query(actual, expected, ddl);

        // Only a Project with a direct Filter input can be rewritten.
        let sql = "SELECT id FROM Item";
        let actual = plan(sql, ddl);
        let expected = table("Item").select().project("id");
        assert_query(actual, expected, ddl);

        let sql = "SELECT id FROM Item WHERE qty > 1 GROUP BY id";
        let actual = plan(sql, ddl);
        let expected = table("Item")
            .select()
            .filter("qty > 1")
            .group_by("id")
            .project("id");
        assert_query(actual, expected, ddl);
    }

    #[test]
    fn plan_filter() {
        let ddl = "CREATE TABLE Item (id INT, qty INT)";

        // Filter input must be a single table source.
        let sql = "
            SELECT i.id
            FROM Item i
            JOIN Item j ON i.id = j.id
            WHERE i.qty > 1
        ";
        let actual = plan(sql, ddl);
        let expected = table("Item")
            .alias_as("i")
            .select()
            .join_as("Item", "j")
            .on("i.id = j.id")
            .filter("i.qty > 1")
            .project("i.id");
        assert_query(actual, expected, ddl);

        // FullScan keeps every column; projected columns are checked below.
        let sql = "SELECT * FROM Item WHERE qty > 0";
        let actual = plan(sql, ddl);
        let expected = native_query(concat!(
            r#"SELECT "id", "qty" "#,
            r#"FROM "Item" "#,
            r#"WHERE ("qty" > ?1) "#,
            r#"ORDER BY "_rowid_" ASC"#,
        ))
        .params([0_i64])
        .columns([("id", "INT"), ("qty", "INT")])
        .alias_as("Item")
        .select();
        assert_query(actual, expected, ddl);

        // PrimaryKey and Index access paths are preserved.
        for source in [
            table("Item").index_by(primary_key().eq(value(1_i64))),
            table("Item").index_by(non_clustered("idx".to_owned())),
        ] {
            let expected = source.select().filter("qty > 1").project("id");
            let StatementPlan {
                kind: StatementPlanKind::Query(mut actual),
                schema_map: SchemaMapPlan::Planned(schemas),
                ..
            } = expected.clone().with_schema(ddl).build().unwrap()
            else {
                panic!("expected a query with a prepared schema");
            };
            plan_native_filter(&mut actual, &schemas);
            assert_query(actual, expected, ddl);
        }

        // The source must have a schema.
        let (mut actual, _) = input(
            "SELECT id FROM Item WHERE qty > 1",
            "CREATE TABLE Item (id INT, qty INT)",
        );
        let expected = actual.clone();
        plan_native_filter(&mut actual, &SchemaMap::new());
        assert_eq!(actual, expected, "the table schema is missing");

        // Schemaless rows have no fixed output columns for native decoding.
        let ddl = "CREATE TABLE Item";
        let actual = plan("SELECT * FROM Item WHERE qty > 1", ddl);
        let expected = table("Item").select().filter("qty > 1");
        assert_query(actual, expected, ddl);

        // Column aliases must fit the schema and have unique logical names.
        let ddl = "CREATE TABLE Item (id INT, qty INT)";
        for (sql, reason) in [
            (
                "SELECT * FROM Item i (a, b, c) WHERE a > 1",
                "aliases exceed the number of physical columns",
            ),
            (
                "SELECT * FROM Item i (same, same) WHERE i.same > 1",
                "duplicate aliases do not identify a unique output column",
            ),
        ] {
            let (mut actual, schemas) = input(sql, ddl);
            let expected = actual.clone();
            plan_native_filter(&mut actual, &schemas);
            assert_eq!(actual, expected, "{reason}: {sql}");
        }

        // TEXT storage order for DECIMAL does not preserve logical key order.
        let ddl = "CREATE TABLE Item (id DECIMAL PRIMARY KEY, qty INT)";
        let actual = plan("SELECT * FROM Item WHERE qty > 1", ddl);
        let expected = table("Item").select().filter("qty > 1");
        assert_query(actual, expected, ddl);

        // A supported PK supplies the source order, even when it is not projected.
        let ddl = "CREATE TABLE Item (id INT PRIMARY KEY, name TEXT, qty INT)";
        let sql = "
            SELECT DISTINCT LOWER(i.name) AS label
            FROM Item i
            WHERE i.qty >= 10
            ORDER BY label
            LIMIT 2 OFFSET 1
        ";
        let actual = plan(sql, ddl);
        let expected = native_query(concat!(
            r#"SELECT "name", "qty" "#,
            r#"FROM "Item" "#,
            r#"WHERE ("qty" >= ?1) "#,
            r#"ORDER BY "id" ASC"#,
        ))
        .params([10_i64])
        .columns([("name", "TEXT"), ("qty", "INT")])
        .alias_as("i")
        .select()
        .project("LOWER(i.name) AS label")
        .order_by("label")
        .distinct()
        .offset(1)
        .limit(2);
        assert_query(actual, expected, ddl);

        // Every SQLite rowid spelling is shadowed by a user column.
        let ddl = "
            CREATE TABLE Item (
                id INT,
                qty INT,
                rowid INT,
                _rowid_ INT,
                oid INT
            )
        ";
        let actual = plan("SELECT id FROM Item WHERE qty > 1", ddl);
        let expected = table("Item").select().filter("qty > 1").project("id");
        assert_query(actual, expected, ddl);

        // No compiled conditions: keep the original filter.
        let ddl = "CREATE TABLE Item (id INT, qty INT)";

        // Arithmetic expressions are evaluated by the common executor.
        let sql = "SELECT id FROM Item WHERE qty + 1 > 2";
        let actual = plan(sql, ddl);
        let expected = table("Item").select().filter("qty + 1 > 2").project("id");
        assert_query(actual, expected, ddl);

        // An outer row cannot be bound as a constant native parameter.
        let sql = "SELECT id FROM Item WHERE outer_row.qty > 1";
        let actual = plan(sql, ddl);
        let expected = table("Item")
            .select()
            .filter("outer_row.qty > 1")
            .project("id");
        assert_query(actual, expected, ddl);

        // An unresolved column leaves this condition in the common Filter.
        let sql = "SELECT id FROM Item WHERE unknown > 1";
        let actual = plan(sql, ddl);
        let expected = table("Item").select().filter("unknown > 1").project("id");
        assert_query(actual, expected, ddl);

        let ddl = "CREATE TABLE Numbers (id INT, n INT8, f FLOAT32, d FLOAT)";

        // The literal cannot be represented in the column's INT8 type.
        let sql = "SELECT id FROM Numbers WHERE n < 1000";
        let actual = plan(sql, ddl);
        let expected = table("Numbers").select().filter("n < 1000").project("id");
        assert_query(actual, expected, ddl);

        // FLOAT support is limited to ordering comparisons.
        let sql = "SELECT id FROM Numbers WHERE d = 0.1";
        let actual = plan(sql, ddl);
        let expected = table("Numbers").select().filter("d = 0.1").project("id");
        assert_query(actual, expected, ddl);

        // Comparing FLOAT32 with FLOAT would require a separate conversion rule.
        let sql = "SELECT id FROM Numbers WHERE f < d";
        let actual = plan(sql, ddl);
        let expected = table("Numbers").select().filter("f < d").project("id");
        assert_query(actual, expected, ddl);

        // Requested output columns must all exist in the schema.
        let (mut actual, schemas) = input(
            "SELECT id FROM Item WHERE qty > 1",
            "CREATE TABLE Item (id INT, qty INT)",
        );
        let ProjectInputPlan::Filter(filter) = &mut actual.project_mut().unwrap().input else {
            panic!("expected filter");
        };
        let FilterInputPlan::Source(SourcePlan::Table(table)) = &mut filter.input else {
            panic!("expected table source");
        };
        table.access = TableAccessPlan::ProjectedScan {
            columns: vec!["missing".to_owned()],
        };
        let expected = actual.clone();
        plan_native_filter(&mut actual, &schemas);
        assert_eq!(
            actual, expected,
            "a projected column is missing from the schema"
        );

        // An empty projection still needs a SQLite SELECT item.
        let ddl = "CREATE TABLE Item (id INT, qty INT)";
        let sql = "SELECT 1 FROM Item WHERE TRUE";
        let actual = plan(sql, ddl);
        let expected = native_query(r#"SELECT 1 FROM "Item" WHERE ?1 ORDER BY "_rowid_" ASC"#)
            .params([true])
            .alias_as("Item")
            .select()
            .project("1");
        assert_query(actual, expected, ddl);

        // Partial: retain the residual filter and its logical column aliases.
        let ddl = "CREATE TABLE Item (id INT, name TEXT, qty INT)";
        let sql = "
            SELECT i.key
            FROM Item AS i (key, label, amount)
            WHERE i.amount > 10
              AND LOWER(i.label) = 'apple'
        ";
        let actual = plan(sql, ddl);
        let expected = native_query(concat!(
            r#"SELECT "id", "name", "qty" "#,
            r#"FROM "Item" "#,
            r#"WHERE ("qty" > ?1) "#,
            r#"ORDER BY "_rowid_" ASC"#,
        ))
        .params([10_i64])
        .columns([("key", "INT"), ("label", "TEXT"), ("amount", "INT")])
        .alias_as("i")
        .select()
        .filter("LOWER(i.label) = 'apple'")
        .project("i.key");
        assert_query(actual, expected, ddl);

        // Full: replace the filter, keeping physical SQL and logical output names.
        let sql = "
            SELECT i.label
            FROM Item AS i (key, label, amount)
            WHERE i.amount > 0
        ";
        let actual = plan(sql, ddl);
        let expected = native_query(concat!(
            r#"SELECT "name", "qty" "#,
            r#"FROM "Item" "#,
            r#"WHERE ("qty" > ?1) "#,
            r#"ORDER BY "_rowid_" ASC"#,
        ))
        .params([0_i64])
        .columns([("label", "TEXT"), ("amount", "INT")])
        .alias_as("i")
        .select()
        .project("i.label");
        assert_query(actual, expected, ddl);
    }
}
