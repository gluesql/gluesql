//! Rewrites single-table filters into native sources.
//! Conditions that cannot be delegated stay in a common Filter above the source.

mod condition;

use {
    super::table_read::{self, ScanOrder},
    gluesql_core::{
        plan::{
            ExprPlan, FilterInputPlan, FilterPlan, NativeSourcePlan, ProjectInputPlan, QueryPlan,
            SchemaMap, SourcePlan,
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

    let Some(schema) = schemas.get(&table.name) else {
        return NativeFilterRewrite::Unchanged;
    };

    let Some(columns) = schema.column_defs.as_ref() else {
        return NativeFilterRewrite::Unchanged;
    };

    let Some(projection) = table_read::scan_projection(table, columns) else {
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

    // Duplicate logical names make filter operands ambiguous.
    let mut names = BTreeSet::new();
    if columns
        .iter()
        .enumerate()
        .any(|(index, column)| !names.insert(column_aliases.get(index).unwrap_or(&column.name)))
    {
        return NativeFilterRewrite::Unchanged;
    }

    let Some(ScanOrder::Sqlite(order)) = table_read::scan_order(&table.name, columns) else {
        return NativeFilterRewrite::Unchanged;
    };

    let Some(condition) =
        condition::plan(&filter.expr, &table.name, alias, columns, column_aliases)
    else {
        return NativeFilterRewrite::Unchanged;
    };

    let mut source = table_read::native_select(table, projection);
    source.query = format!("{} WHERE {} {order}", source.query, condition.sql);
    source.params = condition.params;

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
            plan::{QueryPlan, SchemaMap, SchemaMapPlan, StatementPlan, StatementPlanKind},
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
            r#"SELECT "Item"."id", "Item"."qty" "#,
            r#"FROM "Item" "#,
            r#"WHERE ("Item"."qty" > ?1) "#,
            r#"ORDER BY "Item"."_rowid_" ASC"#,
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
            r#"SELECT "Item"."qty" "#,
            r#"FROM "Item" "#,
            r#"WHERE ("Item"."qty" > ?1) "#,
            r#"ORDER BY "Item"."_rowid_" ASC"#,
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
            r#"SELECT "Item"."id", "Item"."qty" "#,
            r#"FROM "Item" "#,
            r#"WHERE ("Item"."qty" > ?1) "#,
            r#"ORDER BY "Item"."_rowid_" ASC"#,
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

        // A table filter needs a schema with fixed columns.
        let (mut actual, _) = input("SELECT id FROM Item WHERE qty > 1", ddl);
        plan_native_filter(&mut actual, &SchemaMap::new());
        let expected = table("Item").select().filter("qty > 1").project("id");
        assert_query(actual, expected, ddl);

        let schemaless = "CREATE TABLE Item";
        let actual = plan("SELECT * FROM Item WHERE qty > 1", schemaless);
        let expected = table("Item").select().filter("qty > 1");
        assert_query(actual, expected, schemaless);

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

        // Duplicate logical names make the filter column ambiguous.
        let (expected, schemas) = input("SELECT * FROM Item i (same, same) WHERE i.same > 1", ddl);
        let mut actual = expected.clone();
        plan_native_filter(&mut actual, &schemas);
        assert_eq!(actual, expected);

        // GlueSQL must sort an INT128 primary key.
        let sql = "SELECT id FROM Item WHERE qty > 1";
        let wide_key = "CREATE TABLE Item (id INT128 PRIMARY KEY, qty INT)";
        let actual = plan(sql, wide_key);
        let expected = table("Item").select().filter("qty > 1").project("id");
        assert_query(actual, expected, wide_key);

        // With no primary key, shadowing all three rowid names leaves no ordering key.
        let shadowed_rowid = "CREATE TABLE Item (id INT, qty INT, _rowid_ INT, rowid INT, oid INT)";
        let actual = plan(sql, shadowed_rowid);
        let expected = table("Item").select().filter("qty > 1").project("id");
        assert_query(actual, expected, shadowed_rowid);

        // With no native condition, the Filter keeps its original table source.
        let sql = "SELECT id FROM Item WHERE qty + 1 > 2";
        let actual = plan(sql, ddl);
        let expected = table("Item").select().filter("qty + 1 > 2").project("id");
        assert_query(actual, expected, ddl);

        // A partial rewrite leaves the residual filter on the native source.
        let ddl = "CREATE TABLE Item (id INT, name TEXT, qty INT)";
        let sql = "
            SELECT i.key
            FROM Item AS i (key, label, amount)
            WHERE i.amount > 10
              AND LOWER(i.label) = 'apple'
        ";
        let actual = plan(sql, ddl);
        let expected = native_query(concat!(
            r#"SELECT "Item"."id", "Item"."name", "Item"."qty" "#,
            r#"FROM "Item" "#,
            r#"WHERE ("Item"."qty" > ?1) "#,
            r#"ORDER BY "Item"."_rowid_" ASC"#,
        ))
        .params([10_i64])
        .columns([("key", "INT"), ("label", "TEXT"), ("amount", "INT")])
        .alias_as("i")
        .select()
        .filter("LOWER(i.label) = 'apple'")
        .project("i.key");
        assert_query(actual, expected, ddl);

        // The native source exposes the logical names used by the projection.
        let sql = "
            SELECT i.label
            FROM Item AS i (key, label, amount)
            WHERE i.amount > 0
        ";
        let actual = plan(sql, ddl);
        let expected = native_query(concat!(
            r#"SELECT "Item"."name", "Item"."qty" "#,
            r#"FROM "Item" "#,
            r#"WHERE ("Item"."qty" > ?1) "#,
            r#"ORDER BY "Item"."_rowid_" ASC"#,
        ))
        .params([0_i64])
        .columns([("label", "TEXT"), ("amount", "INT")])
        .alias_as("i")
        .select()
        .project("i.label");
        assert_query(actual, expected, ddl);

        // A fully delegated filter keeps all columns for SELECT *.
        let ddl = "CREATE TABLE Item (id INT, qty INT)";
        let sql = "SELECT * FROM Item WHERE qty > 0";
        let actual = plan(sql, ddl);
        let expected = native_query(concat!(
            r#"SELECT "Item"."id", "Item"."qty" "#,
            r#"FROM "Item" "#,
            r#"WHERE ("Item"."qty" > ?1) "#,
            r#"ORDER BY "Item"."_rowid_" ASC"#,
        ))
        .params([0_i64])
        .columns([("id", "INT"), ("qty", "INT")])
        .alias_as("Item")
        .select();
        assert_query(actual, expected, ddl);

        // An empty projection still needs a SQLite SELECT item.
        let sql = "SELECT 1 FROM Item WHERE TRUE";
        let actual = plan(sql, ddl);
        let expected =
            native_query(r#"SELECT 1 FROM "Item" WHERE ?1 ORDER BY "Item"."_rowid_" ASC"#)
                .params([true])
                .alias_as("Item")
                .select()
                .project("1");
        assert_query(actual, expected, ddl);

        // The unprojected PK orders the source beneath DISTINCT, ORDER BY and LIMIT.
        let ddl = "CREATE TABLE Item (id DECIMAL PRIMARY KEY, name TEXT, qty INT)";
        let sql = "
            SELECT DISTINCT LOWER(i.name) AS label
            FROM Item i
            WHERE i.qty >= 10
            ORDER BY label
            LIMIT 2 OFFSET 1
        ";
        let actual = plan(sql, ddl);
        let expected = native_query(concat!(
            r#"SELECT "Item"."name", "Item"."qty" "#,
            r#"FROM "Item" "#,
            r#"WHERE ("Item"."qty" >= ?1) "#,
            r#"ORDER BY "Item"."id" ASC"#,
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
    }
}
