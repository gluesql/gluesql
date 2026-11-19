//! Plans native reads for table scans left by primary-key and filter planning.

use {
    super::table_read::{self, ScanColumn, ScanOrder},
    crate::schema::document_column,
    gluesql_core::{
        plan::{
            ColumnDefPlan, DerivedSourcePlan, ExprPlan, OrderByExprPlan, ProjectInputPlan,
            ProjectPlan, ProjectionPlan, QueryPlan, SchemaMap, SchemaPlan, SelectItemPlan,
            SelectOrderByPlan, SourcePlan, TableAliasPlan, TableSourcePlan,
        },
        planner::{VisitorMut, walk_source_mut},
    },
    std::{borrow::Cow, convert::Infallible},
};

pub(super) fn plan(query: &mut QueryPlan, schemas: &SchemaMap) {
    let Ok(()) = NativeScanPlanner { schemas }.visit_query(query);
}

struct NativeScanPlanner<'a> {
    schemas: &'a SchemaMap,
}

impl VisitorMut for NativeScanPlanner<'_> {
    type Error = Infallible;

    fn visit_source(&mut self, source: &mut SourcePlan) -> Result<(), Self::Error> {
        if let SourcePlan::Table(table) = source
            && let Some(schema) = self.schemas.get(&table.name)
            && let Some(scan) = plan_table_scan(table, schema)
        {
            *source = scan;
            return Ok(());
        }

        walk_source_mut(self, source)
    }
}

fn plan_table_scan(table: &TableSourcePlan, schema: &SchemaPlan) -> Option<SourcePlan> {
    let columns = match schema.column_defs.as_deref() {
        Some(columns) => Cow::Borrowed(columns),
        None => Cow::Owned(vec![document_column().into()]),
    };
    let projection = table_read::scan_projection(table, &columns)?;

    match table_read::scan_order(&table.name, &columns)? {
        ScanOrder::Sqlite(order) => {
            let mut source = table_read::native_select(table, projection);
            source.query = format!("{} {order}", source.query);
            Some(SourcePlan::Native(source))
        }
        ScanOrder::GlueSql(key) => Some(sort_by_primary_key(table, projection, key)),
    }
}

fn sort_by_primary_key(
    table: &TableSourcePlan,
    projection: Vec<ScanColumn<'_>>,
    key: &ColumnDefPlan,
) -> SourcePlan {
    let alias = table
        .alias
        .as_ref()
        .map_or(&table.name, |alias| &alias.name);
    let identifier = |name: &str| ExprPlan::CompoundIdentifier {
        alias: alias.clone(),
        ident: name.to_owned(),
    };

    // Resolve the inner sort against physical names even when user aliases repeat.
    let mut items = Vec::with_capacity(projection.len());
    let mut labels = Vec::with_capacity(projection.len());
    let mut read_columns = Vec::with_capacity(projection.len());
    for ScanColumn { column, label } in projection {
        items.push(SelectItemPlan::Expr {
            expr: identifier(&column.name),
            label: column.name.clone(),
        });
        labels.push(label.to_owned());
        read_columns.push(ScanColumn {
            column,
            label: &column.name,
        });
    }

    if !read_columns.iter().any(|item| item.column.name == key.name) {
        read_columns.push(ScanColumn {
            column: key,
            label: &key.name,
        });
    }

    let source = table_read::native_select(table, read_columns);
    let query = QueryPlan::SelectOrderBy(SelectOrderByPlan {
        input: ProjectPlan {
            input: ProjectInputPlan::Source(SourcePlan::Native(source)),
            projection: ProjectionPlan::SelectItems(items),
        },
        exprs: vec![OrderByExprPlan {
            expr: identifier(&key.name),
            asc: Some(true),
        }],
    });

    SourcePlan::Derived(DerivedSourcePlan {
        query: Box::new(query),
        alias: TableAliasPlan {
            name: alias.clone(),
            columns: labels,
        },
    })
}

#[cfg(test)]
mod tests {
    use {
        super::{NativeScanPlanner, ScanColumn},
        gluesql_core::{
            data::Schema,
            plan::{
                ProjectInputPlan, ProjectPlan, QueryPlan, SchemaPlan, SourcePlan,
                StatementPlanKind, TableAccessPlan, TableAliasPlan, TableSourcePlan,
            },
            planner::VisitorMut,
            query_builder::{Build, SourceNode, native_query, primary_key, subquery, table},
        },
    };

    fn schema(ddl: &str) -> SchemaPlan {
        Schema::from_ddl(ddl).expect(ddl).into()
    }

    fn projected_scan(names: &[&str]) -> TableAccessPlan {
        TableAccessPlan::ProjectedScan {
            columns: names.iter().map(|name| (*name).to_owned()).collect(),
        }
    }

    fn table_source(name: &str, access: TableAccessPlan) -> TableSourcePlan {
        TableSourcePlan {
            name: name.to_owned(),
            alias: None,
            access,
        }
    }

    fn build_source(node: SourceNode<'_>) -> SourcePlan {
        let StatementPlanKind::Query(QueryPlan::Project(ProjectPlan {
            input: ProjectInputPlan::Source(source),
            ..
        })) = node.select().build().unwrap().kind
        else {
            panic!("expected a SELECT from one source");
        };
        source
    }

    #[test]
    fn visit_source() {
        let schemas = [(
            "Item".to_owned(),
            schema("CREATE TABLE Item (id INT PRIMARY KEY)"),
        )]
        .into();
        let mut planner = NativeScanPlanner { schemas: &schemas };

        let mut actual = SourcePlan::Table(table_source("Item", projected_scan(&[])));
        planner.visit_source(&mut actual).unwrap();
        let expected = build_source(
            native_query(r#"SELECT 1 FROM "Item" ORDER BY "Item"."id" ASC"#).alias_as("Item"),
        );
        assert_eq!(actual, expected);

        // A second visit leaves the native source intact.
        planner.visit_source(&mut actual).unwrap();
        assert_eq!(actual, expected);

        let missing = SourcePlan::Table(table_source("Missing", TableAccessPlan::FullScan));
        let mut actual = missing.clone();
        planner.visit_source(&mut actual).unwrap();
        assert_eq!(actual, missing);

        // A lookup keeps its access path while its key's subquery is visited.
        let key = table("Item")
            .alias_as("i")
            .select()
            .project("i.id")
            .limit(1);
        let mut actual = build_source(table("Item").index_by(primary_key().eq(subquery(key))));
        planner.visit_source(&mut actual).unwrap();

        let key = native_query(r#"SELECT "Item"."id" FROM "Item" ORDER BY "Item"."id" ASC"#)
            .columns([("id", "INT")])
            .alias_as("i")
            .select()
            .project("i.id")
            .limit(1);
        let expected = build_source(table("Item").index_by(primary_key().eq(subquery(key))));
        assert_eq!(actual, expected);
    }

    #[test]
    fn plan_table_scan() {
        let items = schema("CREATE TABLE Item (id INT, name TEXT)");
        let table = table_source("Item", TableAccessPlan::FullScan);
        let actual = super::plan_table_scan(&table, &items);
        let expected = native_query(concat!(
            r#"SELECT "Item"."id", "Item"."name" FROM "Item" "#,
            r#"ORDER BY "Item"."_rowid_" ASC"#,
        ))
        .columns([("id", "INT"), ("name", "TEXT")])
        .alias_as("Item");
        assert_eq!(actual, Some(build_source(expected)));

        let table = TableSourcePlan {
            alias: Some(TableAliasPlan {
                name: "i".to_owned(),
                columns: vec![],
            }),
            ..table_source("Item", projected_scan(&["name"]))
        };
        let actual = super::plan_table_scan(&table, &items);
        let expected =
            native_query(r#"SELECT "Item"."name" FROM "Item" ORDER BY "Item"."_rowid_" ASC"#)
                .columns([("name", "TEXT")])
                .alias_as("i");
        assert_eq!(actual, Some(build_source(expected)));

        let docs = schema("CREATE TABLE Doc");
        let table = table_source("Doc", TableAccessPlan::FullScan);
        let actual = super::plan_table_scan(&table, &docs);
        let expected =
            native_query(r#"SELECT "Doc"."_doc" FROM "Doc" ORDER BY "Doc"."_rowid_" ASC"#)
                .columns([("_doc", "MAP")])
                .alias_as("Doc");
        assert_eq!(actual, Some(build_source(expected)));

        let table = table_source("Item", projected_scan(&["missing"]));
        assert_eq!(super::plan_table_scan(&table, &items), None);

        let items = schema("CREATE TABLE Item (_rowid_ INT, rowid INT, oid INT)");
        let table = table_source("Item", TableAccessPlan::FullScan);
        assert_eq!(super::plan_table_scan(&table, &items), None);

        // Even a query with no visible columns needs the key to sort its rows.
        let items = schema("CREATE TABLE Item (id INT128 PRIMARY KEY)");
        let table = table_source("Item", projected_scan(&[]));
        let actual = super::plan_table_scan(&table, &items).unwrap();
        let expected = native_query(r#"SELECT "Item"."id" FROM "Item""#)
            .columns([("id", "INT128")])
            .alias_as("Item")
            .select()
            .project(Vec::<&str>::new())
            .order_by("Item.id ASC");
        assert_sorted_source(actual, "Item", &[], expected);
    }

    #[test]
    fn sort_by_primary_key() {
        let columns = schema("CREATE TABLE Item (name TEXT, id INT128 PRIMARY KEY)")
            .column_defs
            .unwrap();
        let name = &columns[0];
        let id = &columns[1];
        let table = TableSourcePlan {
            alias: Some(TableAliasPlan {
                name: "i".to_owned(),
                columns: vec!["label".to_owned(), "key".to_owned()],
            }),
            ..table_source("Item", TableAccessPlan::FullScan)
        };
        let native = native_query(r#"SELECT "Item"."name", "Item"."id" FROM "Item""#)
            .columns([("name", "TEXT"), ("id", "INT128")])
            .alias_as("i");

        // A visible key is read once and keeps its output alias.
        let projection = vec![
            ScanColumn {
                column: name,
                label: "label",
            },
            ScanColumn {
                column: id,
                label: "key",
            },
        ];
        let actual = super::sort_by_primary_key(&table, projection, id);
        let expected = native
            .clone()
            .select()
            .project("i.name AS name, i.id AS id")
            .order_by("i.id ASC");
        assert_sorted_source(actual, "i", &["label", "key"], expected);

        // A hidden key is added to the inner read. Only the name is returned.
        let table = TableSourcePlan {
            access: projected_scan(&["name"]),
            ..table
        };
        let projection = vec![ScanColumn {
            column: name,
            label: "label",
        }];
        let actual = super::sort_by_primary_key(&table, projection, id);
        let expected = native
            .select()
            .project("i.name AS name")
            .order_by("i.id ASC");
        assert_sorted_source(actual, "i", &["label"], expected);
    }

    fn assert_sorted_source(
        actual: SourcePlan,
        alias: &str,
        columns: &[&str],
        expected: impl Build,
    ) {
        let SourcePlan::Derived(sorted) = actual else {
            panic!("expected a derived source, got {actual:?}");
        };
        assert_eq!(sorted.alias.name, alias);
        assert_eq!(sorted.alias.columns, columns);
        assert_eq!(
            StatementPlanKind::Query(*sorted.query),
            expected.build().unwrap().kind
        );
    }
}
