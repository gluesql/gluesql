//! Builds a native `SELECT` for one table and determines its scan order.

use {
    crate::schema::{available_rowid, key_type_supports_sqlite_order, qualified_column, quote},
    gluesql_core::plan::{
        ColumnDefPlan, NativeColumnPlan, NativeSourcePlan, TableAccessPlan, TableSourcePlan,
    },
};

pub(super) struct ScanColumn<'a> {
    pub(super) column: &'a ColumnDefPlan,
    pub(super) label: &'a str,
}

pub(super) fn scan_projection<'a>(
    table: &'a TableSourcePlan,
    columns: &'a [ColumnDefPlan],
) -> Option<Vec<ScanColumn<'a>>> {
    let selected = match &table.access {
        TableAccessPlan::FullScan => None,
        TableAccessPlan::ProjectedScan { columns } => Some(columns),
        TableAccessPlan::PrimaryKey { .. } | TableAccessPlan::Index { .. } => return None,
    };

    let aliases = table.alias.as_ref().map_or(&[][..], |alias| &alias.columns);
    if aliases.len() > columns.len() {
        return None;
    }

    let projection: Vec<_> = columns
        .iter()
        .enumerate()
        .filter(|(_, column)| selected.is_none_or(|names| names.contains(&column.name)))
        .map(|(index, column)| ScanColumn {
            column,
            label: aliases.get(index).unwrap_or(&column.name),
        })
        .collect();

    if selected.is_some_and(|names| names.len() != projection.len()) {
        return None;
    }

    Some(projection)
}

#[derive(Debug, PartialEq)]
pub(super) enum ScanOrder<'a> {
    Sqlite(String),
    GlueSql(&'a ColumnDefPlan),
    Unavailable,
}

pub(super) fn scan_order<'a>(table: &str, columns: &'a [ColumnDefPlan]) -> ScanOrder<'a> {
    let key = columns
        .iter()
        .find(|column| column.unique.is_some_and(|unique| unique.is_primary));
    let name = match key {
        None => match available_rowid(columns, |column| &column.name) {
            Some(rowid) => rowid,
            None => return ScanOrder::Unavailable,
        },
        Some(key) if key_type_supports_sqlite_order(&key.data_type) => &key.name,
        Some(key) => return ScanOrder::GlueSql(key),
    };

    ScanOrder::Sqlite(format!("ORDER BY {} ASC", qualified_column(table, name)))
}

pub(super) fn native_select(
    table: &TableSourcePlan,
    projection: Vec<ScanColumn<'_>>,
) -> NativeSourcePlan {
    let mut items = Vec::with_capacity(projection.len());
    let mut columns = Vec::with_capacity(projection.len());
    for ScanColumn { column, label } in projection {
        items.push(qualified_column(&table.name, &column.name));
        columns.push(NativeColumnPlan {
            name: label.to_owned(),
            data_type: column.data_type.clone(),
        });
    }

    // SQLite requires a SELECT item even when GlueSQL only needs the row count.
    if items.is_empty() {
        items.push("1".to_owned());
    }

    NativeSourcePlan {
        query: format!("SELECT {} FROM {}", items.join(", "), quote(&table.name)),
        params: vec![],
        alias: table
            .alias
            .as_ref()
            .map_or(&table.name, |alias| &alias.name)
            .clone(),
        columns,
    }
}

#[cfg(test)]
mod tests {
    use {
        super::{ScanColumn, ScanOrder},
        gluesql_core::{
            ast::{ColumnDef, DataType},
            data::Value,
            plan::{
                ColumnDefPlan, ExprPlan, NativeColumnPlan, NativeSourcePlan, TableAccessPlan,
                TableAliasPlan, TableSourcePlan,
            },
            query_builder::ColumnDefNode,
        },
    };

    fn column(definition: &str) -> ColumnDefPlan {
        ColumnDef::try_from(ColumnDefNode::from(definition))
            .expect(definition)
            .into()
    }

    #[test]
    fn scan_projection() {
        let columns = [column("id INT"), column("name TEXT"), column("qty INT")];

        let item = TableSourcePlan {
            name: "Item".to_owned(),
            alias: Some(TableAliasPlan {
                name: "i".to_owned(),
                columns: vec!["key".to_owned()],
            }),
            access: TableAccessPlan::FullScan,
        };
        let actual: Vec<_> = super::scan_projection(&item, &columns)
            .unwrap()
            .into_iter()
            .map(|ScanColumn { column, label }| (column.name.as_str(), label))
            .collect();
        let expected = [("id", "key"), ("name", "name"), ("qty", "qty")];
        assert_eq!(actual, expected);

        let table = TableSourcePlan {
            access: TableAccessPlan::ProjectedScan {
                columns: vec!["qty".to_owned(), "id".to_owned()],
            },
            ..item.clone()
        };
        let actual: Vec<_> = super::scan_projection(&table, &columns)
            .unwrap()
            .into_iter()
            .map(|ScanColumn { column, label }| (column.name.as_str(), label))
            .collect();
        let expected = [("id", "key"), ("qty", "qty")];
        assert_eq!(actual, expected);

        let table = TableSourcePlan {
            access: TableAccessPlan::ProjectedScan { columns: vec![] },
            ..item.clone()
        };
        let actual = super::scan_projection(&table, &columns).unwrap();
        assert!(actual.is_empty());

        let table = TableSourcePlan {
            access: TableAccessPlan::PrimaryKey {
                expr: ExprPlan::Value(Value::I64(1)),
            },
            ..item.clone()
        };
        assert!(super::scan_projection(&table, &columns).is_none());

        let table = TableSourcePlan {
            access: TableAccessPlan::Index {
                name: "by_qty".to_owned(),
                asc: None,
                predicate: None,
            },
            ..item.clone()
        };
        assert!(super::scan_projection(&table, &columns).is_none());

        let table = TableSourcePlan {
            alias: Some(TableAliasPlan {
                name: "i".to_owned(),
                columns: ["key", "label", "amount", "extra"]
                    .map(str::to_owned)
                    .into(),
            }),
            ..item.clone()
        };
        assert!(super::scan_projection(&table, &columns).is_none());

        let table = TableSourcePlan {
            alias: None,
            access: TableAccessPlan::ProjectedScan {
                columns: vec!["missing".to_owned()],
            },
            ..item
        };
        assert!(super::scan_projection(&table, &columns).is_none());
    }

    #[test]
    fn scan_order() {
        let columns = [column("_rowid_ INT")];
        let actual = super::scan_order("Item", &columns);
        let expected = ScanOrder::Sqlite(r#"ORDER BY "Item"."rowid" ASC"#.to_owned());
        assert_eq!(actual, expected);

        let columns = [
            column("_rowid_ INT"),
            column("rowid INT"),
            column("oid INT"),
        ];
        let actual = super::scan_order("Item", &columns);
        assert_eq!(actual, ScanOrder::Unavailable);

        for key_type in [
            "INT8", "INT16", "INT32", "INT", "UINT8", "UINT16", "UINT32", "BOOLEAN", "TEXT",
            "BYTEA", "DECIMAL",
        ] {
            let columns = [column(&format!("id {key_type} PRIMARY KEY"))];
            let actual = super::scan_order("Item", &columns);
            let expected = ScanOrder::Sqlite(r#"ORDER BY "Item"."id" ASC"#.to_owned());
            assert_eq!(actual, expected, "{key_type}");
        }

        let columns = [column("name TEXT"), column("id INT128 PRIMARY KEY")];
        let actual = super::scan_order("Item", &columns);
        let expected = ScanOrder::GlueSql(&columns[1]);
        assert_eq!(actual, expected);
    }

    #[test]
    fn native_select() {
        let name = column("name TEXT");
        let table = TableSourcePlan {
            name: "Item".to_owned(),
            alias: Some(TableAliasPlan {
                name: "i".to_owned(),
                columns: vec!["label".to_owned()],
            }),
            access: TableAccessPlan::FullScan,
        };
        let projection = vec![ScanColumn {
            column: &name,
            label: "label",
        }];
        let actual = super::native_select(&table, projection);
        let expected = NativeSourcePlan {
            query: r#"SELECT "Item"."name" FROM "Item""#.to_owned(),
            params: vec![],
            alias: "i".to_owned(),
            columns: vec![NativeColumnPlan {
                name: "label".to_owned(),
                data_type: DataType::Text,
            }],
        };
        assert_eq!(actual, expected);

        let table = TableSourcePlan {
            alias: None,
            access: TableAccessPlan::ProjectedScan { columns: vec![] },
            ..table
        };
        let actual = super::native_select(&table, vec![]);
        let expected = NativeSourcePlan {
            query: r#"SELECT 1 FROM "Item""#.to_owned(),
            params: vec![],
            alias: "Item".to_owned(),
            columns: vec![],
        };
        assert_eq!(actual, expected);
    }
}
