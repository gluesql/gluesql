use {
    super::{Build, ColumnDefNode, ColumnList, ExprList, ExprNode, QueryBuilderError, QueryNode},
    crate::{
        ast::ColumnDef,
        plan::{ColumnDefPlan, StatementPlan, TableColumnsPlan},
        result::Result,
        row_conversion::ToGlueRow,
    },
    std::borrow::Cow,
};

#[derive(Clone, Debug)]
enum InsertTableColumns {
    Unplanned,
    Schemaless,
    Columns(Vec<ColumnDefNode>),
}

#[derive(Clone, Debug)]
pub struct InsertTableNode {
    table_name: String,
    table_columns: InsertTableColumns,
}

impl InsertTableNode {
    pub(crate) fn columns(table_name: String, columns: Vec<ColumnDefNode>) -> Self {
        Self {
            table_name,
            table_columns: InsertTableColumns::Columns(columns),
        }
    }

    pub(crate) fn schemaless(table_name: String) -> Self {
        Self {
            table_name,
            table_columns: InsertTableColumns::Schemaless,
        }
    }

    pub fn insert(self) -> InsertNode {
        InsertNode {
            table_name: self.table_name,
            columns: None,
            table_columns: self.table_columns,
        }
    }
}

#[derive(Clone, Debug)]
pub struct InsertNode {
    table_name: String,
    columns: Option<ColumnList>,
    table_columns: InsertTableColumns,
}

impl InsertNode {
    pub fn new(table_name: String) -> Self {
        Self {
            table_name,
            columns: None,
            table_columns: InsertTableColumns::Unplanned,
        }
    }

    #[must_use]
    pub fn columns<T: Into<ColumnList>>(mut self, columns: T) -> Self {
        self.columns = Some(columns.into());
        self
    }

    pub fn values<'a, T: Into<ExprList<'a>>>(self, values: Vec<T>) -> InsertSourceNode<'a> {
        let values: Vec<ExprList> = values.into_iter().map(Into::into).collect();

        InsertSourceNode {
            insert_node: self,
            source: QueryNode::Values(values),
        }
    }

    pub fn as_select<'a, T: Into<QueryNode<'a>>>(self, query: T) -> InsertSourceNode<'a> {
        InsertSourceNode {
            insert_node: self,
            source: query.into(),
        }
    }

    /// Builds VALUES rows from [`ToGlueRow`] structs. Columns are always set
    /// from the struct metadata, replacing any set beforehand.
    pub fn values_from<'a, T: ToGlueRow>(self, rows: &[T]) -> Result<InsertSourceNode<'a>> {
        if rows.is_empty() {
            return Err(QueryBuilderError::ValuesFromRequiresRows.into());
        }

        let columns = T::glue_columns().to_vec();
        let values = rows
            .iter()
            .map(|row| {
                let literals = row.to_glue_row();
                if literals.len() != columns.len() {
                    return Err(QueryBuilderError::ValuesFromColumnCountMismatch {
                        expected: columns.len(),
                        found: literals.len(),
                    }
                    .into());
                }

                Ok(literals
                    .into_iter()
                    .map(|literal| ExprNode::Expr(Cow::Owned(literal.into_expr())))
                    .collect::<Vec<_>>()
                    .into())
            })
            .collect::<Result<_>>()?;

        Ok(InsertSourceNode {
            insert_node: self.columns(columns),
            source: QueryNode::Values(values),
        })
    }
}

#[derive(Clone, Debug)]
pub struct InsertSourceNode<'a> {
    insert_node: InsertNode,
    source: QueryNode<'a>,
}

impl Build for InsertSourceNode<'_> {
    fn build(self) -> Result<StatementPlan> {
        let table_name = self.insert_node.table_name;
        let columns = self.insert_node.columns;
        let columns = columns.map_or_else(|| Ok(vec![]), TryInto::try_into)?;
        let source = self.source.build_query_plan()?;

        let table_columns = match self.insert_node.table_columns {
            InsertTableColumns::Unplanned => TableColumnsPlan::Unplanned,
            InsertTableColumns::Schemaless => TableColumnsPlan::Schemaless,
            InsertTableColumns::Columns(columns) => TableColumnsPlan::Columns(
                columns
                    .into_iter()
                    .map(|column| ColumnDef::try_from(column).map(ColumnDefPlan::from))
                    .collect::<Result<Vec<_>>>()?,
            ),
        };

        Ok(StatementPlan::Insert {
            table_name,
            columns,
            source,
            table_columns,
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        ast::ColumnDef,
        data::Value,
        plan::{ColumnDefPlan, StatementPlan, TableColumnsPlan},
        query_builder::{
            Build, ColumnDefNode, QueryBuilderError, num, select::BuildQueryPlan, table, test,
            value, values,
        },
        result::Error,
        row_conversion::ToGlueRow,
        translate::{IntoParamLiteral, ParamLiteral, TranslateError},
    };

    #[test]
    fn insert() {
        let actual = table("Foo").insert().values(vec!["1, 5", "2, 3"]).build();
        let expected = r"INSERT INTO Foo VALUES (1, 5), (2, 3)";
        test(&actual, expected);

        let actual = table("Foo")
            .insert()
            .columns("id, name")
            .values(vec![vec![num(1), num(5)], vec![num(2), num(3)]])
            .build();
        let expected = r"INSERT INTO Foo (id, name) VALUES (1, 5), (2, 3)";
        test(&actual, expected);

        let actual = table("Foo")
            .insert()
            .columns(vec!["hi"])
            .values(vec![vec![num(7)]])
            .build();
        let expected = r"INSERT INTO Foo (hi) VALUES (7)";
        test(&actual, expected);

        let actual = table("Foo")
            .insert()
            .as_select(table("Bar").select().project("id, name").limit(10))
            .build();
        let expected = r"INSERT INTO Foo SELECT id, name FROM Bar LIMIT 10";
        test(&actual, expected);

        let actual = table("Foo")
            .insert()
            .as_select(
                table("Bar")
                    .select()
                    .project("id")
                    .order_by("id")
                    .distinct(),
            )
            .build();
        let expected = r"INSERT INTO Foo SELECT DISTINCT id FROM Bar ORDER BY id";
        test(&actual, expected);
    }

    #[test]
    fn table_columns() {
        let actual = table("Item")
            .table_columns(["id INTEGER NOT NULL DEFAULT 1", "name TEXT NULL"])
            .insert()
            .columns("name")
            .values(vec!["'glue'"])
            .build();
        let expected = Ok(StatementPlan::Insert {
            table_name: "Item".to_owned(),
            columns: vec!["name".to_owned()],
            source: values(vec!["'glue'"]).build_query_plan().unwrap(),
            table_columns: TableColumnsPlan::Columns(vec![
                ColumnDefPlan::from(
                    ColumnDef::try_from(ColumnDefNode::from("id INTEGER NOT NULL DEFAULT 1"))
                        .unwrap(),
                ),
                ColumnDefPlan::from(
                    ColumnDef::try_from(ColumnDefNode::from("name TEXT NULL")).unwrap(),
                ),
            ]),
        });
        pretty_assertions::assert_eq!(actual, expected);

        let actual = table("Log")
            .schemaless()
            .insert()
            .values(vec!["'{}'"])
            .build();
        let expected = Ok(StatementPlan::Insert {
            table_name: "Log".to_owned(),
            columns: Vec::new(),
            source: values(vec!["'{}'"]).build_query_plan().unwrap(),
            table_columns: TableColumnsPlan::Schemaless,
        });
        pretty_assertions::assert_eq!(actual, expected);
    }

    struct Item {
        id: i64,
        name: String,
        in_stock: Option<bool>,
    }

    impl ToGlueRow for Item {
        fn glue_columns() -> &'static [&'static str] {
            static COLUMNS: [&str; 3] = ["id", "title", "in_stock"];
            &COLUMNS
        }

        fn to_glue_row(&self) -> Vec<ParamLiteral> {
            vec![
                self.id.into_param_literal(),
                self.name.clone().into_param_literal(),
                self.in_stock.into_param_literal(),
            ]
        }
    }

    #[test]
    fn insert_values_from() {
        let items = vec![
            Item {
                id: 1,
                name: "glue".to_owned(),
                in_stock: Some(true),
            },
            Item {
                id: 2,
                name: "sql".to_owned(),
                in_stock: None,
            },
        ];

        let actual = table("Foo")
            .insert()
            .values_from(&items)
            .and_then(Build::build);
        let expected = table("Foo")
            .insert()
            .columns(vec!["id", "title", "in_stock"])
            .values(vec![
                vec![
                    value(Value::I64(1)),
                    value(Value::Str("glue".to_owned())),
                    value(Value::Bool(true)),
                ],
                vec![
                    value(Value::I64(2)),
                    value(Value::Str("sql".to_owned())),
                    value(Value::Null),
                ],
            ])
            .build();
        pretty_assertions::assert_eq!(actual, expected);
    }

    #[test]
    fn insert_values_from_replaces_columns() {
        let items = vec![Item {
            id: 7,
            name: "hi".to_owned(),
            in_stock: None,
        }];

        let actual = table("Foo")
            .insert()
            .columns("ignored, also_ignored")
            .values_from(&items)
            .and_then(Build::build);
        let expected = table("Foo")
            .insert()
            .values_from(&items)
            .and_then(Build::build);
        pretty_assertions::assert_eq!(actual, expected);
    }

    #[test]
    fn insert_values_from_requires_rows() {
        let actual = table("Foo").insert().values_from::<Item>(&[]).map(|_| ());
        let expected = Err(Error::QueryBuilder(
            QueryBuilderError::ValuesFromRequiresRows,
        ));
        pretty_assertions::assert_eq!(actual, expected);
    }

    #[test]
    fn insert_values_from_rejects_column_count_mismatch() {
        struct Mismatched;

        impl ToGlueRow for Mismatched {
            fn glue_columns() -> &'static [&'static str] {
                static COLUMNS: [&str; 2] = ["id", "name"];
                &COLUMNS
            }

            fn to_glue_row(&self) -> Vec<ParamLiteral> {
                vec![1_i64.into_param_literal()]
            }
        }

        let actual = table("Foo").insert().values_from(&[Mismatched]).map(|_| ());
        let expected = Err(Error::QueryBuilder(
            QueryBuilderError::ValuesFromColumnCountMismatch {
                expected: 2,
                found: 1,
            },
        ));
        pretty_assertions::assert_eq!(actual, expected);
    }

    #[test]
    fn insert_table_columns_reject_invalid_definitions() {
        let actual = table("Item")
            .table_columns(["id FOOBAR"])
            .insert()
            .values(vec!["1"])
            .build();
        let expected = Err(Error::Translate(TranslateError::UnsupportedDataType(
            "FOOBAR".to_owned(),
        )));

        pretty_assertions::assert_eq!(actual, expected);
    }
}
