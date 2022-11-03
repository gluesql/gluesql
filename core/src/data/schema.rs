use {
    crate::ast::{ColumnDef, ColumnOption, ColumnOptionDef, Expr, Statement, ToSql},
    serde::{Deserialize, Serialize},
    std::fmt::Debug,
    strum_macros::Display,
};

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Display)]
#[strum(serialize_all = "SCREAMING_SNAKE_CASE")]
pub enum SchemaIndexOrd {
    Asc,
    Desc,
    Both,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct SchemaIndex {
    pub name: String,
    pub expr: Expr,
    pub order: SchemaIndexOrd,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct Schema {
    pub table_name: String,
    pub column_defs: Vec<ColumnDef>,
    pub indexes: Vec<SchemaIndex>,
}

impl Schema {
    pub fn to_ddl(self) -> String {
        let Schema {
            table_name,
            column_defs: columns,
            indexes,
            ..
        } = self;

        let create_indexes = indexes.iter().fold(
            "".to_owned(),
            |acc,
             SchemaIndex {
                 name, expr, order, ..
             }| {
                let expr = expr.to_sql();
                let table_name = &table_name;

                format!("{acc}CREATE INDEX {name} ON {table_name} ({expr} {order});\n")
            },
        );

        let create_table = Statement::CreateTable {
            if_not_exists: false,
            name: table_name,
            columns,
            source: None,
        }
        .to_sql();

        format!("{create_table}\n{create_indexes}")
            .trim_end()
            .to_owned()
    }
}

pub trait ColumnDefExt {
    fn is_nullable(&self) -> bool;

    fn get_default(&self) -> Option<&Expr>;
}

impl ColumnDefExt for ColumnDef {
    fn is_nullable(&self) -> bool {
        self.options
            .iter()
            .any(|ColumnOptionDef { option, .. }| option == &ColumnOption::Null)
    }

    fn get_default(&self) -> Option<&Expr> {
        self.options
            .iter()
            .find_map(|ColumnOptionDef { option, .. }| match option {
                ColumnOption::Default(expr) => Some(expr),
                _ => None,
            })
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        ast::{
            AstLiteral, ColumnDef,
            ColumnOption::{self, Unique},
            ColumnOptionDef, Expr,
        },
        data::{Schema, SchemaIndex, SchemaIndexOrd},
        prelude::DataType,
    };

    #[test]
    fn table_basic() {
        let schema = Schema {
            table_name: "User".to_owned(),
            column_defs: vec![
                ColumnDef {
                    name: "id".to_owned(),
                    data_type: DataType::Int,
                    options: Vec::new(),
                },
                ColumnDef {
                    name: "name".to_owned(),
                    data_type: DataType::Text,
                    options: vec![
                        ColumnOptionDef {
                            name: None,
                            option: ColumnOption::Null,
                        },
                        ColumnOptionDef {
                            name: None,
                            option: ColumnOption::Default(Expr::Literal(AstLiteral::QuotedString(
                                "glue".to_owned(),
                            ))),
                        },
                    ],
                },
            ],
            indexes: Vec::new(),
        };

        assert_eq!(
            schema.to_ddl(),
            "CREATE TABLE User (id INT NOT NULL, name TEXT NULL DEFAULT \"glue\");"
        )
    }

    #[test]
    fn table_primary() {
        let schema = Schema {
            table_name: "User".to_owned(),
            column_defs: vec![ColumnDef {
                name: "id".to_owned(),
                data_type: DataType::Int,
                options: vec![ColumnOptionDef {
                    name: None,
                    option: Unique { is_primary: true },
                }],
            }],
            indexes: Vec::new(),
        };

        assert_eq!(
            schema.to_ddl(),
            "CREATE TABLE User (id INT NOT NULL PRIMARY KEY);"
        );
    }

    #[test]
    fn table_with_index() {
        let schema = Schema {
            table_name: "User".to_owned(),
            column_defs: vec![
                ColumnDef {
                    name: "id".to_owned(),
                    data_type: DataType::Int,
                    options: Vec::new(),
                },
                ColumnDef {
                    name: "name".to_owned(),
                    data_type: DataType::Text,
                    options: Vec::new(),
                },
            ],
            indexes: vec![
                SchemaIndex {
                    name: "User_id".to_owned(),
                    expr: Expr::Identifier("id".to_owned()),
                    order: SchemaIndexOrd::Both,
                },
                SchemaIndex {
                    name: "User_name".to_owned(),
                    expr: Expr::Identifier("name".to_owned()),
                    order: SchemaIndexOrd::Both,
                },
            ],
        };

        assert_eq!(
            schema.to_ddl(),
            "CREATE TABLE User (id INT NOT NULL, name TEXT NOT NULL);
CREATE INDEX User_id ON User (id BOTH);
CREATE INDEX User_name ON User (name BOTH);"
        );
    }
}
