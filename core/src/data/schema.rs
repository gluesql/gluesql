use {
    crate::ast::{ColumnDef, ColumnOption, Expr, Statement, ToSql},
    chrono::NaiveDateTime,
    serde::{Deserialize, Serialize},
    std::{fmt::Debug, iter},
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
    pub created: NaiveDateTime,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct Schema {
    pub table_name: String,
    pub column_defs: Vec<ColumnDef>,
    pub indexes: Vec<SchemaIndex>,
    pub created: NaiveDateTime,
}

impl Schema {
    pub fn to_ddl(self) -> String {
        let Schema {
            table_name,
            column_defs: columns,
            indexes,
            ..
        } = self;

        let create_table = Statement::CreateTable {
            if_not_exists: false,
            name: table_name.clone(),
            columns,
            source: None,
        }
        .to_sql();

        let create_indexes = indexes.iter().map(|SchemaIndex { name, expr, .. }| {
            let expr = expr.to_sql();
            let table_name = &table_name;

            format!("CREATE INDEX {name} ON {table_name} ({expr});")
        });

        iter::once(create_table)
            .chain(create_indexes)
            .collect::<Vec<_>>()
            .join("\n")
    }
}

impl ColumnDef {
    pub fn get_default(&self) -> Option<&Expr> {
        self.options.iter().find_map(|option| match option {
            ColumnOption::Default(expr) => Some(expr),
            _ => None,
        })
    }
}

#[cfg(test)]
mod tests {

    use crate::{
        ast::{AstLiteral, ColumnDef, ColumnOption, Expr},
        chrono::Utc,
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
                    nullable: false,
                    options: Vec::new(),
                },
                ColumnDef {
                    name: "name".to_owned(),
                    data_type: DataType::Text,
                    nullable: true,
                    options: vec![ColumnOption::Default(Expr::Literal(
                        AstLiteral::QuotedString("glue".to_owned()),
                    ))],
                },
            ],
            indexes: Vec::new(),
            created: Utc::now().naive_utc(),
        };

        assert_eq!(
            schema.to_ddl(),
            "CREATE TABLE User (id INT NOT NULL, name TEXT NULL DEFAULT 'glue');"
        )
    }

    #[test]
    fn table_primary() {
        let schema = Schema {
            table_name: "User".to_owned(),
            column_defs: vec![ColumnDef {
                name: "id".to_owned(),
                data_type: DataType::Int,
                nullable: false,
                options: vec![ColumnOption::Unique { is_primary: true }],
            }],
            indexes: Vec::new(),
            created: Utc::now().naive_utc(),
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
                    nullable: false,
                    options: Vec::new(),
                },
                ColumnDef {
                    name: "name".to_owned(),
                    data_type: DataType::Text,
                    nullable: false,
                    options: Vec::new(),
                },
            ],
            indexes: vec![
                SchemaIndex {
                    name: "User_id".to_owned(),
                    expr: Expr::Identifier("id".to_owned()),
                    order: SchemaIndexOrd::Both,
                    created: Utc::now().naive_utc(),
                },
                SchemaIndex {
                    name: "User_name".to_owned(),
                    expr: Expr::Identifier("name".to_owned()),
                    order: SchemaIndexOrd::Both,
                    created: Utc::now().naive_utc(),
                },
            ],
            created: Utc::now().naive_utc(),
        };

        assert_eq!(
            schema.to_ddl(),
            "CREATE TABLE User (id INT NOT NULL, name TEXT NOT NULL);
CREATE INDEX User_id ON User (id);
CREATE INDEX User_name ON User (name);"
        );
    }
}
