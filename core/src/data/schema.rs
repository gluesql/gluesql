use {
    crate::ast::{ColumnDef, Expr, Statement, ToSql},
    chrono::NaiveDateTime,
    serde::{Deserialize, Serialize},
    std::{fmt::Debug, iter},
    strum_macros::Display,
};

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq, Display)]
#[strum(serialize_all = "SCREAMING_SNAKE_CASE")]
pub enum SchemaIndexOrd {
    Asc,
    Desc,
    Both,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct SchemaIndex {
    pub name: String,
    pub expr: Expr,
    pub order: SchemaIndexOrd,
    pub created: NaiveDateTime,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Schema {
    pub table_name: String,
    pub column_defs: Option<Vec<ColumnDef>>,
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

        let columns = match columns {
            Some(columns) => columns,
            None => todo!(),
        };

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

#[cfg(test)]
mod tests {
    use crate::{
        ast::{AstLiteral, ColumnDef, ColumnUniqueOption, Expr},
        chrono::Utc,
        data::{Schema, SchemaIndex, SchemaIndexOrd},
        prelude::DataType,
    };

    #[test]
    fn table_basic() {
        let schema = Schema {
            table_name: "User".to_owned(),
            column_defs: Some(vec![
                ColumnDef {
                    name: "id".to_owned(),
                    data_type: DataType::Int,
                    nullable: false,
                    default: None,
                    unique: None,
                },
                ColumnDef {
                    name: "name".to_owned(),
                    data_type: DataType::Text,
                    nullable: true,
                    default: Some(Expr::Literal(AstLiteral::QuotedString("glue".to_owned()))),
                    unique: None,
                },
            ]),
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
            column_defs: Some(vec![ColumnDef {
                name: "id".to_owned(),
                data_type: DataType::Int,
                nullable: false,
                default: None,
                unique: Some(ColumnUniqueOption { is_primary: true }),
            }]),
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
            column_defs: Some(vec![
                ColumnDef {
                    name: "id".to_owned(),
                    data_type: DataType::Int,
                    nullable: false,
                    default: None,
                    unique: None,
                },
                ColumnDef {
                    name: "name".to_owned(),
                    data_type: DataType::Text,
                    nullable: false,
                    default: None,
                    unique: None,
                },
            ]),
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
