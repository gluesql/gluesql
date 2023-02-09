use {
    crate::{
        ast::{ColumnDef, Expr, Statement, ToSql},
        prelude::{parse, translate},
        result::Result,
    },
    chrono::{NaiveDateTime, Utc},
    serde::{Deserialize, Serialize},
    std::{fmt::Debug, iter},
    strum_macros::Display,
    thiserror::Error as ThisError,
};

#[cfg(feature = "index")]
use crate::ast::OrderByExpr;

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
    pub engine: Option<String>,
    pub created: NaiveDateTime,
}

impl Schema {
    pub fn to_ddl(&self) -> String {
        let Schema {
            table_name,
            column_defs,
            indexes,
            engine,
            ..
        } = self;

        let create_table = Statement::CreateTable {
            if_not_exists: false,
            name: table_name.to_owned(),
            columns: column_defs.to_owned(),
            engine: engine.to_owned(),
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

    pub fn from_ddl(ddl: &str) -> Result<Schema> {
        let schema = parse(ddl)?.iter().enumerate().fold(
            Err(SchemaParseError::CannotParseDDL.into()),
            |_schema, (i, parsed)| {
                let translated = translate(parsed)?;
                match i {
                    0 => {
                        let schema: Result<Schema> = match translated {
                            Statement::CreateTable {
                                name,
                                columns,
                                engine,
                                ..
                            } => Ok(Schema {
                                table_name: name,
                                column_defs: columns,
                                indexes: Vec::new(),
                                engine,
                                created: Utc::now().naive_utc(),
                            }),
                            _ => Err(SchemaParseError::CannotParseDDL.into()),
                        };

                        schema
                    }
                    _ => match translated {
                        #[cfg(feature = "index")]
                        Statement::CreateIndex {
                            name,
                            column: OrderByExpr { expr, asc },
                            ..
                        } => {
                            let Schema {
                                table_name,
                                column_defs,
                                indexes,
                                engine,
                                created,
                            } = _schema?;

                            let order = asc
                                .and_then(|bool| bool.then_some(SchemaIndexOrd::Asc))
                                .unwrap_or(SchemaIndexOrd::Both);

                            let index = SchemaIndex {
                                name,
                                expr,
                                order,
                                created,
                            };

                            let indexes = indexes.into_iter().chain(vec![index]).collect();

                            Ok(Schema {
                                table_name,
                                column_defs,
                                indexes,
                                engine,
                                created,
                            })
                        }
                        _ => Err(SchemaParseError::CannotParseDDL.into()),
                    },
                }
            },
        );

        schema
    }
}

#[derive(ThisError, Debug, PartialEq, Serialize)]
pub enum SchemaParseError {
    #[error("cannot parse ddl")]
    CannotParseDDL,
}

#[cfg(test)]
mod tests {
    use {
        super::SchemaParseError,
        crate::{
            ast::{AstLiteral, ColumnDef, ColumnUniqueOption, Expr},
            chrono::Utc,
            data::{Schema, SchemaIndex},
            prelude::DataType,
        },
    };

    fn assert_schema(actual: Schema, expected: Schema) {
        let Schema {
            table_name,
            column_defs,
            indexes,
            engine,
            ..
        } = actual;

        let Schema {
            table_name: table_name_e,
            column_defs: column_defs_e,
            indexes: indexes_e,
            engine: engine_e,
            ..
        } = expected;

        assert_eq!(table_name, table_name_e);
        assert_eq!(column_defs, column_defs_e);
        assert_eq!(engine, engine_e);
        indexes
            .into_iter()
            .zip(indexes_e)
            .for_each(|(actual, expected)| assert_index(actual, expected));
    }

    fn assert_index(actual: SchemaIndex, expected: SchemaIndex) {
        let SchemaIndex {
            name, expr, order, ..
        } = actual;
        let SchemaIndex {
            name: name_e,
            expr: expr_e,
            order: order_e,
            ..
        } = expected;

        assert_eq!(name, name_e);
        assert_eq!(expr, expr_e);
        assert_eq!(order, order_e);
    }

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
            engine: None,
            created: Utc::now().naive_utc(),
        };

        let ddl = "CREATE TABLE User (id INT NOT NULL, name TEXT NULL DEFAULT 'glue');";
        assert_eq!(schema.to_ddl(), ddl);

        let actual = Schema::from_ddl(ddl).unwrap();
        assert_schema(actual, schema);

        let schema = Schema {
            table_name: "Test".to_owned(),
            column_defs: None,
            indexes: Vec::new(),
            engine: None,
            created: Utc::now().naive_utc(),
        };
        let ddl = "CREATE TABLE Test;";
        assert_eq!(schema.to_ddl(), ddl);

        let actual = Schema::from_ddl(ddl).unwrap();
        assert_schema(actual, schema);
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
            engine: None,
            created: Utc::now().naive_utc(),
        };

        let ddl = "CREATE TABLE User (id INT NOT NULL PRIMARY KEY);";
        assert_eq!(schema.to_ddl(), ddl);

        let actual = Schema::from_ddl(ddl).unwrap();
        assert_schema(actual, schema);
    }

    #[test]
    fn invalid_ddl() {
        let invalid_ddl = "DROP TABLE Users";
        let actual = Schema::from_ddl(invalid_ddl);
        assert_eq!(actual, Err(SchemaParseError::CannotParseDDL.into()));
    }

    #[test]
    #[cfg(feature = "index")]
    fn table_with_index() {
        use crate::data::SchemaIndexOrd;

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
            engine: None,
            created: Utc::now().naive_utc(),
        };
        let ddl = "CREATE TABLE User (id INT NOT NULL, name TEXT NOT NULL);
CREATE INDEX User_id ON User (id);
CREATE INDEX User_name ON User (name);";
        assert_eq!(schema.to_ddl(), ddl);

        let actual = Schema::from_ddl(ddl).unwrap();
        assert_schema(actual, schema);

        let index_should_not_be_first = "CREATE INDEX User_id ON User (id);
CREATE TABLE User (id INT NOT NULL, name TEXT NOT NULL);";
        let actual = Schema::from_ddl(index_should_not_be_first);
        assert_eq!(actual, Err(SchemaParseError::CannotParseDDL.into()));
    }
}
