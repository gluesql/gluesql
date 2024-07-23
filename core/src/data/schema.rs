use {
    crate::{
        ast::{ColumnDef, Expr, ForeignKey, OrderByExpr, Statement, ToSql, UniqueConstraint},
        prelude::{parse, translate},
        result::Result,
    },
    chrono::{NaiveDateTime, Utc},
    serde::{Deserialize, Serialize},
    std::{fmt::Debug, iter},
    strum_macros::Display,
    thiserror::Error as ThisError,
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
    pub engine: Option<String>,
    pub foreign_keys: Vec<ForeignKey>,
    pub primary_key: Option<Vec<String>>,
    pub unique_constraints: Vec<UniqueConstraint>,
    pub comment: Option<String>,
}

impl Schema {
    /// Returns whether the schema has a primary key.
    pub fn has_primary_key(&self) -> bool {
        self.primary_key.is_some()
    }

    /// Returns whether the schema has column definitions.
    pub fn has_column_defs(&self) -> bool {
        self.column_defs.is_some()
    }

    /// Returns the names of the columns defined in the schema, if any.
    pub fn get_column_names(&self) -> Option<Vec<String>> {
        self.column_defs.as_ref().map(|column_defs| {
            column_defs
                .iter()
                .map(|column_def| column_def.name.clone())
                .collect()
        })
    }

    /// Returns the indices of the primary key columns.
    ///
    /// # Arguments
    /// * `column_defs` - The column definitions of the table.
    /// * `primary_key` - The primary key of the table.
    pub fn get_primary_key_column_indices(&self) -> Option<Vec<usize>> {
        match (&self.column_defs, &self.primary_key) {
            (Some(column_defs), Some(primary_key)) => Some(
                primary_key
                    .iter()
                    .map(|key| {
                        column_defs
                            .iter()
                            .position(|column_def| column_def.name == *key)
                            .unwrap()
                    })
                    .collect(),
            ),
            _ => None,
        }
    }

    /// Returns whether the provided column is part of the primary key.
    ///
    /// # Arguments
    /// * `column` - The column to check.
    pub fn is_primary_key<S: AsRef<str>>(&self, column: S) -> bool {
        self.primary_key
            .as_ref()
            .map(|primary_key| primary_key.iter().any(|key| key == column.as_ref()))
            .unwrap_or(false)
    }

    /// Returns whether any of the columns in the provided iterator are part of the primary key.
    pub fn has_primary_key_columns<I: IntoIterator<Item = S>, S: AsRef<str>>(
        &self,
        columns: I,
    ) -> bool {
        columns
            .into_iter()
            .any(|column| self.is_primary_key(column))
    }

    pub fn to_ddl(&self) -> String {
        let Schema {
            table_name,
            column_defs,
            indexes,
            engine,
            foreign_keys,
            primary_key,
            unique_constraints,
            comment,
        } = self;

        let create_table = Statement::CreateTable {
            if_not_exists: false,
            name: table_name.to_owned(),
            columns: column_defs.to_owned(),
            engine: engine.to_owned(),
            comment: comment.to_owned(),
            source: None,
            foreign_keys: foreign_keys.to_owned(),
            primary_key: primary_key.to_owned(),
            unique_constraints: unique_constraints.to_owned(),
        }
        .to_sql();

        let create_indexes = indexes.iter().map(|SchemaIndex { name, expr, .. }| {
            let expr = expr.to_sql();

            format!(r#"CREATE INDEX "{name}" ON "{table_name}" ({expr});"#)
        });

        iter::once(create_table)
            .chain(create_indexes)
            .collect::<Vec<_>>()
            .join("\n")
    }

    pub fn from_ddl(ddl: &str) -> Result<Schema> {
        let created = Utc::now().naive_utc();
        let statements = parse(ddl)?;

        let indexes = statements
            .iter()
            .skip(1)
            .map(|create_index| {
                let create_index = translate(create_index)?;
                match create_index {
                    Statement::CreateIndex {
                        name,
                        column: OrderByExpr { expr, asc },
                        ..
                    } => {
                        let order = asc
                            .and_then(|bool| bool.then_some(SchemaIndexOrd::Asc))
                            .unwrap_or(SchemaIndexOrd::Both);

                        let index = SchemaIndex {
                            name,
                            expr,
                            order,
                            created,
                        };

                        Ok(index)
                    }
                    _ => Err(SchemaParseError::CannotParseDDL.into()),
                }
            })
            .collect::<Result<Vec<_>>>()?;

        let create_table = statements.first().ok_or(SchemaParseError::CannotParseDDL)?;
        let create_table = translate(create_table)?;

        match create_table {
            Statement::CreateTable {
                name,
                columns,
                engine,
                foreign_keys,
                primary_key,
                unique_constraints,
                comment,
                ..
            } => Ok(Schema {
                table_name: name,
                column_defs: columns,
                indexes,
                engine,
                foreign_keys,
                primary_key,
                unique_constraints,
                comment,
            }),
            _ => Err(SchemaParseError::CannotParseDDL.into()),
        }
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
            ast::{AstLiteral, ColumnDef, Expr, UniqueConstraint},
            chrono::Utc,
            data::{Schema, SchemaIndex, SchemaIndexOrd},
            prelude::DataType,
        },
    };

    fn assert_schema(actual: Schema, expected: Schema) {
        let Schema {
            table_name,
            column_defs,
            indexes,
            engine,
            foreign_keys,
            primary_key,
            unique_constraints,
            comment,
        } = actual;

        let Schema {
            table_name: table_name_e,
            column_defs: column_defs_e,
            indexes: indexes_e,
            engine: engine_e,
            foreign_keys: foreign_keys_e,
            primary_key: primary_key_e,
            unique_constraints: unique_constraints_e,
            comment: comment_e,
        } = expected;

        assert_eq!(table_name, table_name_e);
        assert_eq!(column_defs, column_defs_e);
        assert_eq!(engine, engine_e);
        assert_eq!(foreign_keys, foreign_keys_e);
        assert_eq!(primary_key, primary_key_e);
        assert_eq!(unique_constraints, unique_constraints_e);
        assert_eq!(comment, comment_e);
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
                    unique: false,
                    comment: None,
                },
                ColumnDef {
                    name: "name".to_owned(),
                    data_type: DataType::Text,
                    nullable: true,
                    default: Some(Expr::Literal(AstLiteral::QuotedString("glue".to_owned()))),
                    unique: false,
                    comment: None,
                },
            ]),
            indexes: Vec::new(),
            engine: None,
            foreign_keys: Vec::new(),
            primary_key: None,
            unique_constraints: Vec::new(),
            comment: None,
        };

        let ddl = r#"CREATE TABLE "User" ("id" INT NOT NULL, "name" TEXT NULL DEFAULT 'glue');"#;
        assert_eq!(schema.to_ddl(), ddl);

        let actual = Schema::from_ddl(ddl).unwrap();
        assert_schema(actual, schema);

        let schema = Schema {
            table_name: "Test".to_owned(),
            column_defs: None,
            indexes: Vec::new(),
            engine: None,
            foreign_keys: Vec::new(),
            primary_key: None,
            unique_constraints: Vec::new(),
            comment: None,
        };
        let ddl = r#"CREATE TABLE "Test";"#;
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
                unique: false,
                comment: None,
            }]),
            indexes: Vec::new(),
            engine: None,
            foreign_keys: Vec::new(),
            primary_key: Some(vec!["id".to_owned()]),
            unique_constraints: Vec::new(),
            comment: None,
        };

        let ddl = r#"CREATE TABLE "User" ("id" INT NOT NULL, PRIMARY KEY (id));"#;
        assert_eq!(schema.to_ddl(), ddl);

        let actual = Schema::from_ddl(ddl).unwrap();
        assert_schema(actual, schema);
    }

    #[test]
    fn table_composite_primary() {
        let schema = Schema {
            table_name: "User".to_owned(),
            column_defs: Some(vec![
                ColumnDef {
                    name: "id".to_owned(),
                    data_type: DataType::Int,
                    nullable: false,
                    default: None,
                    unique: false,
                    comment: None,
                },
                ColumnDef {
                    name: "user_id".to_owned(),
                    data_type: DataType::Int,
                    nullable: false,
                    default: None,
                    unique: false,
                    comment: None,
                },
                ColumnDef {
                    name: "image_id".to_owned(),
                    data_type: DataType::Int,
                    nullable: false,
                    default: None,
                    unique: true,
                    comment: None,
                },
            ]),
            indexes: Vec::new(),
            engine: None,
            foreign_keys: Vec::new(),
            primary_key: Some(vec!["id".to_owned(), "user_id".to_owned()]),
            unique_constraints: Vec::new(),
            comment: None,
        };

        let ddl = r#"CREATE TABLE "User" ("id" INT NOT NULL, "user_id" INT NOT NULL, "image_id" INT NOT NULL UNIQUE, PRIMARY KEY (id, user_id));"#;
        assert_eq!(schema.to_ddl(), ddl);

        let actual = Schema::from_ddl(ddl).unwrap();
        assert_schema(actual, schema);
    }

    #[test]
    fn invalid_ddl() {
        // Only Statement::CreateTable is supported
        let invalid_ddl = r#"DROP TABLE "Users";"#;
        let actual = Schema::from_ddl(invalid_ddl);
        assert_eq!(actual, Err(SchemaParseError::CannotParseDDL.into()));
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
                    unique: false,
                    comment: None,
                },
                ColumnDef {
                    name: "name".to_owned(),
                    data_type: DataType::Text,
                    nullable: false,
                    default: None,
                    unique: false,
                    comment: None,
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
            primary_key: None,
            foreign_keys: Vec::new(),
            unique_constraints: Vec::new(),
            comment: None,
        };
        let ddl = r#"CREATE TABLE "User" ("id" INT NOT NULL, "name" TEXT NOT NULL);
CREATE INDEX "User_id" ON "User" ("id");
CREATE INDEX "User_name" ON "User" ("name");"#;
        assert_eq!(schema.to_ddl(), ddl);

        let actual = Schema::from_ddl(ddl).unwrap();
        assert_schema(actual, schema);

        let index_should_not_be_first = r#"CREATE INDEX "User_id" ON "User" ("id");
CREATE TABLE "User" ("id" INT NOT NULL, "name" TEXT NOT NULL);"#;
        let actual = Schema::from_ddl(index_should_not_be_first);
        assert_eq!(actual, Err(SchemaParseError::CannotParseDDL.into()));
    }

    #[test]
    fn non_word_identifier() {
        let schema = Schema {
            table_name: 1.to_string(),
            column_defs: Some(vec![
                ColumnDef {
                    name: 2.to_string(),
                    data_type: DataType::Int,
                    nullable: true,
                    default: None,
                    unique: false,
                    comment: None,
                },
                ColumnDef {
                    name: ";".to_owned(),
                    data_type: DataType::Int,
                    nullable: true,
                    default: None,
                    unique: false,
                    comment: None,
                },
            ]),
            indexes: vec![SchemaIndex {
                name: ".".to_owned(),
                expr: Expr::Identifier(";".to_owned()),
                order: SchemaIndexOrd::Both,
                created: Utc::now().naive_utc(),
            }],
            engine: None,
            foreign_keys: Vec::new(),
            primary_key: None,
            unique_constraints: Vec::new(),
            comment: None,
        };
        let ddl = r#"CREATE TABLE "1" ("2" INT NULL, ";" INT NULL);
CREATE INDEX "." ON "1" (";");"#;
        assert_eq!(schema.to_ddl(), ddl);

        let actual = Schema::from_ddl(ddl).unwrap();
        assert_schema(actual, schema);
    }

    #[test]
    /// Test schema involving unique constraints.
    fn unique_identifiers() {
        let schema = Schema {
            table_name: "User".to_owned(),
            column_defs: Some(vec![
                ColumnDef {
                    name: "id".to_owned(),
                    data_type: DataType::Int,
                    nullable: false,
                    default: None,
                    unique: false,
                    comment: None,
                },
                ColumnDef {
                    name: "name".to_owned(),
                    data_type: DataType::Text,
                    nullable: false,
                    default: None,
                    unique: false,
                    comment: None,
                },
            ]),
            indexes: Vec::new(),
            engine: None,
            foreign_keys: Vec::new(),
            primary_key: None,
            unique_constraints: vec![
                UniqueConstraint::new(Some("unique_name".to_owned()), vec!["name".to_owned()]),
                UniqueConstraint::new(
                    Some("unique_id_and_name".to_owned()),
                    vec!["id".to_owned(), "name".to_owned()],
                ),
            ],
            comment: None,
        };

        let ddl = r#"CREATE TABLE "User" ("id" INT NOT NULL, "name" TEXT NOT NULL, CONSTRAINT "unique_name" UNIQUE ("name"), CONSTRAINT "unique_id_and_name" UNIQUE ("id", "name"));"#;
        assert_eq!(schema.to_ddl(), ddl);

        let actual = Schema::from_ddl(ddl).unwrap();
        assert_schema(actual, schema);
    }
}
