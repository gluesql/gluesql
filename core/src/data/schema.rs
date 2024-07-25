use itertools::Itertools;
use {
    super::Value,
    crate::{
        ast::{ColumnDef, Expr, ForeignKey, OrderByExpr, Statement, ToSql, UniqueConstraint},
        prelude::{parse, translate, Key},
        result::Result,
    },
    chrono::{NaiveDateTime, Utc},
    serde::{Deserialize, Serialize},
    std::{fmt::Debug, iter},
    strum_macros::Display,
    thiserror::Error as ThisError,
};

#[derive(ThisError, Clone, Debug, PartialEq, Deserialize, Serialize, Eq)]
pub enum SchemaError {
    #[error("no primary key defined")]
    NoPrimaryKeyDefined,

    #[error("incompatible row length: {0}")]
    IncompatibleRowLength(usize),
}

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
    pub primary_key: Option<Vec<usize>>,
    pub unique_constraints: Vec<UniqueConstraint>,
    pub comment: Option<String>,
}

impl Schema {
    /// Returns the number of columns in the schema.
    pub fn number_of_columns(&self) -> usize {
        self.column_defs
            .as_ref()
            .map(|column_defs| column_defs.len())
            .unwrap_or(0)
    }

    /// Returns the key associates to the provided row.
    ///
    /// # Arguments
    /// * `row` - The row to get the key for.
    pub fn get_primary_key<R: AsRef<[Value]>>(&self, row: R) -> Result<Key> {
        if row.as_ref().len() != self.number_of_columns() {
            return Err(SchemaError::IncompatibleRowLength(row.as_ref().len()).into());
        }

        match self.primary_key {
            Some(ref primary_key) => {
                let mut key: Vec<Key> = Vec::with_capacity(primary_key.len());

                for &index in primary_key.iter() {
                    key.push(Key::try_from(row.as_ref()[index].clone())?);
                }

                Ok(if key.len() == 1 {
                    key.pop().unwrap()
                } else {
                    Key::List(key)
                })
            }
            None => Err(SchemaError::NoPrimaryKeyDefined.into()),
        }
    }

    /// Returns whether the provided column name is part of a unique constraint.
    pub fn is_part_of_unique_constraint<S: AsRef<str>>(&self, column: S) -> bool {
        self.unique_constraints.iter().any(|constraint| {
            constraint
                .column_indices()
                .contains(&self.get_column_index(column.as_ref()).unwrap())
        })
    }

    /// Returns whether the provided column name is part of a single-field unique constraint.
    pub fn is_part_of_single_field_unique_constraint<S: AsRef<str>>(&self, column: S) -> bool {
        self.unique_constraints.iter().any(|constraint| {
            constraint.is_single_field()
                && constraint
                    .column_indices()
                    .contains(&self.get_column_index(column.as_ref()).unwrap())
        })
    }

    /// Returns an iterator over the ColumnDef instances in the schema that compose the primary key.
    pub fn primary_key_columns(&self) -> Option<impl Iterator<Item = &ColumnDef>> {
        self.primary_key.as_ref().map(|primary_key| {
            primary_key.iter().filter_map(move |index| {
                self.column_defs
                    .as_ref()
                    .and_then(|column_defs| column_defs.get(*index))
            })
        })
    }

    /// Returns an iterator over the unique constraint column indices and names.
    pub fn unique_constraint_columns_and_indices(
        &self,
    ) -> impl Iterator<Item = (&[usize], Vec<&str>)> {
        self.unique_constraints.iter().map(move |constraint| {
            let indices = constraint.column_indices();
            let names = indices
                .iter()
                .filter_map(move |index| {
                    self.column_defs
                        .as_ref()
                        .and_then(|column_defs| column_defs.get(*index))
                        .map(|column_def| column_def.name.as_str())
                })
                .collect();

            (indices, names)
        })
    }

    /// Returns an iterator over the unique constraint column names.
    pub fn unique_constraint_column_names(&self) -> impl Iterator<Item = &str> {
        self.unique_constraints
            .iter()
            .flat_map(move |constraint| {
                constraint.column_indices().iter().filter_map(move |index| {
                    self.column_defs
                        .as_ref()
                        .and_then(|column_defs| column_defs.get(*index))
                        .map(|column_def| column_def.name.as_str())
                })
            })
            .unique()
    }

    /// Returns an iterator over the ColumnDef instances in the schema that compose the primary key.
    pub fn primary_key_column_names(&self) -> Option<impl Iterator<Item = &str>> {
        self.primary_key_columns()
            .map(|columns| columns.map(|column| column.name.as_str()))
    }

    /// Returns whether the schema has a given column.
    pub fn has_column<S: AsRef<str>>(&self, column: S) -> bool {
        self.column_defs
            .as_ref()
            .map(|column_defs| {
                column_defs
                    .iter()
                    .any(|column_def| column_def.name == column.as_ref())
            })
            .unwrap_or(false)
    }

    /// Returns reference to the column definition for the given column name.
    ///
    /// # Arguments
    /// * `column` - The column name to look up.
    pub fn get_column_def<S: AsRef<str>>(&self, column: S) -> Option<&ColumnDef> {
        self.column_defs.as_ref().and_then(|column_defs| {
            column_defs
                .iter()
                .find(|column_def| column_def.name == column.as_ref())
        })
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

    /// Returns whether the provided column is part of the primary key.
    ///
    /// # Arguments
    /// * `column` - The column to check.
    pub fn is_primary_key<S: AsRef<str>>(&self, column: S) -> bool {
        self.primary_key_columns()
            .map(|mut columns| columns.any(|column_def| column_def.name == column.as_ref()))
            .unwrap_or(false)
    }

    /// Returns the index of the given column.
    pub fn get_column_index<S: AsRef<str>>(&self, column: S) -> Option<usize> {
        self.column_defs.as_ref().and_then(|column_defs| {
            column_defs
                .iter()
                .position(|column_def| column_def.name == column.as_ref())
        })
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
            data::{schema::SchemaError, Schema, SchemaIndex, SchemaIndexOrd, Value},
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
                    comment: None,
                },
                ColumnDef {
                    name: "name".to_owned(),
                    data_type: DataType::Text,
                    nullable: true,
                    default: Some(Expr::Literal(AstLiteral::QuotedString("glue".to_owned()))),
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
                comment: None,
            }]),
            indexes: Vec::new(),
            engine: None,
            foreign_keys: Vec::new(),
            primary_key: Some(vec![0]),
            unique_constraints: Vec::new(),
            comment: None,
        };

        let ddl = r#"CREATE TABLE "User" ("id" INT NOT NULL, PRIMARY KEY ("id"));"#;
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
                    comment: None,
                },
                ColumnDef {
                    name: "user_id".to_owned(),
                    data_type: DataType::Int,
                    nullable: false,
                    default: None,
                    comment: None,
                },
                ColumnDef {
                    name: "image_id".to_owned(),
                    data_type: DataType::Int,
                    nullable: false,
                    default: None,
                    comment: None,
                },
            ]),
            indexes: Vec::new(),
            engine: None,
            foreign_keys: Vec::new(),
            primary_key: Some(vec![0, 1]),
            unique_constraints: vec![UniqueConstraint::new(None, vec![2])],
            comment: None,
        };

        let ddl = r#"CREATE TABLE "User" ("id" INT NOT NULL, "user_id" INT NOT NULL, "image_id" INT NOT NULL, UNIQUE ("image_id"), PRIMARY KEY ("id", "user_id"));"#;
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
                    comment: None,
                },
                ColumnDef {
                    name: "name".to_owned(),
                    data_type: DataType::Text,
                    nullable: false,
                    default: None,
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
                    comment: None,
                },
                ColumnDef {
                    name: ";".to_owned(),
                    data_type: DataType::Int,
                    nullable: true,
                    default: None,
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
    fn test_primary_key_short_row() {
        let schema = Schema {
            table_name: "User".to_owned(),
            column_defs: Some(vec![
                ColumnDef {
                    name: "id".to_owned(),
                    data_type: DataType::Int,
                    nullable: false,
                    default: None,
                    comment: None,
                },
                ColumnDef {
                    name: "name".to_owned(),
                    data_type: DataType::Text,
                    nullable: false,
                    default: None,
                    comment: None,
                },
            ]),
            indexes: Vec::new(),
            engine: None,
            foreign_keys: Vec::new(),
            primary_key: Some(vec![0, 1]),
            unique_constraints: vec![],
            comment: None,
        };

        let error = SchemaError::IncompatibleRowLength(1);
        let actual = schema.get_primary_key(vec![Value::U8(1)]);

        assert_eq!(actual, Err(error.into()));
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
                    comment: None,
                },
                ColumnDef {
                    name: "name".to_owned(),
                    data_type: DataType::Text,
                    nullable: false,
                    default: None,
                    comment: None,
                },
            ]),
            indexes: Vec::new(),
            engine: None,
            foreign_keys: Vec::new(),
            primary_key: None,
            unique_constraints: vec![
                UniqueConstraint::new(Some("unique_name".to_owned()), vec![1]),
                UniqueConstraint::new(Some("unique_id_and_name".to_owned()), vec![0, 1]),
            ],
            comment: None,
        };

        let ddl = r#"CREATE TABLE "User" ("id" INT NOT NULL, "name" TEXT NOT NULL, CONSTRAINT "unique_name" UNIQUE ("name"), CONSTRAINT "unique_id_and_name" UNIQUE ("id", "name"));"#;
        assert_eq!(schema.to_ddl(), ddl);

        let actual = Schema::from_ddl(ddl).unwrap();
        assert_schema(actual, schema);
    }

    #[test]
    fn test_primary_key_no_primary_key() {
        let schema = Schema {
            table_name: "User".to_owned(),
            column_defs: Some(vec![
                ColumnDef {
                    name: "id".to_owned(),
                    data_type: DataType::Int,
                    nullable: false,
                    default: None,
                    comment: None,
                },
                ColumnDef {
                    name: "name".to_owned(),
                    data_type: DataType::Text,
                    nullable: false,
                    default: None,
                    comment: None,
                },
            ]),
            indexes: Vec::new(),
            engine: None,
            primary_key: None,
            foreign_keys: Vec::new(),
            unique_constraints: Vec::new(),
            comment: None,
        };

        let error = SchemaError::NoPrimaryKeyDefined;
        let actual = schema.get_primary_key(vec![Value::U8(1), Value::U8(2)]);

        assert_eq!(actual, Err(error.into()));
    }
}
