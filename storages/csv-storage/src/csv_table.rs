use {
    crate::error::StorageError,
    csv::ReaderBuilder,
    gluesql_core::{ast::ColumnDef, chrono::NaiveDateTime, data::Schema, prelude::DataType},
    std::{
        ffi::OsStr,
        path::{Path, PathBuf},
    },
};

pub struct CsvTable {
    pub file_path: PathBuf,
    pub schema: Schema,
}

impl CsvTable {
    /// Create csv table from given path.
    /// Columns are defaulted as string type.
    pub fn from_path(path: impl AsRef<Path>) -> Result<Self, StorageError> {
        let file_path = path.as_ref().to_path_buf();

        let mut builder = ReaderBuilder::new()
            .from_path(&path)
            .map_err(StorageError::from_csv_error)?;

        let column_defs: Vec<ColumnDef> = builder
            .headers()
            .map_err(StorageError::from_csv_error)?
            .into_iter()
            .map(|col| ColumnDef {
                name: col.to_owned(),
                data_type: DataType::Text,
                options: vec![],
            })
            .collect();

        let table_name = path
            .as_ref()
            .file_name()
            .and_then(OsStr::to_str)
            .and_then(|filename| filename.split('.').next())
            // TODO: Should increment number for default
            .map_or("new_table_0", |filename| {
                if filename.len() == 0 {
                    return "new_table_0";
                }
                filename
            })
            .to_string();

        let schema = Schema {
            table_name,
            column_defs,
            indexes: vec![],
            created: NaiveDateTime::default(),
        };

        Ok(CsvTable { file_path, schema })
    }

    /// Adapts schema and create new `CsvTable`.    
    ///
    /// Following info should be identical between `schema` and `CsvTable`:
    /// - `table_name`
    /// - `column_def.name` for every item in `column_defs`
    pub fn adapt_schema(self, schema: Schema) -> Result<Self, StorageError> {
        if self.schema.table_name != schema.table_name {
            return Err(StorageError::SchemaMismatch(
                format!("Csv table name: {}", self.schema.table_name),
                format!("Schema table name: {}", schema.table_name),
            ));
        }

        let _ = self
            .schema
            .column_defs
            .iter()
            .zip(schema.column_defs.iter())
            .map(|(csv_col, schema_col)| {
                if csv_col.name != schema_col.name {
                    return Err(StorageError::SchemaMismatch(
                        format!("Csv column name: {}", csv_col.name),
                        format!("Schema column name: {}", schema_col.name),
                    ));
                }
                Ok(schema_col)
            })
            .collect::<Result<Vec<_>, StorageError>>()?;

        Ok(CsvTable { schema, ..self })
    }
}

#[cfg(test)]
mod test {
    use {
        super::*,
        crate::error::StorageError,
        gluesql_core::{ast::ColumnDef, chrono::NaiveDateTime, data::Schema, prelude::DataType},
        std::{fs, path::PathBuf, str::FromStr},
    };

    #[test]
    fn create_table_from_path() {
        // Arrange
        let csv_path = "users.csv";
        let csv_contents = "id,name,age\n1,John,23\n2,Patrick,30";
        fs::write(csv_path, csv_contents).unwrap();
        // Act
        let result = CsvTable::from_path(csv_path);
        // Assert
        assert!(matches!(result, Ok(CsvTable { .. })));
        let CsvTable { file_path, schema } = result.unwrap();
        assert_eq!(PathBuf::from_str("users.csv").unwrap(), file_path);
        assert_eq!("users".to_string(), schema.table_name);
        assert_eq!(
            vec![
                ColumnDef {
                    name: "id".to_owned(),
                    data_type: DataType::Text,
                    options: vec![]
                },
                ColumnDef {
                    name: "name".to_owned(),
                    data_type: DataType::Text,
                    options: vec![]
                },
                ColumnDef {
                    name: "age".to_owned(),
                    data_type: DataType::Text,
                    options: vec![]
                },
            ],
            schema.column_defs
        );
        assert_eq!(
            (vec![], NaiveDateTime::default()),
            (schema.indexes, schema.created)
        );
        // Should cleanup created csv file
        fs::remove_file(csv_path).unwrap();
    }

    #[test]
    fn give_default_table_name_for_empty_filename() {
        // Arrange
        let csv_path = ".csv";
        let csv_contents = "id,name,age\n1,John,23\n2,Patrick,30";
        fs::write(csv_path, csv_contents).unwrap();
        // Act
        let result = CsvTable::from_path(csv_path);
        // Assert
        assert!(matches!(result, Ok(CsvTable { .. })));
        let CsvTable { file_path, schema } = result.unwrap();
        assert_eq!(PathBuf::from_str("new_table_0.csv").unwrap(), file_path);
        assert_eq!("users".to_string(), schema.table_name);
        assert_eq!(
            vec![
                ColumnDef {
                    name: "id".to_owned(),
                    data_type: DataType::Text,
                    options: vec![]
                },
                ColumnDef {
                    name: "name".to_owned(),
                    data_type: DataType::Text,
                    options: vec![]
                },
                ColumnDef {
                    name: "age".to_owned(),
                    data_type: DataType::Text,
                    options: vec![]
                },
            ],
            schema.column_defs
        );
        assert_eq!(
            (vec![], NaiveDateTime::default()),
            (schema.indexes, schema.created)
        );

        // Should cleanup created csv file
        fs::remove_file(csv_path).unwrap();
    }

    fn generate_csv_table() -> CsvTable {
        CsvTable {
            file_path: PathBuf::from_str("users.csv").unwrap(),
            schema: Schema {
                table_name: "users".to_string(),
                column_defs: vec![
                    ColumnDef {
                        name: "id".to_owned(),
                        data_type: DataType::Text,
                        options: vec![],
                    },
                    ColumnDef {
                        name: "name".to_owned(),
                        data_type: DataType::Text,
                        options: vec![],
                    },
                    ColumnDef {
                        name: "age".to_owned(),
                        data_type: DataType::Text,
                        options: vec![],
                    },
                ],
                indexes: vec![],
                created: NaiveDateTime::default(),
            },
        }
    }

    #[test]
    fn converts_column_defs_with_given_schema() {
        // Arrange
        let csv_table = generate_csv_table();
        let schema = Schema {
            table_name: "users".to_string(),
            column_defs: vec![
                ColumnDef {
                    name: "id".to_owned(),
                    data_type: DataType::Int128,
                    options: vec![],
                },
                ColumnDef {
                    name: "name".to_owned(),
                    data_type: DataType::Text,
                    options: vec![],
                },
                ColumnDef {
                    name: "age".to_owned(),
                    data_type: DataType::Uint8,
                    options: vec![],
                },
            ],
            indexes: vec![],
            created: NaiveDateTime::default(),
        };
        // Act
        let result = csv_table.adapt_schema(schema);
        // Assert
        assert!(matches!(result, Ok(CsvTable { .. })));
        let result = result.unwrap();
        assert_eq!(PathBuf::from_str("users.csv").unwrap(), result.file_path);
        let schema = result.schema;
        assert_eq!("users".to_string(), schema.table_name);
        assert_eq!(
            vec![
                ColumnDef {
                    name: "id".to_owned(),
                    data_type: DataType::Int128,
                    options: vec![],
                },
                ColumnDef {
                    name: "name".to_owned(),
                    data_type: DataType::Text,
                    options: vec![],
                },
                ColumnDef {
                    name: "age".to_owned(),
                    data_type: DataType::Uint8,
                    options: vec![],
                },
            ],
            schema.column_defs
        );
        assert_eq!(
            (vec![], NaiveDateTime::default()),
            (schema.indexes, schema.created)
        );
    }

    #[test]
    fn fails_when_table_names_are_different() {
        // Arrange
        let csv_table = generate_csv_table();
        let schema = Schema {
            table_name: "animals".to_string(),
            column_defs: vec![],
            indexes: vec![],
            created: NaiveDateTime::default(),
        };
        // Act
        let result = csv_table.adapt_schema(schema);
        // Assert
        // TODO: Test
        // assert_eq!(
        //     Err(StorageError::SchemaMismatch(
        //         "Csv table name: users".to_string(),
        //         "Schema table name: animals".to_string()
        //     )),
        //     result
        // );
    }

    #[test]
    fn fails_when_column_names_are_different() {
        // Arrange
        let csv_table = generate_csv_table();
        let schema = Schema {
            table_name: "users".to_string(),
            column_defs: vec![ColumnDef {
                name: "identifier".to_owned(),
                data_type: DataType::Int128,
                options: vec![],
            }],
            indexes: vec![],
            created: NaiveDateTime::default(),
        };
        // Act
        let result = csv_table.adapt_schema(schema);
        // Assert
        // TODO: Test
        // assert_eq!(
        //     Err(StorageError::SchemaMismatch(
        //         "Csv column name: id".to_string(),
        //         "Schema column name: identifier".to_string()
        //     )),
        //     result
        // );
    }
}
