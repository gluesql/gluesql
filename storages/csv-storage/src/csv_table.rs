use gluesql_core::{result::Result, store::DataRow};

use {
    crate::error::StorageError,
    csv::ReaderBuilder,
    gluesql_core::{ast::ColumnDef, chrono::NaiveDateTime, data::Schema, prelude::DataType},
    std::{
        ffi::OsStr,
        path::{Path, PathBuf},
    },
};

#[derive(Debug, PartialEq)]
pub struct CsvTable {
    pub file_path: PathBuf,
    pub schema: Schema,
}

impl CsvTable {
    /// Create csv table from given path.
    /// Columns are defaulted as string type.
    pub fn from_path(path: impl AsRef<Path>) -> Result<Self, StorageError> {
        let column_defs: Vec<ColumnDef> = ReaderBuilder::new()
            .from_path(&path)
            .map_err(StorageError::from_csv_error)?
            .headers()
            .map_err(StorageError::from_csv_error)?
            .into_iter()
            .map(|col| ColumnDef {
                name: col.to_owned(),
                data_type: DataType::Text,
                nullable: true,
                default: None,
                unique: None,
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

        Ok(CsvTable {
            file_path: path.as_ref().to_path_buf(),
            schema: Schema {
                table_name,
                column_defs: Some(column_defs),
                indexes: vec![],
                created: NaiveDateTime::default(),
                engine: None,
            },
        })
    }

    /// Adapts schema and create new `CsvTable`.    
    ///
    /// Following info should be identical between `schema` and `CsvTable`:
    /// - `table_name`
    /// - `column_def.name` for every item in `column_defs`
    pub fn adapt_schema(self, given_schema: Schema) -> Result<Self, StorageError> {
        if self.schema.table_name != given_schema.table_name {
            return Err(StorageError::SchemaMismatch(
                format!("Csv table name: {}", self.schema.table_name),
                format!("Schema table name: {}", given_schema.table_name),
            ));
        }

        match (self.schema.column_defs, given_schema.column_defs) {
            (Some(self_cds), Some(given_cds)) => {
                let result = self_cds
                    .iter()
                    .zip(given_cds.iter())
                    .map(|(s_col, g_col)| {
                        if s_col.name != g_col.name {
                            return Err(StorageError::SchemaMismatch(
                                format!("Csv column name: {}", s_col.name),
                                format!("Schema column name: {}", g_col.name),
                            ));
                        }
                        Ok(s_col.to_owned())
                    })
                    .collect::<Result<Vec<_>, StorageError>>()?;
                Ok(Self {
                    file_path: self.file_path,
                    schema: Schema {
                        column_defs: Some(result),
                        ..self.schema
                    },
                })
            }
            _ => Err(StorageError::SchemaLessNotSupported.into()),
        }
    }

    /// Append row(s) to csv file.
    /// Due to the nature of reading/writing csv file, this operation
    /// might take some time.
    pub fn append_data(&self, rows: Vec<DataRow>) -> Result<()> {
        let column_defs = self
            .schema
            .column_defs
            .as_ref()
            .ok_or(StorageError::SchemaLessNotSupported)?
            .into_iter()
            .map(|cd| cd.data_type.to_owned());

        let rows = rows
            .into_iter()
            .map(|row| {
                match row {
                    DataRow::Vec(val_vec) => {
                        // The length of val_vec should equal to column_defs
                        if val_vec.len() != column_defs.len() {
                            return Err(StorageError::ColumnDefMismatch.into());
                        }
                        val_vec
                            .into_iter()
                            .zip(column_defs.clone())
                            .map(|(val, dt)| -> Result<String> {
                                val.validate_type(&dt)
                                    .map_err(|_| StorageError::ColumnDefMismatch)?;

                                Ok(val.into())
                            })
                            .collect::<Result<Vec<_>>>()
                    }
                    DataRow::Map(_) => Err(StorageError::SchemaLessNotSupported.into()),
                }
            })
            .collect::<Result<Vec<_>>>()?;

        let rows_string = rows
            .into_iter()
            .map(|row| row.join(","))
            .reduce(|acc, row| format!("{acc}\n{row}"))
            .unwrap_or("".into());

        let original_data = std::fs::read_to_string(self.file_path.to_path_buf())
            .map_err(|e| StorageError::FailedToProcessCsv(e.to_string()))?;

        std::fs::write(
            self.file_path.to_path_buf(),
            format!("{original_data}\n{rows_string}"),
        )
        .map_err(|e| StorageError::FailedToAppendData(e.to_string()).into())
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
            Some(vec![
                ColumnDef {
                    name: "id".to_owned(),
                    data_type: DataType::Text,
                    nullable: true,
                    unique: None,
                    default: None
                },
                ColumnDef {
                    name: "name".to_owned(),
                    data_type: DataType::Text,
                    nullable: true,
                    unique: None,
                    default: None
                },
                ColumnDef {
                    name: "age".to_owned(),
                    data_type: DataType::Text,
                    nullable: true,
                    unique: None,
                    default: None
                },
            ]),
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
        assert_eq!(PathBuf::from_str(".csv").unwrap(), file_path);
        assert_eq!("new_table_0".to_string(), schema.table_name);
        // Should cleanup created csv file
        fs::remove_file(csv_path).unwrap();
    }

    fn generate_csv_table() -> CsvTable {
        CsvTable {
            file_path: PathBuf::from_str("users.csv").unwrap(),
            schema: Schema {
                table_name: "users".to_string(),
                column_defs: Some(vec![
                    ColumnDef {
                        name: "id".to_owned(),
                        data_type: DataType::Text,
                        nullable: true,
                        unique: None,
                        default: None,
                    },
                    ColumnDef {
                        name: "name".to_owned(),
                        data_type: DataType::Text,
                        nullable: true,
                        unique: None,
                        default: None,
                    },
                    ColumnDef {
                        name: "age".to_owned(),
                        data_type: DataType::Text,
                        nullable: true,
                        unique: None,
                        default: None,
                    },
                ]),
                indexes: vec![],
                created: NaiveDateTime::default(),
                engine: None,
            },
        }
    }

    #[test]
    fn converts_column_defs_with_given_schema() {
        // Arrange
        let csv_table = generate_csv_table();
        let schema = Schema {
            table_name: "users".to_string(),
            column_defs: Some(vec![
                ColumnDef {
                    name: "id".to_owned(),
                    data_type: DataType::Int128,
                    nullable: true,
                    unique: None,
                    default: None,
                },
                ColumnDef {
                    name: "name".to_owned(),
                    data_type: DataType::Text,
                    nullable: true,
                    unique: None,
                    default: None,
                },
                ColumnDef {
                    name: "age".to_owned(),
                    data_type: DataType::Uint8,
                    nullable: true,
                    unique: None,
                    default: None,
                },
            ]),
            indexes: vec![],
            created: NaiveDateTime::default(),
            engine: None,
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
            Some(vec![
                ColumnDef {
                    name: "id".to_owned(),
                    data_type: DataType::Int128,
                    nullable: true,
                    unique: None,
                    default: None,
                },
                ColumnDef {
                    name: "name".to_owned(),
                    data_type: DataType::Text,
                    nullable: true,
                    unique: None,
                    default: None,
                },
                ColumnDef {
                    name: "age".to_owned(),
                    data_type: DataType::Uint8,
                    nullable: true,
                    unique: None,
                    default: None,
                },
            ]),
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
            column_defs: Some(vec![]),
            indexes: vec![],
            created: NaiveDateTime::default(),
            engine: None,
        };
        // Act
        let result = csv_table.adapt_schema(schema);
        // Assert
        assert_eq!(
            Err(StorageError::SchemaMismatch(
                "Csv table name: users".to_string(),
                "Schema table name: animals".to_string()
            )),
            result
        );
    }

    #[test]
    fn fails_when_column_names_are_different() {
        // Arrange
        let csv_table = generate_csv_table();
        let schema = Schema {
            table_name: "users".to_string(),
            column_defs: Some(vec![ColumnDef {
                name: "identifier".to_owned(),
                data_type: DataType::Int128,
                nullable: true,
                unique: None,
                default: None,
            }]),
            indexes: vec![],
            created: NaiveDateTime::default(),
            engine: None,
        };
        // Act
        let result = csv_table.adapt_schema(schema);
        // Assert
        assert_eq!(
            Err(StorageError::SchemaMismatch(
                "Csv column name: id".to_string(),
                "Schema column name: identifier".to_string()
            )),
            result
        );
    }
}
