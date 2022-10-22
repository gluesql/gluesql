mod error;

use {
    csv::ReaderBuilder,
    error::StorageError,
    gluesql_core::{ast::ColumnDef, data::Schema, prelude::DataType},
    std::{
        ffi::OsStr,
        path::{Path, PathBuf},
    },
};

#[derive(Debug, PartialEq)]
pub struct CsvTable {
    file_path: PathBuf,
    table_name: String,
    column_defs: Vec<ColumnDef>,
}

impl CsvTable {
    /// Create csv table from given path.
    /// Columns are defaulted as string type.
    pub fn from_path(path: impl AsRef<Path>) -> std::result::Result<Self, StorageError> {
        let column_defs: Vec<ColumnDef> = ReaderBuilder::new()
            .from_path(&path)
            .map_err(StorageError::from_csv_error)?
            .headers()
            .map_err(StorageError::from_csv_error)?
            .into_iter()
            .map(|col| ColumnDef {
                name: col.to_owned(),
                data_type: DataType::Text,
                options: vec![],
            })
            .collect();

        let file_path = path.as_ref().to_path_buf();

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
            file_path,
            table_name,
            column_defs,
        })
    }

    /// Adapts schema and create new `CsvTable`.    
    ///
    /// Following info should be identical between `schema` and `CsvTable`:
    /// - `table_name`
    /// - `column_def.name` for every item in `column_defs`
    pub fn adapt_schema(self, schema: Schema) -> Result<Self, StorageError> {
        if self.table_name != schema.table_name {
            return Err(StorageError::SchemaMismatch(
                format!("Csv table name: {}", self.table_name),
                format!("Schema table name: {}", schema.table_name),
            ));
        }

        let column_defs = self
            .column_defs
            .into_iter()
            .zip(schema.column_defs)
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

        Ok(CsvTable {
            column_defs,
            ..self
        })
    }
}

#[cfg(test)]
mod test {
    use {
        crate::CsvTable,
        gluesql_core::{ast::ColumnDef, data::Schema, prelude::DataType},
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
        assert_eq!(
            Ok(CsvTable {
                file_path: PathBuf::from_str("users.csv").unwrap(),
                table_name: "users".to_string(),
                column_defs: vec![
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
            }),
            result,
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
        assert_eq!(
            Ok(CsvTable {
                file_path: PathBuf::from_str(".csv").unwrap(),
                table_name: "new_table_0".to_string(),
                column_defs: vec![
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
            }),
            result,
        );

        // Should cleanup created csv file
        fs::remove_file(csv_path).unwrap();
    }

    #[test]
    fn converts_column_defs_with_given_schema() {
        // Arrange
        let csv_table = CsvTable {
            file_path: PathBuf::from_str("users.csv").unwrap(),
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
        };

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
        };

        // Act
        let result = csv_table.adapt_schema(schema);

        // Assert
        assert_eq!(
            result,
            Ok(CsvTable {
                file_path: PathBuf::from_str("users.csv").unwrap(),
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
            })
        )
    }
}
