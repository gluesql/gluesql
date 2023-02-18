use std::{borrow::Cow, collections::HashMap};

use gluesql_core::{
    ast::ColumnUniqueOption,
    data::Literal,
    prelude::{Key, Value},
};

use {
    crate::error::CsvStorageError,
    csv::ReaderBuilder,
    gluesql_core::{
        ast::ColumnDef, chrono::NaiveDateTime, data::Schema, prelude::DataType, result::Result,
        store::DataRow,
    },
    std::{
        ffi::OsStr,
        fs::OpenOptions,
        io::prelude::*,
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
    pub fn from_path(path: impl AsRef<Path>) -> Result<Self, CsvStorageError> {
        let column_defs: Vec<ColumnDef> = ReaderBuilder::new()
            .from_path(&path)
            .map_err(CsvStorageError::from_csv_error)?
            .headers()
            .map_err(CsvStorageError::from_csv_error)?
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
    pub fn adapt_schema(self, given_schema: Schema) -> Result<Self, CsvStorageError> {
        if self.schema.table_name != given_schema.table_name {
            return Err(CsvStorageError::SchemaMismatch(
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
                            return Err(CsvStorageError::SchemaMismatch(
                                format!("Csv column name: {}", s_col.name),
                                format!("Schema column name: {}", g_col.name),
                            ));
                        }
                        Ok(s_col.to_owned())
                    })
                    .collect::<Result<Vec<_>, CsvStorageError>>()?;
                Ok(Self {
                    file_path: self.file_path,
                    schema: Schema {
                        column_defs: Some(result),
                        ..self.schema
                    },
                })
            }
            _ => Err(CsvStorageError::SchemaLessNotSupported.into()),
        }
    }

    /// Append row(s) to csv file.
    /// Due to the nature of reading/writing csv file, this operation
    /// might take some time.
    pub fn append_data(&self, rows: Vec<DataRow>) -> Result<()> {
        let rows = rows
            .into_iter()
            .map(|row| match row {
                DataRow::Vec(val_vec) => val_vec
                    .into_iter()
                    .map(|value| Ok(value.into()))
                    .collect::<Result<Vec<String>>>(),
                DataRow::Map(_) => Err(CsvStorageError::SchemaLessNotSupported.into()),
            })
            .collect::<Result<Vec<_>>>()?;

        let rows_string = rows
            .into_iter()
            .map(|row| row.join(","))
            .reduce(|acc, row| format!("{acc}\n{row}"))
            .unwrap_or("".into());

        let mut file = OpenOptions::new()
            .write(true)
            .append(true)
            .open(self.file_path.to_path_buf())
            .map_err(|e| CsvStorageError::FailedToWriteTableFile(e.to_string()))?;

        write!(file, "\n{rows_string}")
            .map_err(|e| CsvStorageError::FailedToWriteTableFile(e.to_string()).into())
    }

    /// Insert row in key position
    pub fn insert_data(&self, rows: Vec<(Key, DataRow)>) -> Result<()> {
        let column_defs = self
            .schema
            .column_defs
            .as_ref()
            .ok_or(CsvStorageError::InvalidKeyType)?;
        // If the table uses primary key, should insert right away
        match column_defs
            .into_iter()
            .enumerate()
            .map_while(|(idx, cd)| match cd.unique {
                Some(ColumnUniqueOption { is_primary: true }) => Some(idx),
                _ => None,
            })
            .next()
        {
            Some(pkey_idx) => {
                let csv_file = std::fs::read_to_string(self.file_path.as_path())
                    .map_err(|e| CsvStorageError::InvalidFileImport(e.to_string()))?;
                let mut data_map = csv_file
                    .split("\n")
                    .skip(1) // skip header
                    .map(|csv_row| -> Result<(Key, String)> {
                        let primary_key = Key::try_from(Value::try_from_literal(
                            &column_defs[pkey_idx].data_type,
                            &Literal::Text(Cow::Borrowed(
                                csv_row
                                    .split(",")
                                    .nth(pkey_idx)
                                    .ok_or(CsvStorageError::ColumnDefMismatch)?,
                            )),
                        )?)?;

                        Ok((primary_key, csv_row.to_string()))
                    })
                    .collect::<Result<HashMap<_, _>>>()?;

                for (key, data_row) in rows {
                    match data_row {
                        DataRow::Vec(row_vec) => {
                            let csv_row = row_vec
                                .into_iter()
                                .map(|val| match val.cast(&DataType::Text) {
                                    Ok(Value::Str(val_text)) => val_text,
                                    _ => "".to_string(),
                                })
                                .collect::<Vec<_>>()
                                .join(",");

                            data_map.insert(key, csv_row);
                        }
                        DataRow::Map(_) => {
                            return Err(CsvStorageError::SchemaLessNotSupported.into())
                        }
                    }
                }

                let csv_rows = data_map
                    .into_iter()
                    .map(|(_, v)| v)
                    .collect::<Vec<_>>()
                    .join("\n");
                let header = csv_file.split(",").next().unwrap_or("");

                std::fs::write(self.file_path.as_path(), format!("{header}\n{csv_rows}"))
                    .map_err(|e| CsvStorageError::FailedToWriteTableFile(e.to_string()).into())
            }
            None => Err(CsvStorageError::InvalidKeyType.into()),
        }
    }

    pub fn delete_data(&self, keys: Vec<Key>) -> Result<()> {
        let column_defs = self
            .schema
            .column_defs
            .as_ref()
            .ok_or(CsvStorageError::InvalidKeyType)?;
        // If the table uses primary key, should insert right away
        match column_defs
            .into_iter()
            .enumerate()
            .map_while(|(idx, cd)| match cd.unique {
                Some(ColumnUniqueOption { is_primary: true }) => Some(idx),
                _ => None,
            })
            .next()
        {
            Some(pkey_idx) => {
                let csv_file = std::fs::read_to_string(self.file_path.as_path())
                    .map_err(|e| CsvStorageError::InvalidFileImport(e.to_string()))?;
                let data_map = csv_file
                    .split("\n")
                    .skip(1) // skip header
                    .map(|csv_row| -> Result<(Key, String)> {
                        let primary_key = Key::try_from(Value::try_from_literal(
                            &column_defs[pkey_idx].data_type,
                            &Literal::Text(Cow::Borrowed(
                                csv_row
                                    .split(",")
                                    .nth(pkey_idx)
                                    .ok_or(CsvStorageError::ColumnDefMismatch)?,
                            )),
                        )?)?;

                        Ok((primary_key, csv_row.to_string()))
                    })
                    .filter_map(|row_result| -> Option<String> {
                        match row_result {
                            Ok((pkey, row)) => {
                                if keys.iter().any(|key| key.eq(&pkey)) {
                                    return None;
                                }

                                Some(row.to_string())
                            }
                            _ => None,
                        }
                    })
                    .collect::<Vec<_>>();

                let csv_rows = data_map.join("\n");
                let header = csv_file.split(",").next().unwrap_or("");

                std::fs::write(self.file_path.as_path(), format!("{header}\n{csv_rows}"))
                    .map_err(|e| CsvStorageError::FailedToWriteTableFile(e.to_string()).into())
            }
            None => Err(CsvStorageError::InvalidKeyType.into()),
        }
    }
}

#[cfg(test)]
mod test {
    use {
        super::*,
        crate::error::CsvStorageError,
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
            Err(CsvStorageError::SchemaMismatch(
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
            Err(CsvStorageError::SchemaMismatch(
                "Csv column name: id".to_string(),
                "Schema column name: identifier".to_string()
            )),
            result
        );
    }
}
