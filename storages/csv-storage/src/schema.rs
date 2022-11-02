use {
    crate::{error::StorageError, CsvTable},
    serde::Deserialize,
    std::{fs, path::Path},
};

#[derive(PartialEq, Debug, Deserialize)]
pub struct SchemaList {
    tables: Vec<CsvTable>,
}

/// Load table schema list from schema file.
pub fn load_schema_list(file_path: impl AsRef<Path>) -> Result<SchemaList, StorageError> {
    let toml_str = fs::read_to_string(file_path)
        .map_err(|e| StorageError::InvalidSchemaFile(e.to_string()))?;
    let schema_list: SchemaList =
        toml::from_str(&toml_str).map_err(|e| StorageError::InvalidSchemaFile(e.to_string()))?;

    Ok(schema_list)
}

#[cfg(test)]
mod test {
    use {
        super::*,
        crate::CsvTable,
        gluesql_core::{
            ast::{ColumnDef, ColumnOption, ColumnOptionDef},
            prelude::DataType,
        },
        std::{path::PathBuf, str::FromStr},
    };

    #[test]
    fn successfully_load_schema() {
        // Arrange
        let file_path = "./example/schema.toml";
        // Act
        let result = load_schema_list(file_path);
        // Assert
        assert_eq!(
            Ok(SchemaList {
                tables: vec![CsvTable {
                    path: PathBuf::from_str("example/data/users.csv").unwrap(),
                    name: "users".to_string(),
                    columns: vec![
                        ColumnDef {
                            name: "id".to_string(),
                            data_type: DataType::Int128,
                            options: vec![ColumnOptionDef {
                                name: None,
                                option: ColumnOption::NotNull
                            }],
                        },
                        ColumnDef {
                            name: "name".to_string(),
                            data_type: DataType::Text,
                            options: vec![ColumnOptionDef {
                                name: None,
                                option: ColumnOption::NotNull
                            }],
                        },
                        ColumnDef {
                            name: "age".to_string(),
                            data_type: DataType::Uint8,
                            options: vec![],
                        },
                        ColumnDef {
                            name: "role".to_string(),
                            data_type: DataType::Text,
                            options: vec![ColumnOptionDef {
                                name: None,
                                option: ColumnOption::NotNull
                            }],
                        },
                    ]
                }]
            }),
            result
        );
    }
}
