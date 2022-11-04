use {
    crate::error::StorageError,
    gluesql_core::prelude::DataType,
    serde::Deserialize,
    std::{
        fs,
        path::{Path, PathBuf},
    },
};

/// Column option parsed from TOML file.
#[derive(PartialEq, Debug, Deserialize)]
pub enum TomlColumnOption {
    NotNull,
    Unique,
    PrimaryKey,
}

/// Column definition parsed from TOML file.
///
/// ### Fields
/// - `name`     : Column name, __mandatory__.   
/// - `data_type`: Data type, _optional_. Default is `Text` type when not specified.  
/// - `default`  : Default value, _optional_.  
/// - `options`  : Column options like `NOT NULL` or `UNIQUE`, _optional_.
#[derive(PartialEq, Debug, Deserialize)]
pub struct TomlColumn {
    pub name: String,
    pub data_type: Option<DataType>,
    pub default: Option<String>,
    pub options: Option<Vec<TomlColumnOption>>,
}

/// Table schema parsed from TOML file.
///
/// ### Fields
/// - `name`   : Table name, __mandatory__.   
/// - `path`   : A path to CSV file, __mandatory__.   
/// - `columns`: Default value, _optional_.
#[derive(PartialEq, Debug, Deserialize)]
pub struct TomlTable {
    name: String,
    path: PathBuf,
    columns: Vec<TomlColumn>,
}

/// Table schema list parsed from TOML file.
#[derive(PartialEq, Debug, Deserialize)]
pub struct TomlSchemaList {
    tables: Vec<TomlTable>,
}

/// Load table schema list from schema file.
pub fn load_schema_list(file_path: impl AsRef<Path>) -> Result<TomlSchemaList, StorageError> {
    let toml_str = fs::read_to_string(file_path)
        .map_err(|e| StorageError::InvalidSchemaFile(e.to_string()))?;
    let schema_list: TomlSchemaList =
        toml::from_str(&toml_str).map_err(|e| StorageError::InvalidSchemaFile(e.to_string()))?;

    Ok(schema_list)
}

#[cfg(test)]
mod test {
    use {
        super::*,
        gluesql_core::prelude::DataType,
        std::{path::PathBuf, str::FromStr},
    };

    #[test]
    fn load_single_schema_from_schema_list() {
        // Arrange
        let schema_list_text = r#"
        [[tables]]
        name = "users"
        path = "example/data/users.csv"
        columns = [
            { name = "id",   data_type = "Int128", options = ["PrimaryKey"] },
            { name = "name", data_type = "Text",   options = ["Unique"]     },
            { name = "age",  data_type = "Uint8"                            },
            { name = "role", default   = "GUEST",  options = ["NotNull"]    },
        ]
        "#;
        let file_path = "schema_list.toml";
        fs::write(file_path, schema_list_text).unwrap();
        // Act
        let result = load_schema_list(file_path);
        fs::remove_file(file_path).unwrap();
        // Assert
        assert_eq!(
            Ok(TomlSchemaList {
                tables: vec![TomlTable {
                    name: "users".to_string(),
                    path: PathBuf::from_str("example/data/users.csv").unwrap(),
                    columns: vec![
                        TomlColumn {
                            name: "id".to_string(),
                            data_type: Some(DataType::Int128),
                            default: None,
                            options: Some(vec![TomlColumnOption::PrimaryKey]),
                        },
                        TomlColumn {
                            name: "name".to_string(),
                            data_type: Some(DataType::Text),
                            default: None,
                            options: Some(vec![TomlColumnOption::Unique]),
                        },
                        TomlColumn {
                            name: "age".to_string(),
                            data_type: Some(DataType::Uint8),
                            default: None,
                            options: None,
                        },
                        TomlColumn {
                            name: "role".to_string(),
                            data_type: None,
                            default: Some("GUEST".to_string()),
                            options: Some(vec![TomlColumnOption::NotNull]),
                        },
                    ]
                }]
            }),
            result
        );
    }
}
