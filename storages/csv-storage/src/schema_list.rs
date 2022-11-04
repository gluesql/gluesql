use {
    crate::error::StorageError,
    gluesql_core::{
        ast::{ColumnDef, ColumnOption, Expr},
        chrono::NaiveDateTime,
        data::Schema,
        prelude::DataType,
    },
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

impl From<TomlColumn> for ColumnDef {
    fn from(column: TomlColumn) -> Self {
        let data_type = match column.data_type {
            Some(dt) => dt,
            None => DataType::Text,
        };
        let options: Vec<ColumnOption> = match column.options {
            Some(opt) => {
                let is_nullable = opt
                    .iter()
                    .find(|o| matches!(o, TomlColumnOption::NotNull | TomlColumnOption::PrimaryKey))
                    .is_none();

                let options_from_toml = opt.into_iter().map(|co| match co {
                    TomlColumnOption::NotNull => ColumnOption::NotNull,
                    TomlColumnOption::PrimaryKey => ColumnOption::Unique { is_primary: true },
                    TomlColumnOption::Unique => ColumnOption::Unique { is_primary: false },
                });

                let options_default = match column.default {
                    Some(value) => options_from_toml.chain(
                        vec![ColumnOption::Default(Expr::TypedString {
                            data_type: data_type.clone(),
                            value,
                        })]
                        .into_iter(),
                    ),
                    None => options_from_toml.chain(vec![].into_iter()),
                };

                if is_nullable {
                    options_default
                        .chain([ColumnOption::Null].into_iter())
                        .collect()
                } else {
                    options_default.collect()
                }
            }
            None => vec![ColumnOption::Null],
        };
        ColumnDef {
            name: column.name,
            data_type,
            options,
        }
    }
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

impl From<TomlTable> for Schema {
    fn from(table: TomlTable) -> Self {
        let column_defs: Vec<ColumnDef> = table.columns.into_iter().map(ColumnDef::from).collect();
        Self {
            table_name: table.name,
            column_defs,
            indexes: vec![],
            created: NaiveDateTime::default(),
        }
    }
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

/// Get schema list from given schema file.
pub fn get_schema_list(file_path: impl AsRef<Path>) -> Result<Vec<Schema>, StorageError> {
    let toml_schema_list = load_schema_list(file_path)?;
    let schema_list: Vec<Schema> = toml_schema_list
        .tables
        .into_iter()
        .map(Schema::from)
        .collect();

    Ok(schema_list)
}

#[cfg(test)]
mod test {
    use {
        super::*,
        gluesql_core::{
            ast::{ColumnDef, ColumnOption, Expr},
            chrono::NaiveDateTime,
            data::Schema,
            prelude::DataType,
        },
        std::{path::PathBuf, str::FromStr},
    };

    #[test]
    fn load_single_schema_from_schema_list() {
        // Arrange
        let file_path = "example/schema_list_single.toml";
        // Act
        let result = load_schema_list(file_path);
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

    #[test]
    fn convert_toml_config_to_gluesql_schema_list() {
        // Arrange
        let file_path = "example/schema_list_single.toml";
        // Act
        let result = get_schema_list(file_path).unwrap();
        // Assert
        assert_eq!(1, result.iter().count());
        let schema = result.get(0).unwrap();
        assert!(matches!(schema, Schema { .. }));
        assert_eq!("users".to_string(), schema.table_name);
        assert_eq!(
            vec![
                ColumnDef {
                    name: "id".to_string(),
                    data_type: DataType::Int128,
                    options: vec![ColumnOption::Unique { is_primary: true }],
                },
                ColumnDef {
                    name: "name".to_string(),
                    data_type: DataType::Text,
                    options: vec![
                        ColumnOption::Unique { is_primary: false },
                        ColumnOption::Null
                    ],
                },
                ColumnDef {
                    name: "age".to_string(),
                    data_type: DataType::Uint8,
                    options: vec![ColumnOption::Null],
                },
                ColumnDef {
                    name: "role".to_string(),
                    data_type: DataType::Text,
                    options: vec![
                        ColumnOption::NotNull,
                        ColumnOption::Default(Expr::TypedString {
                            data_type: DataType::Text,
                            value: "GUEST".to_string()
                        })
                    ],
                },
            ],
            schema.column_defs
        );
        assert_eq!(NaiveDateTime::default(), schema.created);
    }
}
