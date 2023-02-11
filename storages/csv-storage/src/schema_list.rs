use {
    crate::error::StorageError,
    gluesql_core::{
        ast::{ColumnDef, ColumnUniqueOption, Expr},
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
pub enum TomlUniqueOption {
    Unique,
    PrimaryKey,
}

/// Column definition parsed from TOML file.
///
/// ### Fields
/// - `name`     : Column name, __mandatory__.   
/// - `data_type`: Data type, _optional_. Default is `Text` type when not specified.  
/// - `nullable` : Flag to consider the column to be nullable. Default is true.  
/// - `default`  : Default value, _optional_.  
/// - `options`  : Column options like `NOT NULL` or `UNIQUE`, _optional_.
#[derive(PartialEq, Debug, Deserialize)]
pub struct TomlColumn {
    pub name: String,
    pub data_type: Option<DataType>,
    pub nullable: Option<bool>,
    pub default: Option<String>,
    pub unique: Option<TomlUniqueOption>,
}

impl From<TomlColumn> for ColumnDef {
    fn from(column: TomlColumn) -> Self {
        let data_type = match column.data_type {
            Some(dt) => dt,
            None => DataType::Text,
        };
        let nullable = match column.nullable {
            Some(nullable) => nullable,
            None => match column.unique {
                Some(TomlUniqueOption::PrimaryKey) => false,
                _ => true,
            },
        };
        let unique: Option<ColumnUniqueOption> = match column.unique {
            Some(TomlUniqueOption::PrimaryKey) => Some(ColumnUniqueOption { is_primary: true }),
            Some(TomlUniqueOption::Unique) => Some(ColumnUniqueOption { is_primary: false }),
            None => None,
        };
        let default = column.default.map(|value| Expr::TypedString {
            data_type: data_type.clone(),
            value,
        });

        ColumnDef {
            name: column.name,
            data_type,
            nullable,
            default,
            unique,
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
            column_defs: Some(column_defs),
            indexes: vec![],
            created: NaiveDateTime::default(),
            engine: None,
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
            ast::{ColumnDef, Expr},
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
                            nullable: None,
                            default: None,
                            unique: Some(TomlUniqueOption::PrimaryKey)
                        },
                        TomlColumn {
                            name: "name".to_string(),
                            data_type: Some(DataType::Text),
                            nullable: Some(true),
                            default: None,
                            unique: Some(TomlUniqueOption::Unique)
                        },
                        TomlColumn {
                            name: "age".to_string(),
                            data_type: Some(DataType::Uint8),
                            nullable: Some(false),
                            default: None,
                            unique: None
                        },
                        TomlColumn {
                            name: "role".to_string(),
                            data_type: None,
                            nullable: None,
                            default: Some("GUEST".to_string()),
                            unique: None
                        },
                    ]
                }]
            }),
            result
        );
    }

    #[test]
    fn get_single_schema_from_toml_file() {
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
            Some(vec![
                ColumnDef {
                    name: "id".to_string(),
                    data_type: DataType::Int128,
                    nullable: false,
                    default: None,
                    unique: Some(ColumnUniqueOption { is_primary: true })
                },
                ColumnDef {
                    name: "name".to_string(),
                    data_type: DataType::Text,
                    nullable: true,
                    default: None,
                    unique: Some(ColumnUniqueOption { is_primary: false })
                },
                ColumnDef {
                    name: "age".to_string(),
                    data_type: DataType::Uint8,
                    nullable: false,
                    default: None,
                    unique: None,
                },
                ColumnDef {
                    name: "role".to_string(),
                    data_type: DataType::Text,
                    nullable: true,
                    default: Some(Expr::TypedString {
                        data_type: DataType::Text,
                        value: "GUEST".to_string()
                    }),
                    unique: None,
                },
            ]),
            schema.column_defs
        );
        assert_eq!(NaiveDateTime::default(), schema.created);
    }

    #[test]
    fn get_multiple_schema_from_toml_file() {
        // Arrange
        let file_path = "example/schema_list_multiple.toml";
        // Act
        let result = get_schema_list(file_path).unwrap();
        // Assert
        assert_eq!(3, result.iter().count());
        // Table 0 - Users
        let schema = result.get(0).unwrap();
        assert!(matches!(schema, Schema { .. }));
        assert_eq!("users".to_string(), schema.table_name);
        assert_eq!(
            Some(vec![
                ColumnDef {
                    name: "id".to_string(),
                    data_type: DataType::Int128,
                    nullable: false,
                    default: None,
                    unique: Some(ColumnUniqueOption { is_primary: true })
                },
                ColumnDef {
                    name: "name".to_string(),
                    data_type: DataType::Text,
                    nullable: true,
                    default: None,
                    unique: Some(ColumnUniqueOption { is_primary: false })
                },
                ColumnDef {
                    name: "age".to_string(),
                    data_type: DataType::Uint8,
                    nullable: false,
                    default: None,
                    unique: None,
                },
                ColumnDef {
                    name: "role".to_string(),
                    data_type: DataType::Text,
                    nullable: true,
                    default: Some(Expr::TypedString {
                        data_type: DataType::Text,
                        value: "GUEST".to_string()
                    }),
                    unique: None,
                },
            ]),
            schema.column_defs
        );
        assert_eq!(NaiveDateTime::default(), schema.created);
        // Table 1 - orders
        let schema = result.get(1).unwrap();
        assert!(matches!(schema, Schema { .. }));
        assert_eq!("orders".to_string(), schema.table_name);
        assert_eq!(
            Some(vec![
                ColumnDef {
                    name: "id".to_string(),
                    data_type: DataType::Int128,
                    nullable: false,
                    default: None,
                    unique: Some(ColumnUniqueOption { is_primary: true })
                },
                ColumnDef {
                    name: "orderer_id".to_string(),
                    data_type: DataType::Int128,
                    nullable: true,
                    default: None,
                    unique: None,
                },
                ColumnDef {
                    name: "food_id".to_string(),
                    data_type: DataType::Int128,
                    nullable: true,
                    default: None,
                    unique: None,
                },
                ColumnDef {
                    name: "cost".to_string(),
                    data_type: DataType::Uint16,
                    nullable: true,
                    default: Some(Expr::TypedString {
                        data_type: DataType::Uint16,
                        value: "0".to_string()
                    }),
                    unique: None,
                },
            ]),
            schema.column_defs
        );
        assert_eq!(NaiveDateTime::default(), schema.created);
    }
}
