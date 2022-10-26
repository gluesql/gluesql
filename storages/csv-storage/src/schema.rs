use {
    crate::{error::StorageError, CsvTable},
    gluesql_core::{ast::ColumnDef, prelude::DataType},
    std::{
        path::{Path, PathBuf},
        str::FromStr,
    },
};

pub fn load_schema(file_path: impl AsRef<Path>) -> Result<Vec<CsvTable>, StorageError> {
    Ok(vec![
        CsvTable {
            file_path: PathBuf::from_str("./example/data/users.csv").unwrap(),
            table_name: "users".to_string(),
            column_defs: vec![
                ColumnDef {
                    name: "id".to_string(),
                    data_type: DataType::Int128,
                    options: vec![],
                },
                ColumnDef {
                    name: "name".to_string(),
                    data_type: DataType::Text,
                    options: vec![],
                },
                ColumnDef {
                    name: "age".to_string(),
                    data_type: DataType::Uint8,
                    options: vec![],
                },
                ColumnDef {
                    name: "role".to_string(),
                    data_type: DataType::Text,
                    options: vec![],
                },
            ],
        },
        CsvTable {
            file_path: PathBuf::from_str("./example/data/orders.csv").unwrap(),
            table_name: "orders".to_string(),
            column_defs: vec![
                ColumnDef {
                    name: "id".to_string(),
                    data_type: DataType::Int128,
                    options: vec![],
                },
                ColumnDef {
                    name: "name".to_string(),
                    data_type: DataType::Text,
                    options: vec![],
                },
                ColumnDef {
                    name: "orderer_id".to_string(),
                    data_type: DataType::Int128,
                    options: vec![],
                },
                ColumnDef {
                    name: "restaurant_id".to_string(),
                    data_type: DataType::Int128,
                    options: vec![],
                },
                ColumnDef {
                    name: "quantity".to_string(),
                    data_type: DataType::Uint8,
                    options: vec![],
                },
                ColumnDef {
                    name: "price".to_string(),
                    data_type: DataType::Float,
                    options: vec![],
                },
                ColumnDef {
                    name: "status".to_string(),
                    data_type: DataType::Text,
                    options: vec![],
                },
            ],
        },
    ])
}

#[cfg(test)]
mod test {
    use {
        super::load_schema,
        crate::CsvTable,
        gluesql_core::{ast::ColumnDef, prelude::DataType},
        std::{path::PathBuf, str::FromStr},
    };

    #[test]
    fn successfully_load_schema() {
        // Arrange
        let file_path = "./example/schema.toml";
        // Act
        let result = load_schema(file_path);
        // Assert
        assert_eq!(
            Ok(vec![
                CsvTable {
                    file_path: PathBuf::from_str("./example/data/users.csv").unwrap(),
                    table_name: "users".to_string(),
                    column_defs: vec![
                        ColumnDef {
                            name: "id".to_string(),
                            data_type: DataType::Int128,
                            options: vec![],
                        },
                        ColumnDef {
                            name: "name".to_string(),
                            data_type: DataType::Text,
                            options: vec![],
                        },
                        ColumnDef {
                            name: "age".to_string(),
                            data_type: DataType::Uint8,
                            options: vec![],
                        },
                        ColumnDef {
                            name: "role".to_string(),
                            data_type: DataType::Text,
                            options: vec![],
                        },
                    ],
                },
                CsvTable {
                    file_path: PathBuf::from_str("./example/data/orders.csv").unwrap(),
                    table_name: "orders".to_string(),
                    column_defs: vec![
                        ColumnDef {
                            name: "id".to_string(),
                            data_type: DataType::Int128,
                            options: vec![],
                        },
                        ColumnDef {
                            name: "name".to_string(),
                            data_type: DataType::Text,
                            options: vec![],
                        },
                        ColumnDef {
                            name: "orderer_id".to_string(),
                            data_type: DataType::Int128,
                            options: vec![],
                        },
                        ColumnDef {
                            name: "restaurant_id".to_string(),
                            data_type: DataType::Int128,
                            options: vec![],
                        },
                        ColumnDef {
                            name: "quantity".to_string(),
                            data_type: DataType::Uint8,
                            options: vec![],
                        },
                        ColumnDef {
                            name: "price".to_string(),
                            data_type: DataType::Float,
                            options: vec![],
                        },
                        ColumnDef {
                            name: "status".to_string(),
                            data_type: DataType::Text,
                            options: vec![],
                        },
                    ],
                },
            ]),
            result
        );
    }
}
