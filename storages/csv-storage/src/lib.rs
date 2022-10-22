mod error;

use {
    csv::ReaderBuilder,
    error::{err_into, StorageError},
    gluesql_core::{ast::ColumnDef, prelude::DataType, result::Result},
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
    pub fn from_path(path: impl AsRef<Path>) -> Result<Self> {
        let file_path = path.as_ref().to_path_buf();

        let column_defs: Vec<ColumnDef> = ReaderBuilder::new()
            .from_path(&path)
            .map_err(err_into)?
            .headers()
            .map_err(err_into)?
            .into_iter()
            .map(|col| ColumnDef {
                name: col.to_owned(),
                data_type: DataType::Text,
                options: vec![],
            })
            .collect();

        path.as_ref()
            .file_name()
            .and_then(OsStr::to_str)
            .and_then(|filename| {
                Some(CsvTable {
                    file_path,
                    table_name: filename.replace(".csv", ""),
                    column_defs,
                })
            })
            .ok_or(StorageError::InvalidFileImport(format!("{}", path.as_ref().display())).into())
    }
}

#[cfg(test)]
mod test {
    use {
        crate::CsvTable,
        gluesql_core::{ast::ColumnDef, prelude::DataType},
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
            result,
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
            })
        );

        // Should cleanup created csv file
        fs::remove_file(csv_path).unwrap();
    }
}
