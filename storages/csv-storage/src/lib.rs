use error::{err_into, StorageError};

mod error;
use {
    csv::ReaderBuilder,
    gluesql_core::{
        ast::ColumnDef,
        prelude::{DataType, Row},
        result::Result,
    },
    std::path::Path,
};

#[derive(Debug, PartialEq)]
pub struct CsvTable {
    name: String,
    columns: Vec<ColumnDef>,
    rows: Vec<Row>,
}

impl CsvTable {
    /// Create csv table from given path.
    pub fn from_path(path: impl AsRef<Path>) -> Result<Self> {
        let mut result = ReaderBuilder::new().from_path(&path).map_err(err_into)?;

        let columns: Vec<ColumnDef> = result
            .headers()
            .map_err(err_into)?
            .into_iter()
            .map(|col| ColumnDef {
                name: col.to_owned(),
                data_type: DataType::Text,
                options: vec![],
            })
            .collect();

        if let Some(os_filename) = &path.as_ref().file_name() {
            if let Some(filename) = os_filename.to_str() {
                if filename.contains(".csv") {
                    return Ok(CsvTable {
                        name: filename.replace(".csv", ""),
                        columns,
                        rows: vec![],
                    });
                }
            }
        }

        Err(StorageError::InvalidFileImport(format!("{}", path.as_ref().display())).into())
    }
}

#[cfg(test)]
mod test {
    use {
        crate::CsvTable,
        gluesql_core::{ast::ColumnDef, prelude::DataType},
        std::fs,
    };

    #[test]
    fn create_csv_table_from_csv_file() {
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
                name: "users".to_owned(),
                columns: vec![
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
                rows: vec![]
            })
        );

        // Should cleanup created csv file
        fs::remove_file(csv_path).unwrap();
    }
}
