use csv::StringRecord;
use gluesql_core::{
    data::Schema,
    result::{Error, Result},
};

/// A type that contains data info of imported CSV file.
pub struct CsvStorage(Schema);

impl CsvStorage {
    /// Constructs new `CsvStorage` instance from given CSV file
    /// It will create one-table-database since single CSV file only stands
    /// for a single table.
    pub fn read_file(csv_path: &str) -> Result<Self> {
        match csv::ReaderBuilder::new()
            .has_headers(false)
            .from_path(csv_path)
        {
            Err(_) => Err(Error::StorageMsg(
                "Cannot read CSV file from given path".into(),
            )),
            Ok(mut rdr) => {
                let records: Result<Vec<_>> = rdr.records().map(check_record).collect();
                let schema = check_schema(&records?);
                Ok(CsvStorage(schema?))
            }
        }
    }

    /// Constructs new `CsvStorage` instance from given directory.
    /// It will create multi-table-database if there's more than one 
    /// interpretable CSV files.
    pub fn read_dir(dir_path: &str) -> Result<Self> {
        unimplemented!()
    }
}

pub fn check_record(
    record_result: std::result::Result<StringRecord, csv::Error>,
) -> Result<StringRecord> {
    match record_result {
        Err(_) => Err(Error::StorageMsg(
            "Cannot read CSV file from given path".into(),
        )),
        Ok(record) => Ok(record),
    }
}

pub fn check_schema(records: &[StringRecord]) -> Result<Schema> {
    // 1. Check if CSV file has column header
    match records.get(0) {
        None => Err(Error::StorageMsg(
            "Cannot read column headers from given CSV file".into(),
        )),
        Some(_header) => {
            // 2. Check if the header is valid
            // TODO: Fill the code
            let table_name = "table_name".to_string();
            let column_defs = vec![];
            let indexes = vec![];
            Ok(Schema {
                table_name,
                column_defs,
                indexes,
            })
        }
    }
}
