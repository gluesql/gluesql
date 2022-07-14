use {
    csv::StringRecord,
    gluesql_core::{
        data::Schema,
        result::{Error::StorageMsg, Result},
    },
    std::{
        fs::{self, DirEntry, ReadDir},
        io,
        path::Path,
    },
};

/// A type that contains database info of imported CSV file(s).
///
/// ## Fields
///
/// `pub db`: A database, which is a list of table schemas
pub struct CsvStorage {
    pub db: Vec<Schema>,
}

impl CsvStorage {
    /// Constructs new `CsvStorage` instance from given CSV file
    /// It will create one-table-database since single CSV file only stands
    /// for a single table.
    pub fn read_file(csv_path: impl AsRef<Path>) -> Result<Self> {
        let schema = fetch_schema_from_path(csv_path)?;
        let db = vec![schema];

        Ok(CsvStorage { db })
    }

    /// Constructs new `CsvStorage` instance from given directory.
    /// It will create multi-table-database if there's more than one
    /// interpretable CSV files.
    pub fn read_dir(dir_path: &str) -> Result<Self> {
        let schemas: Result<Vec<Schema>> =
            read_dir(dir_path)?.map(fetch_schema_from_entry).collect();
        let db = schemas?;

        Ok(CsvStorage { db })
    }
}

pub fn read_dir(dir_path: impl AsRef<Path>) -> Result<ReadDir> {
    match fs::read_dir(dir_path) {
        Ok(read_dir) => Ok(read_dir),
        Err(_) => Err(StorageMsg("Cannot read dir from given path".into())),
    }
}

pub fn fetch_schema_from_path(csv_path: impl AsRef<Path>) -> Result<Schema> {
    match csv::ReaderBuilder::new()
        .has_headers(false)
        .from_path(csv_path)
    {
        Err(_) => Err(StorageMsg("Cannot read CSV file from given path".into())),
        Ok(mut rdr) => {
            let records: Result<Vec<_>> = rdr.records().map(check_record).collect();
            check_schema(&records?)
        }
    }
}

pub fn fetch_schema_from_entry(entry: io::Result<DirEntry>) -> Result<Schema> {
    match entry {
        Ok(dir_entry) => {
            let csv_path = dir_entry.path();
            fetch_schema_from_path(csv_path)
        }
        Err(_) => Err(StorageMsg("Cannot read entry from given dir".into())),
    }
}

pub fn check_record(
    record_result: std::result::Result<StringRecord, csv::Error>,
) -> Result<StringRecord> {
    match record_result {
        Err(_) => Err(StorageMsg("Cannot read CSV file from given path".into())),
        Ok(record) => Ok(record),
    }
}

pub fn check_schema(records: &[StringRecord]) -> Result<Schema> {
    // 1. Check if CSV file has column header
    match records.get(0) {
        None => Err(StorageMsg(
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
