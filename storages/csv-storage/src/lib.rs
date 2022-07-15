use {
    csv::StringRecord,
    gluesql_core::{
        data::Schema,
        result::{Error::StorageMsg, Result},
    },
    std::{
        fs::{self, DirEntry, ReadDir},
        io,
        path::{Path, PathBuf},
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
    /// Constructs single-table database from given CSV file.
    pub fn read_file(csv_path: impl AsRef<Path>) -> Result<Self> {
        let schema = fetch_schema_from_path(csv_path)?;
        let db = vec![schema];

        Ok(CsvStorage { db })
    }

    /// Constructs multi-table database from given directory.
    ///
    /// This method should work only if ...
    ///
    /// - there's at least one CSV file in directory.
    /// - every CSV file should be convertable to table schema.
    pub fn read_dir(dir_path: &str) -> Result<Self> {
        let db = get_files_from_dir(dir_path)?
            .filter_map(get_csv_file)
            .map(fetch_schema_from_path)
            .collect::<Result<Vec<Schema>>>()?;

        if db.is_empty() {
            return Err(StorageMsg(
                "No interpretable CSV files in given directory".into(),
            ));
        }

        Ok(CsvStorage { db })
    }
}

pub fn get_files_from_dir(dir_path: impl AsRef<Path>) -> Result<ReadDir> {
    match fs::read_dir(dir_path) {
        Ok(read_dir) => Ok(read_dir),
        Err(_) => Err(StorageMsg("Cannot read dir from given path".into())),
    }
}

pub fn get_csv_file(entry: io::Result<DirEntry>) -> Option<PathBuf> {
    match entry {
        Ok(dir_entry) => {
            let path = dir_entry.path();
            if path.ends_with(".csv") {
                return Some(path);
            }
            None
        }
        Err(_) => None,
    }
}

/// Fetch schema from given csv
pub fn fetch_schema_from_path(csv_path: impl AsRef<Path>) -> Result<Schema> {
    match csv::ReaderBuilder::new()
        .has_headers(false)
        .from_path(csv_path)
    {
        Err(_) => Err(StorageMsg("Cannot read CSV file from given path".into())),
        Ok(mut rdr) => {
            let records: Result<Vec<StringRecord>> = rdr.records().map(check_record).collect();
            check_schema(&records?)
        }
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
