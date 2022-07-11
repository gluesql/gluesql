use csv::StringRecord;
use gluesql_core::result::{Error, Result};
use std::collections::HashMap;

pub struct Item {}

pub struct Row {
    pub items: HashMap<String, Item>,
}

/// A type that contains data info of imported CSV file.
pub struct CsvStorage {
    pub records: Vec<StringRecord>,
}

impl CsvStorage {
    pub fn read_file(filename: &str) -> Result<Self> {
        match csv::Reader::from_path(filename) {
            Err(_) => Err(Error::StorageMsg(
                "Cannot read CSV file from given path".into(),
            )),
            Ok(mut rdr) => {
                let records: Result<Vec<_>> = rdr.records().map(check_record).collect();
                Ok(CsvStorage { records: records? })
            }
        }
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
