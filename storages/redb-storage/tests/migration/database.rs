//! File and raw-table operations for migration integration tests.
//! Version policy and expected migration results belong to the calling test.

use {
    redb::{Builder, Database, ReadableTable, TableDefinition},
    std::{collections::BTreeMap, fs, path::PathBuf},
    uuid::Uuid,
};

pub const SCHEMAS: TableDefinition<&str, Vec<u8>> = TableDefinition::new("__SCHEMA__");
pub const META: TableDefinition<&str, u32> = TableDefinition::new("__GLUESQL_META__");
pub const VERSION_KEY: &str = "storage_format_version";
pub type RawRows = Vec<(Vec<u8>, Vec<u8>)>;

pub struct TestFile(pub PathBuf);

impl TestFile {
    pub fn empty() -> Self {
        Self(std::env::temp_dir().join(format!("gluesql-redb-migration-{}.redb", Uuid::now_v7())))
    }

    pub fn fixture(relative: &str) -> Self {
        let file = Self::empty();
        let source = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures")
            .join(relative);
        fs::copy(source, &file.0).expect("copy historical fixture");
        file
    }
}

impl Drop for TestFile {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.0);
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct Snapshot {
    pub version: Option<u32>,
    pub schemas: BTreeMap<String, Vec<u8>>,
    pub rows: BTreeMap<String, RawRows>,
}

pub fn snapshot(file: &TestFile) -> Snapshot {
    let db = Database::open(&file.0).expect("open raw database");
    let txn = db.begin_read().unwrap();
    let version = match txn.open_table(META) {
        Ok(table) => table.get(VERSION_KEY).unwrap().map(|value| value.value()),
        Err(redb::TableError::TableDoesNotExist(_)) => None,
        Err(error) => panic!("metadata: {error}"),
    };
    let schemas: BTreeMap<_, _> = match txn.open_table(SCHEMAS) {
        Ok(table) => table
            .iter()
            .unwrap()
            .map(|entry| {
                let (name, value) = entry.unwrap();
                (name.value().to_owned(), value.value())
            })
            .collect(),
        Err(redb::TableError::TableDoesNotExist(_)) => BTreeMap::new(),
        Err(error) => panic!("schemas: {error}"),
    };
    let rows = schemas
        .keys()
        .map(|name| {
            let definition = TableDefinition::<&[u8], Vec<u8>>::new(name);
            let rows = match txn.open_table(definition) {
                Ok(table) => table
                    .iter()
                    .unwrap()
                    .map(|entry| {
                        let (key, value) = entry.unwrap();
                        (key.value().to_vec(), value.value())
                    })
                    .collect(),
                Err(redb::TableError::TableDoesNotExist(_)) => Vec::new(),
                Err(error) => panic!("rows: {error}"),
            };
            (name.clone(), rows)
        })
        .collect();
    Snapshot {
        version,
        schemas,
        rows,
    }
}

pub fn write_database(file: &TestFile, file_format_v3: bool, state: &Snapshot) {
    let db = Builder::new()
        .create_with_file_format_v3(file_format_v3)
        .create(&file.0)
        .unwrap();
    let txn = db.begin_write().unwrap();
    {
        let mut table = txn.open_table(SCHEMAS).unwrap();
        for (name, payload) in &state.schemas {
            table.insert(name.as_str(), payload.clone()).unwrap();
        }
    }
    for (name, rows) in &state.rows {
        let definition = TableDefinition::<&[u8], Vec<u8>>::new(name);
        let mut table = txn.open_table(definition).unwrap();
        for (key, payload) in rows {
            table.insert(key.as_slice(), payload.clone()).unwrap();
        }
    }
    if let Some(version) = state.version {
        txn.open_table(META)
            .unwrap()
            .insert(VERSION_KEY, &version)
            .unwrap();
    }
    txn.commit().unwrap();
}

pub fn replace_row(file: &TestFile, name: &str, key: &[u8], payload: &[u8]) {
    let db = Database::open(&file.0).unwrap();
    let txn = db.begin_write().unwrap();
    txn.open_table(TableDefinition::<&[u8], Vec<u8>>::new(name))
        .unwrap()
        .insert(key, payload.to_vec())
        .unwrap();
    txn.commit().unwrap();
}
