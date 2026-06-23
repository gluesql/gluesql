mod v1_to_v2;

use {
    crate::{
        core::{SCHEMA_TABLE_NAME, STORAGE_META_TABLE_NAME},
        error::StorageError,
    },
    gluesql_core::error::{Error, Result},
    redb::{Database, ReadTransaction, ReadableTable, TableDefinition, WriteTransaction},
    std::path::Path,
};

pub const REDB_STORAGE_FORMAT_VERSION: u32 = 2;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct MigrationReport {
    pub migrated_tables: usize,
    pub unchanged_tables: usize,
    pub rewritten_rows: usize,
}

const V1_REDB_STORAGE_FORMAT_VERSION: u32 = 1;
const SCHEMA_TABLE: TableDefinition<&str, Vec<u8>> = TableDefinition::new(SCHEMA_TABLE_NAME);
const STORAGE_META_TABLE: TableDefinition<&str, u32> =
    TableDefinition::new(STORAGE_META_TABLE_NAME);
const STORAGE_META_VERSION_KEY: &str = "storage_format_version";

type StorageResult<T> = std::result::Result<T, StorageError>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DetectedStorageFormatVersion {
    V1,
    V2,
    UnsupportedNewer(u32),
    Unsupported(u32),
}

pub(super) fn initialize_storage_format_version(db: &Database) -> StorageResult<()> {
    let txn = db.begin_write()?;
    write_storage_format_version(&txn, REDB_STORAGE_FORMAT_VERSION)?;
    txn.commit()?;

    Ok(())
}

pub(super) fn ensure_storage_format_version_supported(db: &Database) -> StorageResult<()> {
    match detect_storage_format_version(db)? {
        DetectedStorageFormatVersion::V1 => Err(StorageError::MigrationRequired {
            found: V1_REDB_STORAGE_FORMAT_VERSION,
            expected: REDB_STORAGE_FORMAT_VERSION,
        }),
        DetectedStorageFormatVersion::V2 => Ok(()),
        DetectedStorageFormatVersion::UnsupportedNewer(version) => {
            Err(StorageError::UnsupportedNewerFormatVersion(version))
        }
        DetectedStorageFormatVersion::Unsupported(version) => {
            Err(StorageError::UnsupportedFormatVersion(version))
        }
    }
}

pub fn migrate_to_latest<P: AsRef<Path>>(filename: P) -> Result<MigrationReport> {
    let path = filename.as_ref();
    if !path.exists() {
        return Err(Error::StorageMsg(format!(
            "[RedbStorage] storage path '{}' does not exist",
            path.display()
        )));
    }
    if !path.is_file() {
        return Err(Error::StorageMsg(format!(
            "[RedbStorage] storage path '{}' is not a file",
            path.display()
        )));
    }

    let db = Database::open(path).map_err(StorageError::from)?;
    migrate_database_to_latest(&db).map_err(Into::into)
}

fn migrate_database_to_latest(db: &Database) -> StorageResult<MigrationReport> {
    match detect_storage_format_version(db)? {
        DetectedStorageFormatVersion::V1 => {
            let mut report = MigrationReport::default();
            let txn = db.begin_write()?;
            let table_names = list_user_table_names(&txn)?;
            report.migrated_tables = table_names.len();

            for table_name in &table_names {
                let rewritten_rows = v1_to_v2::migrate_table_rows(&txn, table_name)?;
                report.rewritten_rows += rewritten_rows;
            }

            write_storage_format_version(&txn, REDB_STORAGE_FORMAT_VERSION)?;
            txn.commit()?;

            Ok(report)
        }
        DetectedStorageFormatVersion::V2 => {
            let read_txn = db.begin_read()?;
            let unchanged_tables = list_user_table_names_from_read(&read_txn)?.len();

            Ok(MigrationReport {
                migrated_tables: 0,
                unchanged_tables,
                rewritten_rows: 0,
            })
        }
        DetectedStorageFormatVersion::UnsupportedNewer(version) => {
            Err(StorageError::UnsupportedNewerFormatVersion(version))
        }
        DetectedStorageFormatVersion::Unsupported(version) => {
            Err(StorageError::UnsupportedFormatVersion(version))
        }
    }
}

fn detect_storage_format_version(db: &Database) -> StorageResult<DetectedStorageFormatVersion> {
    let txn = db.begin_read()?;
    match txn.open_table(STORAGE_META_TABLE) {
        Ok(table) => {
            let version = table
                .get(STORAGE_META_VERSION_KEY)?
                .map(|value| value.value())
                .ok_or(StorageError::MissingFormatVersionMetadata)?;

            Ok(match version {
                REDB_STORAGE_FORMAT_VERSION => DetectedStorageFormatVersion::V2,
                version if version > REDB_STORAGE_FORMAT_VERSION => {
                    DetectedStorageFormatVersion::UnsupportedNewer(version)
                }
                version => DetectedStorageFormatVersion::Unsupported(version),
            })
        }
        Err(redb::TableError::TableDoesNotExist(_)) => Ok(DetectedStorageFormatVersion::V1),
        Err(err) => Err(err.into()),
    }
}

fn list_user_table_names(txn: &WriteTransaction) -> StorageResult<Vec<String>> {
    let table = txn.open_table(SCHEMA_TABLE)?;
    let mut names = table
        .iter()?
        .map(|entry| {
            let (table_name, _schema) = entry?;
            Ok(table_name.value().to_owned())
        })
        .collect::<StorageResult<Vec<_>>>()?;
    names.sort();

    Ok(names)
}

fn list_user_table_names_from_read(txn: &ReadTransaction) -> StorageResult<Vec<String>> {
    match txn.open_table(SCHEMA_TABLE) {
        Ok(table) => {
            let mut names = table
                .iter()?
                .map(|entry| {
                    let (table_name, _schema) = entry?;
                    Ok(table_name.value().to_owned())
                })
                .collect::<StorageResult<Vec<_>>>()?;
            names.sort();

            Ok(names)
        }
        Err(redb::TableError::TableDoesNotExist(_)) => Ok(Vec::new()),
        Err(err) => Err(err.into()),
    }
}

fn write_storage_format_version(txn: &WriteTransaction, version: u32) -> StorageResult<()> {
    let mut table = txn.open_table(STORAGE_META_TABLE)?;
    table.insert(STORAGE_META_VERSION_KEY, &version)?;

    Ok(())
}
