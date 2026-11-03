mod decode;
mod error;
mod schema_rewrites;
mod v1_to_v2;
mod v2_to_v3;
mod v2_to_v4;
mod v3_to_v4;

pub(super) use error::MigrationError;

use {
    crate::{
        core::{SCHEMA_TABLE_NAME, STORAGE_META_TABLE_NAME},
        error::StorageError,
    },
    gluesql_core::error::Result,
    redb::{Database, ReadTransaction, ReadableTable, TableDefinition, WriteTransaction},
    schema_rewrites::SchemaRewrites,
    std::path::Path,
};

/// `GlueSQL`'s redb storage format version.
///
/// This is stored in the `__GLUESQL_META__` table and is independent of both
/// the redb crate version and redb's internal file format version. Storage
/// format v4 uses the current core bincode schema layout, keeps the v2 row
/// serialization and requires redb file format v3.
pub const REDB_STORAGE_FORMAT_VERSION: u32 = 4;

const V1_REDB_STORAGE_FORMAT_VERSION: u32 = 1;
const V2_REDB_STORAGE_FORMAT_VERSION: u32 = 2;
const V3_REDB_STORAGE_FORMAT_VERSION: u32 = 3;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct MigrationReport {
    pub migrated_tables: usize,
    pub unchanged_tables: usize,
    pub rewritten_rows: usize,
}

const SCHEMA_TABLE: TableDefinition<&str, Vec<u8>> = TableDefinition::new(SCHEMA_TABLE_NAME);
const STORAGE_META_TABLE: TableDefinition<&str, u32> =
    TableDefinition::new(STORAGE_META_TABLE_NAME);
const STORAGE_META_VERSION_KEY: &str = "storage_format_version";

type StorageResult<T> = std::result::Result<T, StorageError>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DetectedStorageFormatVersion {
    V1,
    V2,
    V3,
    V4,
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
        DetectedStorageFormatVersion::V1 => Err(MigrationError::MigrationRequired {
            found: V1_REDB_STORAGE_FORMAT_VERSION,
            expected: REDB_STORAGE_FORMAT_VERSION,
        }
        .into()),
        DetectedStorageFormatVersion::V2 => Err(MigrationError::MigrationRequired {
            found: V2_REDB_STORAGE_FORMAT_VERSION,
            expected: REDB_STORAGE_FORMAT_VERSION,
        }
        .into()),
        DetectedStorageFormatVersion::V3 => Err(MigrationError::MigrationRequired {
            found: V3_REDB_STORAGE_FORMAT_VERSION,
            expected: REDB_STORAGE_FORMAT_VERSION,
        }
        .into()),
        DetectedStorageFormatVersion::V4 => Ok(()),
        DetectedStorageFormatVersion::UnsupportedNewer(version) => {
            Err(MigrationError::UnsupportedNewerFormatVersion(version).into())
        }
        DetectedStorageFormatVersion::Unsupported(version) => {
            Err(MigrationError::UnsupportedFormatVersion(version).into())
        }
    }
}

pub fn migrate_to_latest<P: AsRef<Path>>(filename: P) -> Result<MigrationReport> {
    let path = filename.as_ref();
    if !path.exists() {
        return Err(
            StorageError::Migration(MigrationError::StoragePathDoesNotExist(path.to_path_buf()))
                .into(),
        );
    }
    if !path.is_file() {
        return Err(StorageError::Migration(MigrationError::StoragePathNotFile(
            path.to_path_buf(),
        ))
        .into());
    }

    let mut db = Database::open(path).map_err(StorageError::from)?;
    migrate_database_to_latest(&mut db).map_err(Into::into)
}

fn migrate_database_to_latest(db: &mut Database) -> StorageResult<MigrationReport> {
    match detect_storage_format_version(db)? {
        DetectedStorageFormatVersion::V1 => migrate_v1_to_v4(db),
        DetectedStorageFormatVersion::V2 => v2_to_v4::migrate(db),
        DetectedStorageFormatVersion::V3 => v3_to_v4::migrate(db),
        DetectedStorageFormatVersion::V4 => {
            let read_txn = db.begin_read()?;
            let unchanged_tables = list_user_table_names_from_read(&read_txn)?.len();
            Ok(MigrationReport {
                migrated_tables: 0,
                unchanged_tables,
                rewritten_rows: 0,
            })
        }
        DetectedStorageFormatVersion::UnsupportedNewer(version) => {
            Err(MigrationError::UnsupportedNewerFormatVersion(version).into())
        }
        DetectedStorageFormatVersion::Unsupported(version) => {
            Err(MigrationError::UnsupportedFormatVersion(version).into())
        }
    }
}

fn migrate_v1_to_v4(db: &mut Database) -> StorageResult<MigrationReport> {
    // Keep v1 provenance during preflight: its schemas use the v0.19 layout.
    let schemas = SchemaRewrites::inspect(db, v2_to_v4::rewrite_schema)?;

    // This physical upgrade is outside the payload transaction. If it succeeds
    // and a later row fails, logical v1 remains and upgrade() is a no-op on retry.
    v2_to_v3::migrate(db)?;

    let txn = db.begin_write()?;
    let mut report = MigrationReport {
        migrated_tables: schemas.table_names().len(),
        unchanged_tables: 0,
        rewritten_rows: 0,
    };
    // Logical path: v1 -> v2 rows -> v4 schemas, committed together with meta.
    for table_name in schemas.table_names() {
        report.rewritten_rows += v1_to_v2::migrate_table_rows(&txn, table_name)?;
    }
    schemas.apply(&txn)?;
    commit_v4(txn)?;
    Ok(report)
}

fn commit_v4(txn: WriteTransaction) -> StorageResult<()> {
    write_storage_format_version(&txn, REDB_STORAGE_FORMAT_VERSION)?;
    txn.commit()?;
    Ok(())
}

fn detect_storage_format_version(db: &Database) -> StorageResult<DetectedStorageFormatVersion> {
    let txn = db.begin_read()?;
    match txn.open_table(STORAGE_META_TABLE) {
        Ok(table) => {
            let version = table
                .get(STORAGE_META_VERSION_KEY)?
                .map(|value| value.value())
                .ok_or(MigrationError::MissingFormatVersionMetadata)?;

            Ok(match version {
                V2_REDB_STORAGE_FORMAT_VERSION => DetectedStorageFormatVersion::V2,
                V3_REDB_STORAGE_FORMAT_VERSION => DetectedStorageFormatVersion::V3,
                REDB_STORAGE_FORMAT_VERSION => DetectedStorageFormatVersion::V4,
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
