mod decode;
mod error;
mod schema_rewrites;
mod v1_to_v2;
mod v2_to_v3;
mod v2_to_v4;
mod v3_to_v4;
mod v4_to_v5;

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
/// format v5 adds index maintenance, keeps v4 schemas and v2 row serialization,
/// and requires redb file format v3. Older writers must not modify indexed data.
pub const REDB_STORAGE_FORMAT_VERSION: u32 = V5_REDB_STORAGE_FORMAT_VERSION;

const V1_REDB_STORAGE_FORMAT_VERSION: u32 = 1;
const V2_REDB_STORAGE_FORMAT_VERSION: u32 = 2;
const V3_REDB_STORAGE_FORMAT_VERSION: u32 = 3;
const V4_REDB_STORAGE_FORMAT_VERSION: u32 = 4;
const V5_REDB_STORAGE_FORMAT_VERSION: u32 = 5;

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
    V5,
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
        DetectedStorageFormatVersion::V4 => Err(MigrationError::MigrationRequired {
            found: V4_REDB_STORAGE_FORMAT_VERSION,
            expected: REDB_STORAGE_FORMAT_VERSION,
        }
        .into()),
        DetectedStorageFormatVersion::V5 => Ok(()),
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
    let report = match detect_storage_format_version(db)? {
        DetectedStorageFormatVersion::V1 => migrate_v1_to_v4(db),
        DetectedStorageFormatVersion::V2 => v2_to_v4::migrate(db),
        DetectedStorageFormatVersion::V3 => v3_to_v4::migrate(db),
        DetectedStorageFormatVersion::V4 => return v4_to_v5::migrate(db),
        DetectedStorageFormatVersion::V5 => {
            let read_txn = db.begin_read()?;
            let unchanged_tables = list_user_table_names_from_read(&read_txn)?.len();
            return Ok(MigrationReport {
                migrated_tables: 0,
                unchanged_tables,
                rewritten_rows: 0,
            });
        }
        DetectedStorageFormatVersion::UnsupportedNewer(version) => {
            Err(MigrationError::UnsupportedNewerFormatVersion(version).into())
        }
        DetectedStorageFormatVersion::Unsupported(version) => {
            Err(MigrationError::UnsupportedFormatVersion(version).into())
        }
    }?;
    // Preserve the source migration's counts; v5 does not rewrite its payloads.
    v4_to_v5::migrate(db)?;
    Ok(report)
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
    write_storage_format_version(&txn, V4_REDB_STORAGE_FORMAT_VERSION)?;
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
                V4_REDB_STORAGE_FORMAT_VERSION => DetectedStorageFormatVersion::V4,
                V5_REDB_STORAGE_FORMAT_VERSION => DetectedStorageFormatVersion::V5,
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

#[cfg(test)]
mod tests {
    use {
        super::{
            DetectedStorageFormatVersion, MigrationError, MigrationReport, SCHEMA_TABLE,
            STORAGE_META_TABLE, STORAGE_META_VERSION_KEY, StorageError, decode::DecodeError,
            detect_storage_format_version, ensure_storage_format_version_supported,
            error::SchemaDecodeError, initialize_storage_format_version,
            list_user_table_names_from_read, migrate_database_to_latest, migrate_to_latest,
        },
        gluesql_core::{
            data::{Key, Schema, Value},
            error::Error,
        },
        redb::{Database, TableDefinition, TableError, backends::InMemoryBackend},
        std::{env::temp_dir, fs},
        uuid::Uuid,
    };

    #[test]
    fn missing_path_is_rejected_without_creating_a_database() {
        let path = temp_dir().join(format!("gluesql-redb-missing-{}", Uuid::now_v7()));
        assert_eq!(
            migrate_to_latest(&path),
            Err(Error::StorageMsg(format!(
                "[RedbStorage] storage path '{}' does not exist",
                path.display()
            )))
        );
        assert!(!path.exists());
    }

    #[test]
    fn directory_path_is_rejected() {
        let path = temp_dir();
        assert_eq!(
            migrate_to_latest(&path),
            Err(Error::StorageMsg(format!(
                "[RedbStorage] storage path '{}' is not a file",
                path.display()
            )))
        );
    }

    #[test]
    fn path_entry_point_opens_the_database_and_returns_its_migration_report() {
        let path = temp_dir().join(format!("gluesql-redb-migration-{}", Uuid::now_v7()));
        let db = Database::create(&path).unwrap();
        initialize_storage_format_version(&db).unwrap();
        drop(db);
        let result = migrate_to_latest(&path);
        fs::remove_file(path).unwrap();
        assert_eq!(result, Ok(MigrationReport::default()));
    }

    #[test]
    fn detects_absent_metadata_and_each_stored_version() {
        for (version, expected) in [
            (None, DetectedStorageFormatVersion::V1),
            (Some(2), DetectedStorageFormatVersion::V2),
            (Some(3), DetectedStorageFormatVersion::V3),
            (Some(4), DetectedStorageFormatVersion::V4),
            (Some(5), DetectedStorageFormatVersion::V5),
            (Some(0), DetectedStorageFormatVersion::Unsupported(0)),
            (Some(1), DetectedStorageFormatVersion::Unsupported(1)),
            (Some(6), DetectedStorageFormatVersion::UnsupportedNewer(6)),
        ] {
            let db = Database::builder()
                .create_with_backend(InMemoryBackend::new())
                .unwrap();
            if let Some(version) = version {
                let txn = db.begin_write().unwrap();
                txn.open_table(STORAGE_META_TABLE)
                    .unwrap()
                    .insert(STORAGE_META_VERSION_KEY, version)
                    .unwrap();
                txn.commit().unwrap();
            }
            assert_eq!(detect_storage_format_version(&db).unwrap(), expected);
        }
    }

    #[test]
    fn old_versions_require_migration_before_opening_storage() {
        for (version, expected_found) in [(None, 1), (Some(2), 2), (Some(3), 3), (Some(4), 4)] {
            let db = Database::builder()
                .create_with_backend(InMemoryBackend::new())
                .unwrap();
            if let Some(version) = version {
                let txn = db.begin_write().unwrap();
                txn.open_table(STORAGE_META_TABLE)
                    .unwrap()
                    .insert(STORAGE_META_VERSION_KEY, version)
                    .unwrap();
                txn.commit().unwrap();
            }
            assert!(matches!(ensure_storage_format_version_supported(&db),
                Err(StorageError::Migration(MigrationError::MigrationRequired { found, expected: 5 }))
                if found == expected_found
            ));
        }
    }

    #[test]
    fn initialization_records_v5_and_allows_opening_storage() {
        let db = Database::builder()
            .create_with_backend(InMemoryBackend::new())
            .unwrap();
        initialize_storage_format_version(&db).unwrap();
        let txn = db.begin_read().unwrap();
        let table = txn.open_table(STORAGE_META_TABLE).unwrap();
        assert_eq!(
            table
                .get(STORAGE_META_VERSION_KEY)
                .unwrap()
                .unwrap()
                .value(),
            5
        );
        ensure_storage_format_version_supported(&db).unwrap();
    }

    #[test]
    fn unsupported_versions_are_rejected_by_admission_and_migration() {
        for version in [0, 1] {
            let mut db = Database::builder()
                .create_with_backend(InMemoryBackend::new())
                .unwrap();
            let txn = db.begin_write().unwrap();
            txn.open_table(STORAGE_META_TABLE)
                .unwrap()
                .insert(STORAGE_META_VERSION_KEY, version)
                .unwrap();
            txn.commit().unwrap();
            for error in [
                ensure_storage_format_version_supported(&db).unwrap_err(),
                migrate_database_to_latest(&mut db).unwrap_err(),
            ] {
                assert!(matches!(error,
                    StorageError::Migration(MigrationError::UnsupportedFormatVersion(found))
                    if found == version
                ));
            }
        }
    }

    #[test]
    fn newer_versions_are_rejected_by_admission_and_migration() {
        let mut db = Database::builder()
            .create_with_backend(InMemoryBackend::new())
            .unwrap();
        let txn = db.begin_write().unwrap();
        txn.open_table(STORAGE_META_TABLE)
            .unwrap()
            .insert(STORAGE_META_VERSION_KEY, 6)
            .unwrap();
        txn.commit().unwrap();
        for error in [
            ensure_storage_format_version_supported(&db).unwrap_err(),
            migrate_database_to_latest(&mut db).unwrap_err(),
        ] {
            assert!(matches!(
                error,
                StorageError::Migration(MigrationError::UnsupportedNewerFormatVersion(6))
            ));
        }
    }

    #[test]
    fn metadata_without_a_version_key_is_rejected() {
        let mut db = Database::builder()
            .create_with_backend(InMemoryBackend::new())
            .unwrap();
        let txn = db.begin_write().unwrap();
        txn.open_table(STORAGE_META_TABLE).unwrap();
        txn.commit().unwrap();
        for error in [
            detect_storage_format_version(&db).unwrap_err(),
            ensure_storage_format_version_supported(&db).unwrap_err(),
            migrate_database_to_latest(&mut db).unwrap_err(),
        ] {
            assert!(matches!(
                error,
                StorageError::Migration(MigrationError::MissingFormatVersionMetadata)
            ));
        }
    }

    #[test]
    fn metadata_table_type_errors_keep_their_source() {
        let mut db = Database::builder()
            .create_with_backend(InMemoryBackend::new())
            .unwrap();
        let txn = db.begin_write().unwrap();
        txn.open_table(TableDefinition::<&str, Vec<u8>>::new("__GLUESQL_META__"))
            .unwrap();
        txn.commit().unwrap();
        for error in [
            detect_storage_format_version(&db).unwrap_err(),
            ensure_storage_format_version_supported(&db).unwrap_err(),
            migrate_database_to_latest(&mut db).unwrap_err(),
        ] {
            assert!(matches!(error,
                StorageError::RedbTable(TableError::TableTypeMismatch { table, key, value })
                if table == "__GLUESQL_META__"
                    && key == <&str as redb::Value>::type_name()
                    && value == <Vec<u8> as redb::Value>::type_name()
            ));
        }
    }

    #[test]
    fn old_versions_dispatch_to_v5_and_report_their_row_work() {
        let key = Key::I64(7);
        let values = vec![Value::I64(9)];
        for (version, payload, expected) in [
            (
                None,
                bincode::serialize(&(&key, 0_u32, &values)).unwrap(),
                MigrationReport {
                    migrated_tables: 1,
                    unchanged_tables: 0,
                    rewritten_rows: 1,
                },
            ),
            (
                Some(2),
                bincode::serialize(&(&key, &values)).unwrap(),
                MigrationReport {
                    migrated_tables: 0,
                    unchanged_tables: 1,
                    rewritten_rows: 0,
                },
            ),
            (
                Some(3),
                bincode::serialize(&(&key, &values)).unwrap(),
                MigrationReport {
                    migrated_tables: 0,
                    unchanged_tables: 1,
                    rewritten_rows: 0,
                },
            ),
        ] {
            let mut db = Database::builder()
                .create_with_backend(InMemoryBackend::new())
                .unwrap();
            let txn = db.begin_write().unwrap();
            if let Some(version) = version {
                txn.open_table(STORAGE_META_TABLE)
                    .unwrap()
                    .insert(STORAGE_META_VERSION_KEY, version)
                    .unwrap();
            }
            let schema = Schema::from_ddl("CREATE TABLE Item (id INT);").unwrap();
            txn.open_table(SCHEMA_TABLE)
                .unwrap()
                .insert("Item", bincode::serialize(&schema).unwrap())
                .unwrap();
            let rows = TableDefinition::<&[u8], Vec<u8>>::new("Item");
            txn.open_table(rows)
                .unwrap()
                .insert(b"row-key".as_slice(), payload)
                .unwrap();
            txn.commit().unwrap();

            assert_eq!(migrate_database_to_latest(&mut db).unwrap(), expected);
            let txn = db.begin_read().unwrap();
            assert_eq!(
                txn.open_table(STORAGE_META_TABLE)
                    .unwrap()
                    .get(STORAGE_META_VERSION_KEY)
                    .unwrap()
                    .unwrap()
                    .value(),
                5
            );
            assert_eq!(
                txn.open_table(rows)
                    .unwrap()
                    .get(b"row-key".as_slice())
                    .unwrap()
                    .unwrap()
                    .value(),
                bincode::serialize(&(&key, &values)).unwrap()
            );
        }
    }

    #[test]
    fn missing_row_table_stops_each_old_version_before_recording_v4() {
        let schema = Schema::from_ddl("CREATE TABLE Item (id INT);").unwrap();
        let payload = bincode::serialize(&schema).unwrap();
        for (version, expected) in [
            (None, DetectedStorageFormatVersion::V1),
            (Some(2), DetectedStorageFormatVersion::V2),
            (Some(3), DetectedStorageFormatVersion::V3),
        ] {
            let mut db = Database::builder()
                .create_with_backend(InMemoryBackend::new())
                .unwrap();
            let txn = db.begin_write().unwrap();
            if let Some(version) = version {
                txn.open_table(STORAGE_META_TABLE)
                    .unwrap()
                    .insert(STORAGE_META_VERSION_KEY, version)
                    .unwrap();
            }
            txn.open_table(SCHEMA_TABLE)
                .unwrap()
                .insert("Item", payload.clone())
                .unwrap();
            txn.commit().unwrap();

            assert!(matches!(migrate_database_to_latest(&mut db),
                Err(StorageError::RedbTable(TableError::TableDoesNotExist(name)))
                if name == "Item"
            ));
            assert_eq!(detect_storage_format_version(&db).unwrap(), expected);
            let txn = db.begin_read().unwrap();
            assert!(matches!(
                txn.open_table(TableDefinition::<&[u8], Vec<u8>>::new("Item")),
                Err(TableError::TableDoesNotExist(name)) if name == "Item"
            ));
            assert_eq!(
                txn.open_table(SCHEMA_TABLE)
                    .unwrap()
                    .get("Item")
                    .unwrap()
                    .unwrap()
                    .value(),
                payload
            );
        }
    }

    #[test]
    fn source_version_selects_the_schema_failure_policy() {
        for version in [None, Some(2), Some(3)] {
            let mut db = Database::builder()
                .create_with_backend(InMemoryBackend::new())
                .unwrap();
            let txn = db.begin_write().unwrap();
            if let Some(version) = version {
                txn.open_table(STORAGE_META_TABLE)
                    .unwrap()
                    .insert(STORAGE_META_VERSION_KEY, version)
                    .unwrap();
            }
            txn.open_table(SCHEMA_TABLE)
                .unwrap()
                .insert("Item", vec![255])
                .unwrap();
            txn.commit().unwrap();
            assert!(matches!((version, migrate_database_to_latest(&mut db)),
                (None | Some(2), Err(StorageError::Migration(MigrationError::InvalidSchemaPayload {
                    table_name,
                    source: SchemaDecodeError::Payload(DecodeError::Invalid(_)),
                })))
                | (Some(3), Err(StorageError::Migration(MigrationError::BothSchemaCandidatesFailed {
                    table_name,
                    v1: SchemaDecodeError::Payload(DecodeError::Invalid(_)),
                    v3: SchemaDecodeError::Payload(DecodeError::Invalid(_)),
                }))) if table_name == "Item"
            ));
        }
    }

    #[test]
    fn current_version_counts_existing_schemas_without_rewriting_them() {
        let mut db = Database::builder()
            .create_with_backend(InMemoryBackend::new())
            .unwrap();
        initialize_storage_format_version(&db).unwrap();
        assert_eq!(
            migrate_database_to_latest(&mut db).unwrap(),
            MigrationReport::default()
        );

        let txn = db.begin_write().unwrap();
        {
            let mut table = txn.open_table(SCHEMA_TABLE).unwrap();
            table.insert("ZLast", b"last".to_vec()).unwrap();
            table.insert("AFirst", b"first".to_vec()).unwrap();
        }
        txn.commit().unwrap();
        assert_eq!(
            migrate_database_to_latest(&mut db).unwrap(),
            MigrationReport {
                migrated_tables: 0,
                unchanged_tables: 2,
                rewritten_rows: 0,
            }
        );
        let txn = db.begin_read().unwrap();
        assert_eq!(
            list_user_table_names_from_read(&txn).unwrap(),
            vec!["AFirst", "ZLast"]
        );
        let table = txn.open_table(SCHEMA_TABLE).unwrap();
        assert_eq!(table.get("AFirst").unwrap().unwrap().value(), b"first");
        assert_eq!(table.get("ZLast").unwrap().unwrap().value(), b"last");
    }

    #[test]
    fn current_version_propagates_schema_table_errors() {
        let mut db = Database::builder()
            .create_with_backend(InMemoryBackend::new())
            .unwrap();
        initialize_storage_format_version(&db).unwrap();
        let txn = db.begin_write().unwrap();
        txn.open_table(TableDefinition::<&str, u32>::new("__SCHEMA__"))
            .unwrap();
        txn.commit().unwrap();
        assert!(matches!(migrate_database_to_latest(&mut db),
            Err(StorageError::RedbTable(TableError::TableTypeMismatch { table, key, value }))
            if table == "__SCHEMA__"
                && key == <&str as redb::Value>::type_name()
                && value == <u32 as redb::Value>::type_name()
        ));
    }
}
