mod atomic_file;
mod lock;
mod paths;
mod recovery;
mod schema_file;
mod staging;
mod v1_to_v2;

use {
    crate::FileStorage,
    gluesql_core::{
        data::Schema,
        error::{Error, Result},
    },
    lock::MigrationLock,
    paths::MigrationPaths,
    schema_file::TableVersions,
    std::{convert::AsRef, fs, path::Path},
};

pub const FILE_STORAGE_FORMAT_VERSION: u32 = 2;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct MigrationReport {
    pub migrated_tables: usize,
    pub unchanged_tables: usize,
    pub rewritten_rows: usize,
}

impl FileStorage {
    pub(super) fn ensure_no_migration_lock(path: &Path) -> Result<()> {
        let Ok(lock_path) = MigrationPaths::lock_of(path) else {
            return Ok(());
        };
        if !lock_path.exists() {
            return Ok(());
        }

        Err(Error::StorageMsg(format!(
            "[FileStorage] migration or recovery in progress for '{}'; lock '{}' exists, run migrate_to_latest to finish or recover it before opening",
            path.display(),
            lock_path.display(),
        )))
    }

    pub(super) fn ensure_schema_versions_supported(path: &Path) -> Result<()> {
        schema_file::ensure_versions_supported(path)
    }

    pub(super) fn write_schema_file(path: &Path, schema: &Schema) -> Result<()> {
        schema_file::write(path, schema)
    }
}

pub fn migrate_to_latest<T: AsRef<Path>>(path: T) -> Result<MigrationReport> {
    let paths = MigrationPaths::new(path.as_ref())?;

    if paths.lock.exists() {
        recovery::finish_interrupted(&paths)?;
    }

    paths.ensure_storage_dir()?;
    staging::reject_interrupted_writes(&paths.storage)?;

    let versions = schema_file::classify(&paths.storage)?;
    if versions.v1 == 0 {
        return Ok(summarize(versions, 0));
    }

    paths.ensure_renamable_root()?;
    paths.ensure_siblings_available()?;

    let mut lock = MigrationLock::create(&paths.lock)?;
    lock.record_staging()?;

    let rewritten_rows = match staging::build(&paths.storage, &paths.staging, v1_to_v2::decode_row)
    {
        Ok(rewritten_rows) => rewritten_rows,
        Err(err) => {
            let _ = fs::remove_dir_all(&paths.staging);

            return Err(err);
        }
    };

    lock.begin_cutover()?;
    lock.rename(&paths.storage, &paths.backup)?;
    lock.rename(&paths.staging, &paths.storage)?;

    lock.remove_dir_all(&paths.backup)?;
    lock.release()?;

    Ok(summarize(versions, rewritten_rows))
}

fn summarize(versions: TableVersions, rewritten_rows: usize) -> MigrationReport {
    MigrationReport {
        migrated_tables: versions.v1,
        unchanged_tables: versions.latest,
        rewritten_rows,
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        gluesql_core::{
            data::{Key, Value},
            store::Store,
        },
        std::mem,
        uuid::Uuid,
    };

    const V1_ROW: &str = "(\n    key: I64(1),\n    row: Vec([\n        I64(7),\n    ]),\n)";

    fn v1_storage(name: &str) -> MigrationPaths {
        let _ = fs::create_dir_all("tmp");
        let storage = format!("tmp/{name}-{}", Uuid::now_v7());

        let handle = FileStorage::new(&storage).expect("FileStorage::new");
        fs::write(
            handle.path("Foo").with_extension("sql"),
            "CREATE TABLE Foo;",
        )
        .expect("write v1 schema");
        fs::create_dir_all(handle.path("Foo")).expect("create table directory");
        fs::write(
            handle.data_path("Foo", &Key::I64(1)).expect("row path"),
            V1_ROW,
        )
        .expect("write v1 row");

        MigrationPaths::new(Path::new(&storage)).expect("migration paths")
    }

    fn assert_recovered(paths: &MigrationPaths) {
        assert!(paths.storage.is_dir(), "storage must be authoritative");
        assert!(!paths.staging.exists(), "staging must be gone");
        assert!(!paths.backup.exists(), "backup must be gone");
        assert!(!paths.lock.exists(), "lock must be removed last");

        let storage = FileStorage::new(&paths.storage).expect("open recovered storage");
        let row = storage
            .fetch_data("Foo", &Key::I64(1))
            .expect("fetch data")
            .expect("row exists");
        assert_eq!(row, vec![Value::I64(7)]);
    }

    fn cleanup(paths: &MigrationPaths) {
        let _ = fs::remove_dir_all(&paths.storage);
        let _ = fs::remove_dir_all(&paths.staging);
        let _ = fs::remove_dir_all(&paths.backup);
        let _ = fs::remove_file(&paths.lock);
    }

    #[test]
    fn an_interrupted_build_is_reported_not_discarded() {
        let paths = v1_storage("real-interrupted-build");
        let mut lock = MigrationLock::create(&paths.lock).expect("create lock");
        lock.record_staging().expect("record staging");
        staging::build(&paths.storage, &paths.staging, v1_to_v2::decode_row)
            .expect("build staging");
        mem::forget(lock);

        assert!(paths.storage.is_dir() && paths.staging.is_dir() && !paths.backup.exists());

        let err = migrate_to_latest(&paths.storage).expect_err("must not discard staging");
        assert!(err.to_string().contains("interrupted migration"));
        assert!(paths.staging.is_dir(), "staging must be preserved");
        assert!(paths.lock.exists(), "the lock must be preserved");
        assert!(paths.storage.join("Foo.sql").exists());

        cleanup(&paths);
    }

    #[test]
    fn recovers_from_a_real_interruption_between_the_two_renames() {
        let paths = v1_storage("real-interrupted-cutover");
        let mut lock = MigrationLock::create(&paths.lock).expect("create lock");
        lock.record_staging().expect("record staging");
        staging::build(&paths.storage, &paths.staging, v1_to_v2::decode_row)
            .expect("build staging");
        lock.begin_cutover().expect("begin cutover");
        fs::rename(&paths.storage, &paths.backup).expect("first cutover rename");
        mem::forget(lock);

        assert!(!paths.storage.exists() && paths.staging.is_dir() && paths.backup.is_dir());

        let report = migrate_to_latest(&paths.storage).expect("recover");
        assert_eq!(report.migrated_tables, 0);
        assert_eq!(report.unchanged_tables, 1);
        assert_recovered(&paths);

        cleanup(&paths);
    }

    #[test]
    fn recovers_from_a_real_interruption_after_the_second_rename() {
        let paths = v1_storage("real-interrupted-cleanup");
        let mut lock = MigrationLock::create(&paths.lock).expect("create lock");
        lock.record_staging().expect("record staging");
        staging::build(&paths.storage, &paths.staging, v1_to_v2::decode_row)
            .expect("build staging");
        lock.begin_cutover().expect("begin cutover");
        fs::rename(&paths.storage, &paths.backup).expect("first cutover rename");
        fs::rename(&paths.staging, &paths.storage).expect("second cutover rename");
        mem::forget(lock);

        assert!(paths.storage.is_dir() && !paths.staging.exists() && paths.backup.is_dir());

        let report = migrate_to_latest(&paths.storage).expect("recover");
        assert_eq!(report.unchanged_tables, 1);
        assert_recovered(&paths);

        cleanup(&paths);
    }

    #[test]
    fn recovers_from_a_real_interruption_before_the_lock_was_removed() {
        let paths = v1_storage("real-interrupted-lock-cleanup");
        let mut lock = MigrationLock::create(&paths.lock).expect("create lock");
        lock.record_staging().expect("record staging");
        staging::build(&paths.storage, &paths.staging, v1_to_v2::decode_row)
            .expect("build staging");
        lock.begin_cutover().expect("begin cutover");
        fs::rename(&paths.storage, &paths.backup).expect("first cutover rename");
        fs::rename(&paths.staging, &paths.storage).expect("second cutover rename");
        fs::remove_dir_all(&paths.backup).expect("remove backup");
        mem::forget(lock);

        assert!(paths.storage.is_dir() && !paths.staging.exists() && !paths.backup.exists());

        let report = migrate_to_latest(&paths.storage).expect("recover");
        assert_eq!(report.unchanged_tables, 1);
        assert_recovered(&paths);

        cleanup(&paths);
    }

    #[test]
    fn a_user_directory_at_the_backup_path_is_never_removed() {
        let paths = v1_storage("user-backup-preserved");
        let lock = MigrationLock::create(&paths.lock).expect("create lock");
        mem::forget(lock);

        fs::create_dir_all(&paths.backup).expect("create user backup");
        fs::write(paths.backup.join("mine.txt"), "hand made").expect("write user file");

        let err = migrate_to_latest(&paths.storage).expect_err("must refuse");
        assert!(err.to_string().contains("interrupted migration"));
        assert_eq!(
            fs::read_to_string(paths.backup.join("mine.txt")).expect("read user file"),
            "hand made"
        );

        cleanup(&paths);
    }

    #[test]
    fn a_failed_build_does_not_strand_the_lock() {
        let paths = v1_storage("failed-build-releases-lock");
        fs::write(
            paths.storage.join("Foo").join("00010000000000000002.ron"),
            "this is not ron",
        )
        .expect("write invalid row");

        let err = migrate_to_latest(&paths.storage).expect_err("build must fail");
        assert!(err.to_string().contains("failed to parse v1 row file"));
        assert!(!paths.lock.exists(), "the lock must not be stranded");
        assert!(!paths.staging.exists(), "staging must be cleaned up");

        cleanup(&paths);
    }
}
