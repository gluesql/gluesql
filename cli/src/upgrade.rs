use {
    crate::Storage,
    anyhow::{Result, bail},
    gluesql_file_storage::migrate_to_latest as migrate_file_storage_to_latest,
    gluesql_redb_storage::migrate_to_latest as migrate_redb_storage_to_latest,
    gluesql_sled_storage::migrate_to_latest as migrate_sled_storage_to_latest,
    std::path::Path,
};

pub(super) fn run_upgrade(
    path: Option<&Path>,
    storage: Option<Storage>,
    has_execute: bool,
    has_dump: bool,
) -> Result<()> {
    if has_execute || has_dump {
        bail!("--upgrade cannot be used with --execute or --dump");
    }

    let (Some(path), Some(storage)) = (path, storage) else {
        bail!("both --path and --storage should be specified with --upgrade");
    };

    match storage {
        Storage::Sled => {
            let report = migrate_sled_storage_to_latest(path)?;
            print_upgrade_report(
                "sled",
                path,
                report.migrated_tables,
                report.unchanged_tables,
                report.rewritten_rows,
            );
        }
        Storage::Redb => {
            let report = migrate_redb_storage_to_latest(path)?;
            print_upgrade_report(
                "redb",
                path,
                report.migrated_tables,
                report.unchanged_tables,
                report.rewritten_rows,
            );
        }
        Storage::File => {
            let report = migrate_file_storage_to_latest(path)?;
            print_upgrade_report(
                "file",
                path,
                report.migrated_tables,
                report.unchanged_tables,
                report.rewritten_rows,
            );
        }
        _ => {
            bail!("--upgrade is supported only for storage types: sled, redb, file");
        }
    }

    Ok(())
}

fn print_upgrade_report(
    storage_name: &str,
    path: &Path,
    migrated_tables: usize,
    unchanged_tables: usize,
    rewritten_rows: usize,
) {
    println!("[{storage_name}-storage] upgraded {}", path.display());
    println!(
        "[{storage_name}-storage] migration report: migrated_tables={migrated_tables}, unchanged_tables={unchanged_tables}, rewritten_rows={rewritten_rows}",
    );
}

#[cfg(test)]
mod tests {
    use {
        super::run_upgrade,
        crate::Storage,
        gluesql_file_storage::FileStorage,
        gluesql_redb_storage::RedbStorage,
        gluesql_sled_storage::SledStorage,
        std::{
            fs,
            path::{Path, PathBuf},
            time::{SystemTime, UNIX_EPOCH},
        },
    };

    fn test_path(name: &str) -> PathBuf {
        let suffix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time")
            .as_nanos();

        std::env::temp_dir().join(format!("gluesql-cli-upgrade-{name}-{suffix}"))
    }

    #[test]
    fn upgrade_rejects_unsupported_storage() {
        let actual = run_upgrade(Some(Path::new("./tmp")), Some(Storage::Json), false, false);
        let expected = "--upgrade is supported only for storage types: sled, redb, file";

        assert_eq!(
            actual
                .expect_err("unsupported storage should fail")
                .to_string(),
            expected
        );
    }

    #[test]
    fn upgrade_rejects_execute_or_dump() {
        let actual = run_upgrade(Some(Path::new("./tmp")), Some(Storage::Sled), true, false);
        let expected = "--upgrade cannot be used with --execute or --dump";

        assert_eq!(
            actual.expect_err("execute should conflict").to_string(),
            expected
        );
    }

    #[test]
    fn upgrade_requires_path_and_storage() {
        let actual = run_upgrade(None, Some(Storage::Sled), false, false);
        let expected = "both --path and --storage should be specified with --upgrade";

        assert_eq!(
            actual.expect_err("missing path should fail").to_string(),
            expected
        );
    }

    #[test]
    fn upgrade_accepts_sled_storage() {
        let path = test_path("sled");
        SledStorage::new(&path).expect("failed to initialize sled storage for upgrade test");

        let actual = run_upgrade(Some(path.as_path()), Some(Storage::Sled), false, false);

        assert!(actual.is_ok(), "sled upgrade should succeed: {actual:?}");
        let _ = fs::remove_dir_all(path);
    }

    #[test]
    fn upgrade_accepts_redb_storage() {
        let path = test_path("redb.db");
        RedbStorage::new(&path).expect("failed to initialize redb storage for upgrade test");

        let actual = run_upgrade(Some(path.as_path()), Some(Storage::Redb), false, false);

        assert!(actual.is_ok(), "redb upgrade should succeed: {actual:?}");
        let _ = fs::remove_file(path);
    }

    #[test]
    fn upgrade_accepts_file_storage() {
        let path = test_path("file");
        FileStorage::new(&path).expect("failed to initialize file storage for upgrade test");

        let actual = run_upgrade(Some(path.as_path()), Some(Storage::File), false, false);

        assert!(actual.is_ok(), "file upgrade should succeed: {actual:?}");
        let _ = fs::remove_dir_all(path);
    }
}
