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
    use {super::run_upgrade, crate::Storage, std::path::Path};

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
}
