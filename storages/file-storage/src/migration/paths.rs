use {
    crate::ResultExt,
    gluesql_core::error::{Error, Result},
    std::{
        ffi::OsStr,
        fs,
        path::{Path, PathBuf},
    },
};

/// Siblings of the storage root: it is absent between the two cutover
/// renames, and a sibling keeps every rename on one filesystem.
const LOCK_SUFFIX: &str = ".migration-lock";
const STAGING_SUFFIX: &str = ".migrating";
const BACKUP_SUFFIX: &str = ".backup";

#[derive(Debug)]
pub(super) struct MigrationPaths {
    pub(super) storage: PathBuf,
    pub(super) lock: PathBuf,
    pub(super) staging: PathBuf,
    pub(super) backup: PathBuf,
}

impl MigrationPaths {
    pub(super) fn new(storage: &Path) -> Result<Self> {
        Ok(Self {
            storage: storage.to_owned(),
            lock: sibling(storage, LOCK_SUFFIX)?,
            staging: sibling(storage, STAGING_SUFFIX)?,
            backup: sibling(storage, BACKUP_SUFFIX)?,
        })
    }

    pub(super) fn lock_of(storage: &Path) -> Result<PathBuf> {
        sibling(storage, LOCK_SUFFIX)
    }

    pub(super) fn layout(&self) -> Layout {
        Layout::detect(self)
    }

    pub(super) fn ensure_storage_dir(&self) -> Result<()> {
        if !self.storage.exists() {
            return Err(Error::StorageMsg(format!(
                "[FileStorage] storage path '{}' does not exist",
                self.storage.display()
            )));
        }
        if !self.storage.is_dir() {
            return Err(Error::StorageMsg(format!(
                "[FileStorage] storage path '{}' is not a directory",
                self.storage.display()
            )));
        }

        Ok(())
    }

    pub(super) fn ensure_renamable_root(&self) -> Result<()> {
        let is_symlink = self
            .storage
            .symlink_metadata()
            .map_storage_err()?
            .file_type()
            .is_symlink();

        if is_symlink {
            return Err(Error::StorageMsg(format!(
                "[FileStorage] storage path '{}' is a symbolic link; migration renames the storage directory, so run it against the resolved path instead",
                self.storage.display()
            )));
        }

        Ok(())
    }

    pub(super) fn ensure_siblings_available(&self) -> Result<()> {
        for sibling in [&self.staging, &self.backup] {
            if sibling.exists() {
                return Err(Error::StorageMsg(format!(
                    "[FileStorage] migration needs the sibling path '{}', but it already exists and no migration lock is present; move it aside, nothing was removed",
                    sibling.display()
                )));
            }
        }

        Ok(())
    }

    pub(super) fn interrupted_migration_error(&self) -> Error {
        Error::StorageMsg(format!(
            "[FileStorage] an interrupted migration was found for '{}': storage exists={}, backup '{}' exists={}, staging '{}' exists={}. Nothing was removed and the storage was not modified. Another migration may still be running; once you have confirmed that none is, remove the staging and backup directories left by that migration and then '{}', and run the migration again.",
            self.storage.display(),
            self.storage.exists(),
            self.backup.display(),
            self.backup.exists(),
            self.staging.display(),
            self.staging.exists(),
            self.lock.display(),
        ))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Layout {
    Settled,
    StagingBesideStorage,
    BackupBesideStorage,
    StorageRenamedAway { staging_present: bool },
    Unexpected,
}

impl Layout {
    fn detect(paths: &MigrationPaths) -> Self {
        match (
            paths.storage.exists(),
            paths.backup.exists(),
            paths.staging.exists(),
        ) {
            (true, false, false) => Self::Settled,
            (true, false, true) => Self::StagingBesideStorage,
            (true, true, false) => Self::BackupBesideStorage,
            (false, true, staging_present) => Self::StorageRenamedAway { staging_present },
            _ => Self::Unexpected,
        }
    }
}

fn sibling(path: &Path, suffix: &str) -> Result<PathBuf> {
    if let Some(name) = path.file_name().and_then(OsStr::to_str) {
        return Ok(path.with_file_name(format!("{name}{suffix}")));
    }

    let resolved = fs::canonicalize(path).map_storage_err()?;
    resolved
        .file_name()
        .and_then(OsStr::to_str)
        .map(|name| resolved.with_file_name(format!("{name}{suffix}")))
        .ok_or_else(|| {
            Error::StorageMsg(format!(
                "[FileStorage] storage path '{}' has no file name; migration needs sibling paths",
                path.display()
            ))
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_path_with_no_file_name_is_resolved_first() {
        let cwd = std::env::current_dir().expect("current dir");
        let name = cwd.file_name().and_then(OsStr::to_str).expect("cwd name");

        assert_eq!(
            sibling(Path::new("."), LOCK_SUFFIX).expect("sibling of ."),
            cwd.with_file_name(format!("{name}{LOCK_SUFFIX}"))
        );
    }

    #[test]
    fn layout_names_every_combination() {
        let _ = fs::create_dir_all("tmp");
        let root = PathBuf::from(format!("tmp/layout-{}", uuid::Uuid::now_v7()));
        let paths = MigrationPaths::new(&root).expect("paths");

        assert_eq!(paths.layout(), Layout::Unexpected);
        fs::create_dir_all(&paths.storage).expect("create storage");
        assert_eq!(paths.layout(), Layout::Settled);
        fs::create_dir_all(&paths.staging).expect("create staging");
        assert_eq!(paths.layout(), Layout::StagingBesideStorage);
        fs::create_dir_all(&paths.backup).expect("create backup");
        assert_eq!(paths.layout(), Layout::Unexpected);
        fs::remove_dir_all(&paths.staging).expect("remove staging");
        assert_eq!(paths.layout(), Layout::BackupBesideStorage);
        fs::remove_dir_all(&paths.storage).expect("remove storage");
        assert_eq!(
            paths.layout(),
            Layout::StorageRenamedAway {
                staging_present: false
            }
        );

        let _ = fs::remove_dir_all(&paths.backup);
    }
}
