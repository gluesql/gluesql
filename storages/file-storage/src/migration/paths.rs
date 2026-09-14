use {
    crate::ResultExt,
    gluesql_core::error::{Error, Result},
    std::{
        ffi::OsStr,
        fs,
        path::{Path, PathBuf},
    },
};

/// Siblings, not children: the storage root is absent between the two cutover
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
        let root = resolved_root(storage)?;
        let name = root
            .file_name()
            .and_then(OsStr::to_str)
            .ok_or_else(|| no_file_name_error(storage))?
            .to_owned();

        Ok(Self {
            storage: root.with_file_name(&name),
            lock: root.with_file_name(format!("{name}{LOCK_SUFFIX}")),
            staging: root.with_file_name(format!("{name}{STAGING_SUFFIX}")),
            backup: root.with_file_name(format!("{name}{BACKUP_SUFFIX}")),
        })
    }

    pub(super) fn lock_of(storage: &Path) -> Result<PathBuf> {
        Self::new(storage).map(|paths| paths.lock)
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

/// `.` and `..` carry no name to build siblings from, and a trailing slash makes
/// `symlink_metadata` follow a symlinked root instead of seeing it.
fn resolved_root(path: &Path) -> Result<PathBuf> {
    match path.file_name() {
        Some(_) => Ok(path.to_owned()),
        None => fs::canonicalize(path).map_storage_err(),
    }
}

fn no_file_name_error(path: &Path) -> Error {
    Error::StorageMsg(format!(
        "[FileStorage] storage path '{}' has no file name; migration needs sibling paths",
        path.display()
    ))
}

impl MigrationPaths {
    pub(super) fn inconsistent_state_error(&self) -> Error {
        Error::StorageMsg(format!(
            "[FileStorage] the migration state of '{}' is inconsistent: storage exists={}, backup '{}' exists={}, staging '{}' exists={}, lock '{}' exists={}. Nothing was removed and the storage was not modified; resolve it by hand.",
            self.storage.display(),
            self.storage.exists(),
            self.backup.display(),
            self.backup.exists(),
            self.staging.display(),
            self.staging.exists(),
            self.lock.display(),
            self.lock.exists(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_path_with_no_file_name_is_resolved_first() {
        let cwd = std::env::current_dir().expect("current dir");
        let name = cwd.file_name().and_then(OsStr::to_str).expect("cwd name");
        let paths = MigrationPaths::new(Path::new(".")).expect("paths of .");

        assert_eq!(paths.storage, cwd, "the root must be renamable");
        assert_eq!(
            paths.lock,
            cwd.with_file_name(format!("{name}{LOCK_SUFFIX}"))
        );
    }

    #[test]
    fn a_trailing_slash_still_leaves_a_symlinked_root_detectable() {
        let paths = MigrationPaths::new(Path::new("tmp/some-db/")).expect("paths");

        assert_eq!(paths.storage, Path::new("tmp/some-db"));
        assert_eq!(paths.backup, Path::new("tmp/some-db.backup"));
    }
}
