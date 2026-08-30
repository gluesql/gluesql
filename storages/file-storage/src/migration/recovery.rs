use {
    super::{
        lock::{LockPhase, MigrationLock},
        paths::{Layout, MigrationPaths},
        schema_file,
    },
    gluesql_core::error::Result,
};

/// A staging directory beside an intact storage root is left alone on purpose:
/// std has no advisory lock and no portable liveness check, so a crashed owner
/// is indistinguishable from a running one, and discarding a live build's
/// staging would let two migrations publish over each other.
pub(super) fn finish_interrupted(paths: &MigrationPaths) -> Result<()> {
    let state = MigrationLock::peek(&paths.lock)?;

    match paths.layout() {
        Layout::Settled => {
            paths.ensure_storage_dir()?;
            schema_file::classify(&paths.storage)?;

            MigrationLock::resume(&paths.lock)?.release()
        }
        Layout::BackupBesideStorage if state.owns.backup => {
            let lock = MigrationLock::resume(&paths.lock)?;
            lock.remove_dir_all(&paths.backup)?;

            lock.release()
        }
        Layout::StorageRenamedAway { staging_present } if state.owns.backup => {
            let lock = MigrationLock::resume(&paths.lock)?;
            let complete_staging =
                staging_present && state.owns.staging && state.phase == LockPhase::Staged;

            if complete_staging {
                lock.rename(&paths.staging, &paths.storage)?;
            } else {
                lock.rename(&paths.backup, &paths.storage)?;
            }

            if paths.backup.exists() {
                lock.remove_dir_all(&paths.backup)?;
            }
            if paths.staging.exists() && state.owns.staging {
                lock.remove_dir_all(&paths.staging)?;
            }

            lock.release()
        }
        _ => Err(paths.interrupted_migration_error()),
    }
}
