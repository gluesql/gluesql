use {
    super::{
        lock::{LockPhase, MigrationLock},
        paths::MigrationPaths,
    },
    gluesql_core::error::Result,
};

/// Where a migration has got to, from the lock record and the directories that exist.
/// `db/` is authoritative in every state but `SourceMoved`, where it is absent and the backup is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum MigrationState {
    Idle,
    Building,
    ReadyToCutover,
    SourceMoved { staging_present: bool },
    Published,
    CleanupPending,
}

pub(super) fn inspect(paths: &MigrationPaths) -> Result<MigrationState> {
    let storage = paths.storage.exists();
    let backup = paths.backup.exists();
    let staging = paths.staging.exists();

    if !paths.lock.exists() {
        // Without a lock the sibling paths belong to whoever made them.
        paths.ensure_siblings_available()?;

        return Ok(MigrationState::Idle);
    }

    match (
        MigrationLock::peek(&paths.lock)?.phase,
        storage,
        backup,
        staging,
    ) {
        (LockPhase::Building, true, false, _) => Ok(MigrationState::Building),
        (LockPhase::Ready, true, false, true) => Ok(MigrationState::ReadyToCutover),
        (LockPhase::Ready, false, true, staging_present) => {
            Ok(MigrationState::SourceMoved { staging_present })
        }
        (LockPhase::Ready, true, true, false) => Ok(MigrationState::Published),
        (LockPhase::Ready, true, false, false) => Ok(MigrationState::CleanupPending),
        _ => Err(paths.inconsistent_state_error()),
    }
}

/// `Building` and `ReadyToCutover` are refused rather than repaired: std cannot
/// tell a crashed owner from a live one, and discarding a live build's staging
/// copy would let two migrations publish over each other.
pub(super) fn finish_interrupted(paths: &MigrationPaths, state: MigrationState) -> Result<()> {
    match state {
        MigrationState::Idle => Ok(()),
        MigrationState::Building | MigrationState::ReadyToCutover => {
            Err(paths.interrupted_migration_error())
        }
        MigrationState::SourceMoved {
            staging_present: true,
        } => {
            let lock = MigrationLock::resume(&paths.lock)?;
            lock.rename(&paths.staging, &paths.storage)?;
            lock.remove_dir_all(&paths.backup)?;

            lock.release()
        }
        MigrationState::SourceMoved {
            staging_present: false,
        } => {
            let lock = MigrationLock::resume(&paths.lock)?;
            lock.rename(&paths.backup, &paths.storage)?;

            lock.release()
        }
        MigrationState::Published => {
            let lock = MigrationLock::resume(&paths.lock)?;
            lock.remove_dir_all(&paths.backup)?;

            lock.release()
        }
        MigrationState::CleanupPending => MigrationLock::resume(&paths.lock)?.release(),
    }
}

#[cfg(test)]
mod tests {
    use {super::*, crate::FileStorage, std::fs, uuid::Uuid};

    #[test]
    fn every_reachable_situation_maps_to_one_state() {
        let cases: [(&str, &[&str], Option<MigrationState>); 12] = [
            // no lock
            ("", &["storage"], Some(MigrationState::Idle)),
            ("", &["storage", "staging"], None),
            ("", &["storage", "backup"], None),
            // building
            ("building", &["storage"], Some(MigrationState::Building)),
            (
                "building",
                &["storage", "staging"],
                Some(MigrationState::Building),
            ),
            ("building", &["storage", "backup"], None),
            ("building", &["backup", "staging"], None),
            // ready
            (
                "ready",
                &["storage", "staging"],
                Some(MigrationState::ReadyToCutover),
            ),
            (
                "ready",
                &["backup", "staging"],
                Some(MigrationState::SourceMoved {
                    staging_present: true,
                }),
            ),
            (
                "ready",
                &["backup"],
                Some(MigrationState::SourceMoved {
                    staging_present: false,
                }),
            ),
            (
                "ready",
                &["storage", "backup"],
                Some(MigrationState::Published),
            ),
            ("ready", &["storage"], Some(MigrationState::CleanupPending)),
        ];

        for (phase, present, expected) in cases {
            let paths = situation(phase, present);
            let actual = inspect(&paths).ok();

            assert_eq!(
                actual, expected,
                "lock {phase:?} with {present:?} should map to {expected:?}"
            );

            let _ = fs::remove_dir_all(&paths.storage);
            let _ = fs::remove_dir_all(&paths.staging);
            let _ = fs::remove_dir_all(&paths.backup);
            let _ = fs::remove_file(&paths.lock);
        }
    }

    fn situation(phase: &str, present: &[&str]) -> MigrationPaths {
        let _ = fs::create_dir_all("tmp");
        let root = format!("tmp/state-{}", Uuid::now_v7());
        let paths = MigrationPaths::new(std::path::Path::new(&root)).expect("paths");

        for name in present {
            let dir = match *name {
                "storage" => &paths.storage,
                "staging" => &paths.staging,
                "backup" => &paths.backup,
                other => panic!("unknown directory {other}"),
            };
            FileStorage::new(dir).expect("create directory");
        }
        if !phase.is_empty() {
            fs::write(&paths.lock, format!("{phase}\n")).expect("write lock");
        }

        paths
    }
}
