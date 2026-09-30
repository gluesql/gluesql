//! A state is the lock record and which of the three directories exist — never
//! what the storage holds, so having nothing to migrate only keeps `run` at `Idle`.

use {
    super::{
        lock::{LockPhase, MigrationLock},
        paths::MigrationPaths,
        staging::{self, DecodeRow},
    },
    gluesql_core::error::{Error, Result},
    std::fs,
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
    if !paths.lock.exists() {
        // Without a lock the sibling paths belong to whoever made them.
        paths.ensure_siblings_available()?;

        return Ok(MigrationState::Idle);
    }

    // Read first: a stale phase reads earlier than it is, and earlier phases refuse.
    let phase = MigrationLock::peek(&paths.lock)?.phase;

    match (
        phase,
        paths.storage.exists(),
        paths.backup.exists(),
        paths.staging.exists(),
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

/// Walks the map from `Idle` back to `Idle`, and reports how many rows it rewrote.
pub(super) fn run(paths: &MigrationPaths, decode: DecodeRow) -> Result<usize> {
    let mut migration = Migration {
        paths,
        decode: Some(decode),
        lock: None,
        rewritten_rows: 0,
        deferred: None,
    };
    migration.drive(MigrationState::Idle)?;

    Ok(migration.rewritten_rows)
}

/// Joins the map at the state found on disk and takes the same steps home.
///
/// `Building` and `ReadyToCutover` are refused: std cannot tell a crashed owner
/// from a live one, and discarding a live build's staging copy would let two
/// migrations publish over each other.
pub(super) fn finish_interrupted(paths: &MigrationPaths, state: MigrationState) -> Result<()> {
    match state {
        MigrationState::Idle => Ok(()),
        MigrationState::Building | MigrationState::ReadyToCutover => {
            Err(paths.interrupted_migration_error())
        }
        state => Migration {
            paths,
            decode: None,
            lock: Some(MigrationLock::resume(&paths.lock)?),
            rewritten_rows: 0,
            deferred: None,
        }
        .drive(state),
    }
}

struct Migration<'a> {
    paths: &'a MigrationPaths,
    /// Absent for recovery, which refuses to enter `Building` at all.
    decode: Option<DecodeRow>,
    lock: Option<MigrationLock>,
    rewritten_rows: usize,
    deferred: Option<Error>,
}

impl Migration<'_> {
    fn drive(&mut self, from: MigrationState) -> Result<()> {
        let mut state = self.step(from)?;
        while state != MigrationState::Idle {
            state = self.step(state)?;
        }

        match self.deferred.take() {
            Some(err) => Err(err),
            None => Ok(()),
        }
    }

    /// One step per state, taken by the normal run and by recovery alike.
    fn step(&mut self, state: MigrationState) -> Result<MigrationState> {
        let paths = self.paths;

        match state {
            MigrationState::Idle => {
                paths.ensure_renamable_root()?;
                self.lock = Some(MigrationLock::create(&paths.lock)?);

                Ok(MigrationState::Building)
            }
            MigrationState::Building => {
                self.rewritten_rows = self.build()?;
                self.lock()?.mark_ready()?;

                Ok(MigrationState::ReadyToCutover)
            }
            MigrationState::ReadyToCutover => {
                self.lock()?.rename(&paths.storage, &paths.backup)?;

                Ok(MigrationState::SourceMoved {
                    staging_present: true,
                })
            }
            MigrationState::SourceMoved {
                staging_present: true,
            } => {
                self.lock()?.rename(&paths.staging, &paths.storage)?;

                Ok(MigrationState::Published)
            }
            MigrationState::SourceMoved {
                staging_present: false,
            } => {
                self.lock()?.rename(&paths.backup, &paths.storage)?;

                Ok(MigrationState::CleanupPending)
            }
            MigrationState::Published => {
                self.discard_backup();

                Ok(MigrationState::CleanupPending)
            }
            MigrationState::CleanupPending => {
                self.lock
                    .take()
                    .ok_or_else(|| paths.inconsistent_state_error())?
                    .release()?;

                Ok(MigrationState::Idle)
            }
        }
    }

    fn build(&mut self) -> Result<usize> {
        let decode = self
            .decode
            .ok_or_else(|| self.paths.inconsistent_state_error())?;

        staging::build(&self.paths.storage, &self.paths.staging, decode).inspect_err(|_| {
            let _ = fs::remove_dir_all(&self.paths.staging);
        })
    }

    /// The new storage is published by now, so a backup that will not go away
    /// must not stop the last step from releasing the lock.
    fn discard_backup(&mut self) {
        let backup = self.paths.backup.clone();

        if let Err(err) = self.lock().and_then(|lock| lock.ensure_owned()) {
            self.deferred = Some(err);

            return;
        }

        if let Err(err) = fs::remove_dir_all(&backup) {
            self.deferred = Some(Error::StorageMsg(format!(
                "[FileStorage] the migration finished, but its backup '{}' could not be removed: {err}. The storage is complete and can be opened; remove the backup by hand.",
                backup.display()
            )));
        }
    }

    fn lock(&mut self) -> Result<&mut MigrationLock> {
        let paths = self.paths;

        self.lock
            .as_mut()
            .ok_or_else(|| paths.inconsistent_state_error())
    }
}

#[cfg(test)]
mod tests {
    use {super::*, crate::FileStorage, std::fs, uuid::Uuid};

    /// Both paths share these steps, so each must leave the state it claims.
    #[test]
    fn each_step_leaves_the_state_it_returns() {
        let cases: [(&str, &[&str], MigrationState, MigrationState); 6] = [
            (
                "",
                &["storage"],
                MigrationState::Idle,
                MigrationState::Building,
            ),
            (
                "ready",
                &["storage", "staging"],
                MigrationState::ReadyToCutover,
                MigrationState::SourceMoved {
                    staging_present: true,
                },
            ),
            (
                "ready",
                &["backup", "staging"],
                MigrationState::SourceMoved {
                    staging_present: true,
                },
                MigrationState::Published,
            ),
            (
                "ready",
                &["backup"],
                MigrationState::SourceMoved {
                    staging_present: false,
                },
                MigrationState::CleanupPending,
            ),
            (
                "ready",
                &["storage", "backup"],
                MigrationState::Published,
                MigrationState::CleanupPending,
            ),
            (
                "ready",
                &["storage"],
                MigrationState::CleanupPending,
                MigrationState::Idle,
            ),
        ];

        for (phase, present, from, expected) in cases {
            let paths = situation(phase, present);
            let lock = (!phase.is_empty())
                .then(|| MigrationLock::resume(&paths.lock).expect("resume the lock"));
            let mut migration = Migration {
                paths: &paths,
                decode: None,
                lock,
                rewritten_rows: 0,
                deferred: None,
            };

            assert_eq!(
                migration.step(from).expect("step"),
                expected,
                "{from:?} should step to {expected:?}"
            );
            assert_eq!(
                inspect(&paths).ok(),
                Some(expected),
                "after {from:?} the disk should read as {expected:?}"
            );

            drop(migration);
            let _ = fs::remove_dir_all(&paths.storage);
            let _ = fs::remove_dir_all(&paths.staging);
            let _ = fs::remove_dir_all(&paths.backup);
            let _ = fs::remove_file(&paths.lock);
        }
    }

    /// The one step the table test cannot set up by hand, since it needs rows to
    /// stage and a decoder to read them with.
    #[test]
    fn building_steps_to_ready_to_cutover() {
        let paths = situation("", &["storage"]);
        fs::write(paths.storage.join("Foo.sql"), "CREATE TABLE Foo;").expect("write v1 schema");
        fs::create_dir_all(paths.storage.join("Foo")).expect("create table directory");
        fs::write(
            paths.storage.join("Foo").join("00010000000000000001.ron"),
            "(\n    key: I64(1),\n    row: Vec([\n        I64(7),\n    ]),\n)",
        )
        .expect("write v1 row");

        let mut migration = Migration {
            paths: &paths,
            decode: Some(super::super::v1_to_v2::decode_row),
            lock: None,
            rewritten_rows: 0,
            deferred: None,
        };

        assert_eq!(
            migration
                .step(MigrationState::Idle)
                .expect("step out of Idle"),
            MigrationState::Building
        );
        assert_eq!(
            migration
                .step(MigrationState::Building)
                .expect("step out of Building"),
            MigrationState::ReadyToCutover
        );
        assert_eq!(migration.rewritten_rows, 1, "the v1 row must be rewritten");
        assert_eq!(
            inspect(&paths).ok(),
            Some(MigrationState::ReadyToCutover),
            "the disk should read as ReadyToCutover"
        );

        drop(migration);
        let _ = fs::remove_dir_all(&paths.storage);
        let _ = fs::remove_dir_all(&paths.staging);
        let _ = fs::remove_file(&paths.lock);
    }

    /// Once the cutover has begun, a failed step must leave the lock behind.
    #[test]
    fn a_step_that_fails_mid_cutover_keeps_the_lock() {
        let paths = situation("ready", &["storage", "staging", "backup"]);
        fs::write(paths.backup.join("occupied"), "not empty").expect("occupy the backup");

        let mut migration = Migration {
            paths: &paths,
            decode: None,
            lock: Some(MigrationLock::resume(&paths.lock).expect("resume the lock")),
            rewritten_rows: 0,
            deferred: None,
        };

        assert!(
            migration.step(MigrationState::ReadyToCutover).is_err(),
            "renaming onto a non-empty backup must fail"
        );

        drop(migration);
        assert!(
            paths.lock.exists(),
            "the lock must survive a failed cutover"
        );
        assert!(paths.storage.exists(), "the source must be untouched");

        let _ = fs::remove_dir_all(&paths.storage);
        let _ = fs::remove_dir_all(&paths.staging);
        let _ = fs::remove_dir_all(&paths.backup);
        let _ = fs::remove_file(&paths.lock);
    }

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
