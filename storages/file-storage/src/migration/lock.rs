use {
    crate::ResultExt,
    gluesql_core::error::{Error, Result},
    std::{
        fs,
        io::{ErrorKind, Write},
        path::{Path, PathBuf},
    },
    uuid::Uuid,
};

const NONCE_PREFIX: &str = "owner=";
const BUILDING: &str = "building";
const STAGED: &str = "staged";
const OWNS_STAGING: &str = "owns-staging";
const OWNS_BACKUP: &str = "owns-backup";

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(super) enum LockPhase {
    #[default]
    Building,
    Staged,
}

/// Written before the directory it describes is created, so it is always a
/// superset of what is on disk.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(super) struct Ownership {
    pub(super) staging: bool,
    pub(super) backup: bool,
    nonce: u128,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(super) struct LockState {
    pub(super) phase: LockPhase,
    pub(super) owns: Ownership,
}

impl LockState {
    fn parse(data: &str) -> Self {
        data.lines()
            .map(str::trim)
            .fold(Self::default(), |state, line| match line {
                STAGED => Self {
                    phase: LockPhase::Staged,
                    ..state
                },
                OWNS_STAGING => Self {
                    owns: Ownership {
                        staging: true,
                        ..state.owns
                    },
                    ..state
                },
                OWNS_BACKUP => Self {
                    owns: Ownership {
                        backup: true,
                        ..state.owns
                    },
                    ..state
                },
                line => match line
                    .strip_prefix(NONCE_PREFIX)
                    .map(|nonce| u128::from_str_radix(nonce, 16))
                {
                    Some(Ok(nonce)) => Self {
                        owns: Ownership {
                            nonce,
                            ..state.owns
                        },
                        ..state
                    },
                    _ => state,
                },
            })
    }

    fn render(self) -> String {
        let phase = match self.phase {
            LockPhase::Building => BUILDING,
            LockPhase::Staged => STAGED,
        };
        let staging = if self.owns.staging { OWNS_STAGING } else { "" };
        let backup = if self.owns.backup { OWNS_BACKUP } else { "" };
        let nonce = self.owns.nonce;

        format!("{phase}\n{staging}\n{backup}\n{NONCE_PREFIX}{nonce:032x}\n")
    }
}

/// Carries no liveness information: recovery is decided by which directories
/// exist, never by how old the lock is.
#[derive(Debug)]
pub(super) struct MigrationLock {
    path: PathBuf,
    state: LockState,
    keep_on_drop: bool,
}

impl MigrationLock {
    pub(super) fn create(path: &Path) -> Result<Self> {
        let file = fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(path)
            .map_err(|err| match err.kind() {
                ErrorKind::AlreadyExists => Error::StorageMsg(format!(
                    "[FileStorage] another migration already holds the lock '{}'",
                    path.display()
                )),
                _ => Error::StorageMsg(err.to_string()),
            })?;
        let lock = Self {
            path: path.to_owned(),
            state: LockState {
                owns: Ownership {
                    nonce: Uuid::now_v7().as_u128(),
                    ..Ownership::default()
                },
                ..LockState::default()
            },
            keep_on_drop: false,
        };
        write_state(file, lock.state)?;

        Ok(lock)
    }

    pub(super) fn peek(path: &Path) -> Result<LockState> {
        fs::read_to_string(path)
            .map_storage_err()
            .map(|data| LockState::parse(&data))
    }

    pub(super) fn resume(path: &Path) -> Result<Self> {
        let previous = Self::peek(path)?;
        let lock = Self {
            path: path.to_owned(),
            state: LockState {
                owns: Ownership {
                    nonce: Uuid::now_v7().as_u128(),
                    ..previous.owns
                },
                ..previous
            },
            keep_on_drop: true,
        };
        lock.persist()?;

        Ok(lock)
    }

    pub(super) fn ensure_owned(&self) -> Result<()> {
        let owned = Self::peek(&self.path)
            .map(|state| state.owns.nonce == self.state.owns.nonce)
            .unwrap_or(false);

        if owned {
            return Ok(());
        }

        Err(Error::StorageMsg(format!(
            "[FileStorage] this migration no longer owns the lock '{}'; another migration or recovery took it over, so nothing further was changed",
            self.path.display()
        )))
    }

    pub(super) fn record_staging(&mut self) -> Result<()> {
        self.state.owns.staging = true;

        self.persist()
    }

    pub(super) fn begin_cutover(&mut self) -> Result<()> {
        self.state.phase = LockPhase::Staged;
        self.state.owns.backup = true;
        self.keep_on_drop = true;

        self.persist()
    }

    pub(super) fn rename(&self, from: &Path, to: &Path) -> Result<()> {
        self.ensure_owned()?;

        fs::rename(from, to).map_storage_err()
    }

    pub(super) fn remove_dir_all(&self, path: &Path) -> Result<()> {
        self.ensure_owned()?;

        fs::remove_dir_all(path).map_storage_err()
    }

    pub(super) fn release(mut self) -> Result<()> {
        self.ensure_owned()?;
        self.keep_on_drop = false;

        fs::remove_file(&self.path).map_storage_err()
    }

    fn persist(&self) -> Result<()> {
        let file = fs::File::create(&self.path).map_storage_err()?;

        write_state(file, self.state)
    }
}

impl Drop for MigrationLock {
    fn drop(&mut self) {
        if !self.keep_on_drop && self.ensure_owned().is_ok() {
            let _ = fs::remove_file(&self.path);
        }
    }
}

fn write_state(mut file: fs::File, state: LockState) -> Result<()> {
    file.write_all(state.render().as_bytes())
        .map_storage_err()?;
    file.sync_all().map_storage_err()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lock_path(name: &str) -> PathBuf {
        let _ = fs::create_dir_all("tmp");

        PathBuf::from(format!("tmp/{name}-{}.migration-lock", Uuid::now_v7()))
    }

    #[test]
    fn a_second_create_is_always_rejected() {
        let path = lock_path("second-create");
        let first = MigrationLock::create(&path).expect("first create");

        assert!(
            MigrationLock::create(&path).is_err(),
            "a lock must never be claimed twice"
        );
        assert!(
            path.exists(),
            "the rejected create must not remove the lock"
        );

        first.release().expect("release");
        assert!(!path.exists());
    }

    #[test]
    fn dropping_a_build_lock_removes_it_but_a_cutover_lock_survives() {
        let path = lock_path("drop-behaviour");
        drop(MigrationLock::create(&path).expect("create"));
        assert!(
            !path.exists(),
            "an abandoned build must not strand its lock"
        );

        let mut lock = MigrationLock::create(&path).expect("create");
        lock.begin_cutover().expect("begin cutover");
        drop(lock);
        assert!(path.exists(), "an unfinished cutover must stay locked");

        MigrationLock::resume(&path)
            .expect("resume")
            .release()
            .expect("release");
        assert!(!path.exists());
    }

    #[test]
    fn ownership_and_phase_survive_a_resume() {
        let path = lock_path("state-roundtrip");
        let mut lock = MigrationLock::create(&path).expect("create");
        let fresh = MigrationLock::peek(&path).expect("peek");
        assert_eq!(fresh.phase, LockPhase::Building);
        assert!(!fresh.owns.staging && !fresh.owns.backup);

        lock.record_staging().expect("record staging");
        lock.begin_cutover().expect("begin cutover");
        drop(lock);

        let state = MigrationLock::peek(&path).expect("peek");
        assert_eq!(state.phase, LockPhase::Staged);
        assert!(state.owns.staging && state.owns.backup);

        MigrationLock::resume(&path)
            .expect("resume")
            .release()
            .expect("release");
    }

    #[test]
    fn an_unreadable_lock_claims_nothing() {
        let path = lock_path("unreadable-lock");
        fs::write(&path, "").expect("write empty lock");

        assert_eq!(
            MigrationLock::peek(&path).expect("peek"),
            LockState::default()
        );

        let taken_over = MigrationLock::resume(&path).expect("resume");
        let stale = MigrationLock::resume(&path).expect("resume again");
        assert!(
            taken_over.ensure_owned().is_err(),
            "the first owner lost the lock"
        );
        assert!(stale.ensure_owned().is_ok());
        stale.release().expect("release");

        let _ = fs::remove_file(&path);
    }
}
