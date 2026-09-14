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
const READY: &str = "ready";

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(super) enum LockPhase {
    #[default]
    Building,
    Ready,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(super) struct LockRecord {
    pub(super) phase: LockPhase,
    nonce: u128,
}

impl LockRecord {
    fn parse(data: &str) -> Self {
        data.lines()
            .map(str::trim)
            .fold(Self::default(), |record, line| match line {
                READY => Self {
                    phase: LockPhase::Ready,
                    ..record
                },
                line => match line
                    .strip_prefix(NONCE_PREFIX)
                    .map(|nonce| u128::from_str_radix(nonce, 16))
                {
                    Some(Ok(nonce)) => Self { nonce, ..record },
                    _ => record,
                },
            })
    }

    fn render(self) -> String {
        let phase = match self.phase {
            LockPhase::Building => BUILDING,
            LockPhase::Ready => READY,
        };
        let nonce = self.nonce;

        format!("{phase}\n{NONCE_PREFIX}{nonce:032x}\n")
    }
}

/// Carries no liveness information: recovery reads the state, never the age.
#[derive(Debug)]
pub(super) struct MigrationLock {
    path: PathBuf,
    record: LockRecord,
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
            record: LockRecord {
                nonce: Uuid::now_v7().as_u128(),
                ..LockRecord::default()
            },
            keep_on_drop: false,
        };
        write_record(file, lock.record)?;

        Ok(lock)
    }

    pub(super) fn peek(path: &Path) -> Result<LockRecord> {
        fs::read_to_string(path)
            .map_storage_err()
            .map(|data| LockRecord::parse(&data))
    }

    pub(super) fn resume(path: &Path) -> Result<Self> {
        let previous = Self::peek(path)?;
        let lock = Self {
            path: path.to_owned(),
            record: LockRecord {
                nonce: Uuid::now_v7().as_u128(),
                ..previous
            },
            keep_on_drop: true,
        };
        lock.persist()?;

        Ok(lock)
    }

    pub(super) fn ensure_owned(&self) -> Result<()> {
        let owned = Self::peek(&self.path)
            .map(|record| record.nonce == self.record.nonce)
            .unwrap_or(false);

        if owned {
            return Ok(());
        }

        Err(Error::StorageMsg(format!(
            "[FileStorage] this migration no longer owns the lock '{}'; another migration or recovery took it over, so nothing further was changed",
            self.path.display()
        )))
    }

    /// Must precede the first cutover rename, which makes the storage root vanish.
    pub(super) fn mark_ready(&mut self) -> Result<()> {
        self.record.phase = LockPhase::Ready;
        self.keep_on_drop = true;

        self.persist()
    }

    pub(super) fn rename(&self, from: &Path, to: &Path) -> Result<()> {
        self.ensure_owned()?;

        fs::rename(from, to).map_storage_err()
    }

    /// The cutover has already published the new storage, so a backup that cannot
    /// be removed must not keep the lock and shut the caller out of intact data.
    pub(super) fn discard_backup(self, backup: &Path) -> Result<()> {
        self.ensure_owned()?;
        let removed = fs::remove_dir_all(backup);
        self.release()?;

        removed.map_err(|err| {
            Error::StorageMsg(format!(
                "[FileStorage] the migration finished, but its backup '{}' could not be removed: {err}. The storage is complete and can be opened; remove the backup by hand.",
                backup.display()
            ))
        })
    }

    pub(super) fn release(mut self) -> Result<()> {
        self.ensure_owned()?;
        self.keep_on_drop = false;

        fs::remove_file(&self.path).map_storage_err()
    }

    fn persist(&self) -> Result<()> {
        let file = fs::File::create(&self.path).map_storage_err()?;

        write_record(file, self.record)
    }
}

impl Drop for MigrationLock {
    fn drop(&mut self) {
        if !self.keep_on_drop && self.ensure_owned().is_ok() {
            let _ = fs::remove_file(&self.path);
        }
    }
}

fn write_record(mut file: fs::File, record: LockRecord) -> Result<()> {
    file.write_all(record.render().as_bytes())
        .map_storage_err()?;
    file.sync_all().map_storage_err()
}

#[cfg(test)]
mod tests {
    use {super::*, uuid::Uuid};

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
    fn dropping_a_build_lock_removes_it_but_a_ready_lock_survives() {
        let path = lock_path("drop-behaviour");
        drop(MigrationLock::create(&path).expect("create"));
        assert!(
            !path.exists(),
            "an abandoned build must not strand its lock"
        );

        let mut lock = MigrationLock::create(&path).expect("create");
        lock.mark_ready().expect("mark ready");
        drop(lock);
        assert!(path.exists(), "an unfinished cutover must stay locked");

        MigrationLock::resume(&path)
            .expect("resume")
            .release()
            .expect("release");
        assert!(!path.exists());
    }

    #[test]
    fn the_phase_survives_a_resume() {
        let path = lock_path("phase-roundtrip");
        let mut lock = MigrationLock::create(&path).expect("create");
        assert_eq!(
            MigrationLock::peek(&path).expect("peek").phase,
            LockPhase::Building
        );

        lock.mark_ready().expect("mark ready");
        drop(lock);
        assert_eq!(
            MigrationLock::peek(&path).expect("peek").phase,
            LockPhase::Ready
        );

        MigrationLock::resume(&path)
            .expect("resume")
            .release()
            .expect("release");
    }

    #[test]
    fn a_resume_stops_the_previous_owner() {
        let path = lock_path("resume-takes-over");
        fs::write(&path, "").expect("write empty lock");

        // An unreadable record parses as Building, which is the safe default.
        assert_eq!(
            MigrationLock::peek(&path).expect("peek").phase,
            LockPhase::Building
        );

        let taken_over = MigrationLock::resume(&path).expect("resume");
        let latest = MigrationLock::resume(&path).expect("resume again");
        assert!(
            taken_over.ensure_owned().is_err(),
            "the earlier owner must stop acting"
        );
        assert!(latest.ensure_owned().is_ok());

        latest.release().expect("release");
    }
}
