use {
    crate::error::err_into,
    gluesql_core::error::{Error, Result},
    sled::{Config, Db},
    std::{
        io::ErrorKind,
        thread,
        time::{Duration, Instant},
    },
};

const LOCK_WAIT_TIMEOUT: Duration = Duration::from_secs(1);
const LOCK_WAIT_INTERVAL: Duration = Duration::from_millis(10);

pub(crate) fn open_with_lock_wait(config: &Config) -> Result<Db> {
    let started_at = Instant::now();

    open_with_lock_wait_by(
        || config.open(),
        || {
            let remaining = LOCK_WAIT_TIMEOUT.saturating_sub(started_at.elapsed());
            if remaining.is_zero() {
                return false;
            }

            thread::sleep(LOCK_WAIT_INTERVAL.min(remaining));
            true
        },
        LOCK_WAIT_TIMEOUT,
    )
}

fn open_with_lock_wait_by<T>(
    mut open: impl FnMut() -> sled::Result<T>,
    mut wait: impl FnMut() -> bool,
    timeout: Duration,
) -> Result<T> {
    loop {
        match open() {
            Ok(tree) => return Ok(tree),
            Err(error) if is_lock_unavailable(&error) => {
                if !wait() {
                    return Err(Error::StorageMsg(format!(
                        "[SledStorage] timed out after {} ms waiting for the database lock: {error}",
                        timeout.as_millis(),
                    )));
                }
            }
            Err(error) => return Err(err_into(error)),
        }
    }
}

fn is_lock_unavailable(error: &sled::Error) -> bool {
    matches!(
        error,
        sled::Error::Io(error)
            if error.kind() == ErrorKind::Other
                && error.to_string().starts_with("could not acquire lock on ")
    )
}

#[cfg(test)]
mod tests {
    use {super::*, std::io};

    fn configs_for_same_temporary_path() -> (Config, Config) {
        let owner = Config::new().temporary(true).flush_every_ms(None);
        let contender = Config::new().path(owner.get_path()).flush_every_ms(None);

        (owner, contender)
    }

    fn lock_error() -> sled::Error {
        sled::Error::Io(io::Error::other(
            "could not acquire lock on \"db\": Resource temporarily unavailable",
        ))
    }

    #[test]
    fn retries_after_waiting_for_lock() {
        let mut attempts = 0;
        let mut waits = 0;

        open_with_lock_wait_by(
            || {
                attempts += 1;
                if attempts == 1 {
                    Err(lock_error())
                } else {
                    Ok(())
                }
            },
            || {
                waits += 1;
                true
            },
            Duration::from_secs(1),
        )
        .expect("open after waiting");

        assert_eq!(attempts, 2);
        assert_eq!(waits, 1);
    }

    #[test]
    fn returns_timeout_without_another_attempt() {
        let mut attempts = 0;
        let mut waits = 0;
        let actual: Result<()> = open_with_lock_wait_by(
            || {
                attempts += 1;
                Err(lock_error())
            },
            || {
                waits += 1;
                false
            },
            Duration::from_millis(20),
        );
        assert!(matches!(
            actual,
            Err(Error::StorageMsg(message))
                if message.starts_with(
                    "[SledStorage] timed out after 20 ms waiting for the database lock:"
                )
        ));
        assert_eq!(attempts, 1);
        assert_eq!(waits, 1);
    }

    #[test]
    fn returns_non_lock_error_without_waiting() {
        let mut waits = 0;
        let actual: Result<()> = open_with_lock_wait_by(
            || Err(sled::Error::Unsupported("invalid config".to_owned())),
            || {
                waits += 1;
                true
            },
            Duration::from_secs(1),
        );

        assert!(matches!(actual, Err(Error::StorageMsg(_))));
        assert_eq!(waits, 0);
    }

    #[test]
    fn recognizes_lock_error_from_sled() {
        let (owner_config, contender_config) = configs_for_same_temporary_path();
        let owner = owner_config.open().expect("open lock owner");
        let actual = contender_config
            .open()
            .expect_err("second open should be locked");

        assert!(is_lock_unavailable(&actual));
        drop(owner);
    }
}
