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
    open_with_lock_wait_by(
        || config.open(),
        Instant::now,
        thread::sleep,
        LOCK_WAIT_TIMEOUT,
        LOCK_WAIT_INTERVAL,
    )
}

fn open_with_lock_wait_by<T>(
    mut open: impl FnMut() -> sled::Result<T>,
    mut now: impl FnMut() -> Instant,
    mut sleep: impl FnMut(Duration),
    timeout: Duration,
    interval: Duration,
) -> Result<T> {
    let started_at = now();

    loop {
        match open() {
            Ok(tree) => return Ok(tree),
            Err(error) if is_lock_unavailable(&error) => {
                let remaining = timeout.saturating_sub(now().duration_since(started_at));
                if remaining.is_zero() {
                    return Err(Error::StorageMsg(format!(
                        "[SledStorage] timed out after {} ms waiting for the database lock: {error}",
                        timeout.as_millis(),
                    )));
                }

                sleep(interval.min(remaining));
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
        let started_at = Instant::now();
        let mut now = [started_at, started_at].into_iter();
        let mut sleeps = Vec::new();

        open_with_lock_wait_by(
            || {
                attempts += 1;
                if attempts == 1 {
                    Err(lock_error())
                } else {
                    Ok(())
                }
            },
            || now.next().expect("current time"),
            |duration| sleeps.push(duration),
            Duration::from_secs(1),
            Duration::from_millis(10),
        )
        .expect("open after waiting");

        assert_eq!(attempts, 2);
        assert_eq!(sleeps, [Duration::from_millis(10)]);
    }

    #[test]
    fn caps_wait_at_timeout_and_stops_retrying() {
        let mut attempts = 0;
        let started_at = Instant::now();
        let mut now = [
            started_at,
            started_at + Duration::from_millis(15),
            started_at + Duration::from_millis(20),
        ]
        .into_iter();
        let mut sleeps = Vec::new();
        let actual: Result<()> = open_with_lock_wait_by(
            || {
                attempts += 1;
                Err(lock_error())
            },
            || now.next().expect("current time"),
            |duration| sleeps.push(duration),
            Duration::from_millis(20),
            Duration::from_millis(10),
        );
        assert!(matches!(
            actual,
            Err(Error::StorageMsg(message))
                if message.starts_with(
                    "[SledStorage] timed out after 20 ms waiting for the database lock:"
                )
        ));
        assert_eq!(attempts, 2);
        assert_eq!(sleeps, [Duration::from_millis(5)]);
    }

    #[test]
    fn returns_non_lock_error_without_waiting() {
        let mut attempts = 0;
        let now = Instant::now();
        let actual: Result<()> = open_with_lock_wait_by(
            || {
                attempts += 1;
                Err(sled::Error::Unsupported("invalid config".to_owned()))
            },
            || now,
            thread::sleep,
            Duration::from_secs(1),
            Duration::from_millis(10),
        );

        assert!(matches!(actual, Err(Error::StorageMsg(_))));
        assert_eq!(attempts, 1);
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
