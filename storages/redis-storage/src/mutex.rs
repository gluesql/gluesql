use {
    gluesql_core::error::{Error, Result},
    std::sync::{Mutex, MutexGuard},
};

pub trait MutexExt<T> {
    fn lock_err(&self) -> Result<MutexGuard<'_, T>>;
}

impl<T> MutexExt<T> for Mutex<T> {
    fn lock_err(&self) -> Result<MutexGuard<'_, T>> {
        self.lock()
            .map_err(|e| Error::StorageMsg(format!("[RedisStorage] failed to acquire lock: {e}",)))
    }
}
