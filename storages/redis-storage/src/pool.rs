use {
    gluesql_core::error::{Error, Result},
    redis::{Client, Connection},
    std::{
        ops::{Deref, DerefMut},
        sync::{Condvar, Mutex},
    },
};

fn lock_err<T>(e: impl std::fmt::Display) -> Result<T> {
    Err(Error::StorageMsg(format!(
        "[RedisStorage] failed to acquire pool lock: {e}"
    )))
}

pub struct ConnectionPool {
    idle: Mutex<Vec<Connection>>,
    available: Condvar,
}

impl ConnectionPool {
    pub fn new(client: &Client, size: usize) -> Result<Self> {
        if size == 0 {
            return Err(Error::StorageMsg(
                "[RedisStorage] connection pool size must be greater than 0".to_owned(),
            ));
        }

        let idle = (0..size)
            .map(|_| {
                client.get_connection().map_err(|e| {
                    Error::StorageMsg(format!("[RedisStorage] failed to connect to Redis: {e}"))
                })
            })
            .collect::<Result<Vec<Connection>>>()?;

        Ok(ConnectionPool {
            idle: Mutex::new(idle),
            available: Condvar::new(),
        })
    }

    pub fn checkout(&self) -> Result<PooledConnection<'_>> {
        let mut idle = match self.idle.lock() {
            Ok(idle) => idle,
            Err(e) => return lock_err(e),
        };

        while idle.is_empty() {
            idle = match self.available.wait(idle) {
                Ok(idle) => idle,
                Err(e) => return lock_err(e),
            };
        }

        let conn = idle.pop().expect("idle connection checked non-empty above");

        Ok(PooledConnection {
            pool: self,
            conn: Some(conn),
        })
    }

    fn checkin(&self, conn: Connection) {
        if let Ok(mut idle) = self.idle.lock() {
            idle.push(conn);
            self.available.notify_one();
        }
    }
}

pub struct PooledConnection<'a> {
    pool: &'a ConnectionPool,
    conn: Option<Connection>,
}

impl Deref for PooledConnection<'_> {
    type Target = Connection;

    fn deref(&self) -> &Connection {
        self.conn.as_ref().expect("connection taken before drop")
    }
}

impl DerefMut for PooledConnection<'_> {
    fn deref_mut(&mut self) -> &mut Connection {
        self.conn.as_mut().expect("connection taken before drop")
    }
}

impl Drop for PooledConnection<'_> {
    fn drop(&mut self) {
        if let Some(conn) = self.conn.take() {
            self.pool.checkin(conn);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::ConnectionPool;

    #[test]
    fn new_rejects_zero_size() {
        let client = redis::Client::open("redis://127.0.0.1:6379").unwrap();

        let result = ConnectionPool::new(&client, 0);

        assert!(result.is_err());
    }
}
