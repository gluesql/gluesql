#![deny(clippy::str_to_string)]

use {
    gluesql_core::{
        error::{Error as GlueError, Result},
        store::{AlterTable, CustomFunction, CustomFunctionMut, Index, IndexMut, Metadata},
    },
    std::{
        fmt::Display,
        path::Path,
        sync::{Arc, Mutex},
    },
    tokio_rusqlite::Connection,
};

mod codec;
mod operations;
mod planner;
mod schema;
mod sql_builder;
mod store;
mod store_mut;
mod transaction;

#[derive(Clone)]
pub struct SqliteStorage {
    conn: Arc<Connection>,
    tx_active: Arc<Mutex<bool>>,
}

impl SqliteStorage {
    pub async fn new<P: AsRef<Path>>(path: P) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        let conn = Connection::open(path).await.map_err(map_conn_err)?;

        let storage = Self {
            conn: Arc::new(conn),
            tx_active: Arc::new(Mutex::new(false)),
        };
        storage.execute("PRAGMA foreign_keys = ON;").await?;

        Ok(storage)
    }

    pub async fn memory() -> Result<Self> {
        let conn = Connection::open_in_memory().await.map_err(map_conn_err)?;

        let storage = Self {
            conn: Arc::new(conn),
            tx_active: Arc::new(Mutex::new(false)),
        };
        storage.execute("PRAGMA foreign_keys = ON;").await?;

        Ok(storage)
    }

    fn connection(&self) -> Arc<Connection> {
        Arc::clone(&self.conn)
    }

    async fn execute(&self, sql: &str) -> Result<()> {
        let sql = sql.to_owned();
        self.with_conn(move |conn| conn.execute(sql.as_str(), []).map(|_| ()))
            .await
    }

    async fn with_conn<F, R>(&self, f: F) -> Result<R>
    where
        F: FnOnce(&mut rusqlite::Connection) -> rusqlite::Result<R> + Send + 'static,
        R: Send + 'static,
    {
        self.connection()
            .call(move |conn| f(conn).map_err(tokio_rusqlite::Error::Rusqlite))
            .await
            .map_err(map_tokio_err)
    }
}

impl Metadata for SqliteStorage {}
impl AlterTable for SqliteStorage {}
impl Index for SqliteStorage {}
impl IndexMut for SqliteStorage {}
impl CustomFunction for SqliteStorage {}
impl CustomFunctionMut for SqliteStorage {}

fn map_conn_err<E: Display>(err: E) -> GlueError {
    GlueError::StorageMsg(err.to_string())
}

pub(crate) fn map_sql_err<E: Display>(err: E) -> GlueError {
    GlueError::StorageMsg(err.to_string())
}

pub(crate) fn map_ser_err<E: Display>(err: E) -> GlueError {
    GlueError::StorageMsg(err.to_string())
}

pub(crate) fn map_tokio_err(err: tokio_rusqlite::Error) -> GlueError {
    match err {
        tokio_rusqlite::Error::Rusqlite(e) => map_sql_err(e),
        other => GlueError::StorageMsg(other.to_string()),
    }
}

pub(crate) fn glue_to_rusqlite(err: GlueError) -> rusqlite::Error {
    rusqlite::Error::ToSqlConversionFailure(Box::new(err))
}
