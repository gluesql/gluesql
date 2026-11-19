#![deny(clippy::str_to_string)]

mod alter_table;
mod codec;
mod collation;
mod error;
mod native_query;
mod planner;
mod schema;
mod store;
mod store_mut;
mod transaction;

pub use error::SqliteStorageError;

use {
    crate::{
        collation::{DECIMAL_COLLATION, compare_decimal},
        transaction::TransactionState,
    },
    gluesql_core::store::{CustomFunction, CustomFunctionMut, Index, IndexMut, Metadata},
    rusqlite::{Connection, vtab::array},
    std::path::Path,
};

type Result<T> = std::result::Result<T, SqliteStorageError>;

/// Stores `GlueSQL` tables in a `SQLite` database using STRICT columns and logical type metadata.
///
/// Table scans and eligible filters run directly in `SQLite`. The common executor
/// consumes the returned rows. This backend supports tables created through `GlueSQL`.
#[derive(Debug)]
pub struct SqliteStorage {
    connection: Connection,
    transaction: TransactionState,
}

impl SqliteStorage {
    pub fn new(path: impl AsRef<Path>) -> Result<Self> {
        let storage = Self::from_connection(Connection::open(path)?)?;
        storage.validate_metadata()?;

        Ok(storage)
    }

    pub fn in_memory() -> Result<Self> {
        Self::from_connection(Connection::open_in_memory()?)
    }

    fn from_connection(connection: Connection) -> Result<Self> {
        connection.create_collation(DECIMAL_COLLATION, compare_decimal)?;
        array::load_module(&connection)?;

        Ok(Self {
            connection,
            transaction: TransactionState::Idle,
        })
    }
}

impl Index for SqliteStorage {}
impl IndexMut for SqliteStorage {}
impl Metadata for SqliteStorage {}
impl CustomFunction for SqliteStorage {}
impl CustomFunctionMut for SqliteStorage {}
