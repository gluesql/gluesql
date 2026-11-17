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
    rusqlite::Connection,
    std::path::Path,
};

type Result<T> = std::result::Result<T, SqliteStorageError>;

/// Stores `GlueSQL` tables in a `SQLite` database using STRICT columns and logical type metadata.
///
/// Supported filters use `SQLite`'s native planner. Remaining operations use the
/// common executor. Tables created by other `SQLite` applications are not yet supported.
#[derive(Debug)]
pub struct SqliteStorage {
    connection: Connection,
    transaction: TransactionState,
}

impl SqliteStorage {
    pub fn new(path: impl AsRef<Path>) -> Result<Self> {
        let connection = Connection::open(path)?;
        connection.create_collation(DECIMAL_COLLATION, compare_decimal)?;

        let storage = Self {
            connection,
            transaction: TransactionState::Idle,
        };
        storage.validate_metadata()?;

        Ok(storage)
    }

    pub fn in_memory() -> Result<Self> {
        let connection = Connection::open_in_memory()?;
        connection.create_collation(DECIMAL_COLLATION, compare_decimal)?;

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
