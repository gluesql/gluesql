//! Synchronous `GlueSQL` storage backed by `SlateDB` and an object store.
//!
//! Each instance owns its database and Tokio runtime. From an async application,
//! construct, use, and close the storage inside a blocking thread, such as
//! `tokio::task::spawn_blocking`.
//!
//! ```
//! use {
//!     gluesql_core::prelude::Glue,
//!     gluesql_slatedb_storage::SlatedbStorage,
//! };
//!
//! let storage = SlatedbStorage::open("memory:///example")?;
//! let mut glue = Glue::new(storage);
//! glue.execute("CREATE TABLE Items (id INTEGER PRIMARY KEY)")?;
//! glue.execute("INSERT INTO Items VALUES (1)")?;
//! glue.storage.close()?;
//! # Ok::<(), gluesql_core::error::Error>(())
//! ```

mod error;
mod key;
mod store;
mod store_mut;
mod transaction;

pub use slatedb::object_store;

use {
    error::StorageError,
    gluesql_core::{
        error::Result,
        store::{
            AlterTable, CustomFunction, CustomFunctionMut, Index, IndexMut, Metadata, Planner,
        },
    },
    object_store::{ObjectStore, parse_url_opts, path::Path},
    slatedb::{Db, DbTransaction, config::CloseOptions},
    std::{env, sync::Arc},
    tokio::runtime::{Builder, Runtime},
    url::Url,
};

/// A single-writer database with one active `GlueSQL` transaction per instance.
pub struct SlatedbStorage {
    db: Db,
    txn: Option<DbTransaction>,
    runtime: Runtime,
}

impl SlatedbStorage {
    /// Opens a database using an object-store URL and environment configuration.
    ///
    /// The URL selects the store and database prefix. Credentials and other
    /// settings follow [`object_store::parse_url_opts`]. Like [`Self::new`], this
    /// method must run outside async execution.
    ///
    /// Each memory URL opens a fresh in-memory store.
    ///
    /// S3 and R2 examples (configure credentials in the environment):
    ///
    /// ```no_run
    /// use gluesql_slatedb_storage::SlatedbStorage;
    ///
    /// let storage = SlatedbStorage::open("s3://my-bucket/my_database")?;
    /// storage.close()?;
    ///
    /// let storage = SlatedbStorage::open(
    ///     "https://ACCOUNT_ID.r2.cloudflarestorage.com/my-bucket/my_database",
    /// )?;
    /// storage.close()?;
    /// # Ok::<(), gluesql_core::error::Error>(())
    /// ```
    pub fn open(url: &str) -> Result<Self> {
        let url = Url::parse(url).map_err(StorageError::Url)?;
        let options = env::vars_os().filter_map(|(key, value)| {
            let key = key.into_string().ok()?;
            let value = value.into_string().ok()?;

            Some((key, value))
        });
        let (object_store, prefix) =
            parse_url_opts(&url, options).map_err(StorageError::ObjectStore)?;

        Self::new(Arc::from(object_store), prefix)
    }

    /// Opens a database at `prefix` in the supplied object store.
    ///
    /// Use a separate prefix for each database. Only one writer should open a
    /// given prefix at a time; `SlateDB` fences an older writer when another opens.
    /// This method, queries, and destruction must run outside async execution.
    ///
    /// ```no_run
    /// use {
    ///     gluesql_slatedb_storage::{SlatedbStorage, object_store::aws::AmazonS3Builder},
    ///     std::sync::Arc,
    /// };
    ///
    /// let objects = AmazonS3Builder::from_env()
    ///     .with_bucket_name("my-bucket")
    ///     .with_region("us-east-1")
    ///     .build()?;
    /// let storage = SlatedbStorage::new(Arc::new(objects), "my_database")?;
    /// storage.close()?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn new(object_store: Arc<dyn ObjectStore>, prefix: impl Into<Path>) -> Result<Self> {
        let runtime = Builder::new_multi_thread()
            .enable_all()
            .build()
            .map_err(StorageError::Runtime)?;
        let db = runtime
            .block_on(Db::open(prefix, object_store))
            .map_err(StorageError::SlateDb)?;

        Ok(Self {
            db,
            txn: None,
            runtime,
        })
    }

    /// Discards an active transaction and closes the database, returning any
    /// shutdown error. Successful commits have already waited for durability.
    pub fn close(mut self) -> Result<()> {
        self.txn.take();

        self.runtime
            .block_on(self.db.close())
            .map_err(StorageError::SlateDb)?;

        Ok(())
    }

    fn txn(&self) -> Result<&DbTransaction> {
        self.txn
            .as_ref()
            .ok_or_else(|| StorageError::TransactionNotFound.into())
    }
}

impl Drop for SlatedbStorage {
    fn drop(&mut self) {
        self.txn.take();

        // Commits have already waited for the WAL. Avoid an additional final
        // flush when the caller relies on Drop, which cannot report errors.
        let options = CloseOptions { flush_type: None };
        let _ = self.runtime.block_on(self.db.close_with_options(options));
    }
}

impl AlterTable for SlatedbStorage {}
impl Planner for SlatedbStorage {}
impl Metadata for SlatedbStorage {}
impl Index for SlatedbStorage {}
impl IndexMut for SlatedbStorage {}
impl CustomFunction for SlatedbStorage {}
impl CustomFunctionMut for SlatedbStorage {}
