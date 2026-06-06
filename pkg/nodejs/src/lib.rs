#![allow(clippy::all)]

mod payload;

use {
    gluesql_core::prelude::{execute, parse, translate},
    gluesql_core::store::Planner,
    gluesql_memory_storage::MemoryStorage,
    napi::bindgen_prelude::*,
    napi_derive::napi,
    tokio::sync::Mutex,
};

/// `GlueSQL` engine backed by in-memory storage.
///
/// ```js
/// const { Glue } = require('@gluesql/node');
/// const glue = new Glue();
/// const result = await glue.query('SELECT 1 + 1 AS n');
/// ```
#[napi]
pub struct Glue {
    storage: Mutex<MemoryStorage>,
}

#[napi]
impl Glue {
    /// Create a new in-memory `GlueSQL` instance.
    #[napi(constructor)]
    pub fn new() -> Self {
        Glue {
            storage: Mutex::new(MemoryStorage::default()),
        }
    }

    /// Execute one or more SQL statements separated by semicolons.
    ///
    /// Returns a Promise that resolves to an array of payload objects,
    /// one per statement.  Rejects with an Error on parse or execution failure.
    ///
    /// The shape of each payload mirrors the existing `gluesql` wasm package:
    /// - `{ type: "SELECT",       rows: [...] }`
    /// - `{ type: "INSERT",       affected: n }`
    /// - `{ type: "UPDATE",       affected: n }`
    /// - `{ type: "DELETE",       affected: n }`
    /// - `{ type: "CREATE TABLE" }`
    /// - `{ type: "DROP TABLE",   affected: n }`
    /// - `{ type: "SHOW TABLES",  tables: [...] }`
    /// - `{ type: "SHOW COLUMNS", columns: [...] }`
    /// - `{ type: "BEGIN" }`, `{ type: "COMMIT" }`, `{ type: "ROLLBACK" }`
    ///
    /// Concurrent `query()` calls on the same instance are serialized
    /// internally via a `tokio::sync::Mutex`, so overlapping `await`s are
    /// safe and never race on the underlying storage.
    #[napi]
    pub async fn query(&self, sql: String) -> Result<serde_json::Value> {
        let queries =
            parse(&sql).map_err(|e| Error::from_reason(format!("[GlueSQL] parse error: {e}")))?;

        let mut payloads = Vec::with_capacity(queries.len());

        let mut storage = self.storage.lock().await;

        for query in &queries {
            let statement = translate(query)
                .map_err(|e| Error::from_reason(format!("[GlueSQL] translate error: {e}")))?;

            let statement = storage
                .plan(statement.into())
                .await
                .map_err(|e| Error::from_reason(format!("[GlueSQL] plan error: {e}")))?;

            let payload = execute(&mut *storage, &statement)
                .await
                .map_err(|e| Error::from_reason(format!("[GlueSQL] execute error: {e}")))?;

            payloads.push(payload);
        }

        payload::convert(payloads)
            .map_err(|e| Error::from_reason(format!("[GlueSQL] payload conversion error: {e}")))
    }
}
