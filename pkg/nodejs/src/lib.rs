#![allow(clippy::all)]

mod payload;

use {
    gluesql_core::prelude::{execute, parse, translate},
    gluesql_core::store::Planner,
    gluesql_memory_storage::MemoryStorage,
    napi::bindgen_prelude::*,
    napi_derive::napi,
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
    storage: MemoryStorage,
}

#[napi]
impl Glue {
    /// Create a new in-memory `GlueSQL` instance.
    #[napi(constructor)]
    pub fn new() -> Self {
        Glue {
            storage: MemoryStorage::default(),
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
    /// # Safety
    ///
    /// `&mut self` in an async napi context requires `unsafe` because napi-rs
    /// wraps the struct in `Arc` and cannot statically guarantee exclusive
    /// access.  The JavaScript event loop is single-threaded, so concurrent
    /// mutation will not occur in practice — callers must not `await` two
    /// `query()` calls on the same instance simultaneously.
    #[napi]
    pub async unsafe fn query(&mut self, sql: String) -> Result<serde_json::Value> {
        let queries =
            parse(&sql).map_err(|e| Error::from_reason(format!("[GlueSQL] parse error: {e}")))?;

        let mut payloads = Vec::with_capacity(queries.len());

        for query in &queries {
            let statement = translate(query)
                .map_err(|e| Error::from_reason(format!("[GlueSQL] translate error: {e}")))?;

            let statement = self
                .storage
                .plan(statement.into())
                .await
                .map_err(|e| Error::from_reason(format!("[GlueSQL] plan error: {e}")))?;

            let payload = execute(&mut self.storage, &statement)
                .await
                .map_err(|e| Error::from_reason(format!("[GlueSQL] execute error: {e}")))?;

            payloads.push(payload);
        }

        Ok(payload::convert(payloads))
    }
}
