#![deny(clippy::all)]

mod debug;
mod payload;
mod utils;

use debug::console_debug;
use gluesql_core::prelude::{execute, parse, plan, translate};
use gluesql_memory_storage::MemoryStorage;
use napi::bindgen_prelude::*;
use napi_derive::napi;
use std::sync::{Arc, Mutex};

use gluesql_composite_storage::CompositeStorage;

// Web / IDB storage crates are only available on wasm; import them conditionally
#[cfg(target_family = "wasm")]
use gluesql_web_storage::{WebStorage, WebStorageType};

#[napi]
pub struct Glue {
  #[cfg(not(target_family = "wasm"))]
  storage: Arc<Mutex<Option<CompositeStorage>>>,

  // For wasm target, use a CompositeStorage so we can support memory,
  // localStorage, sessionStorage.
  #[cfg(target_family = "wasm")]
  storage: Arc<Mutex<Option<CompositeStorage>>>,
}

impl Default for Glue {
  fn default() -> Self {
    Self::new()
  }
}

#[napi]
impl Glue {
  #[napi(constructor)]
  pub fn new() -> Self {
    utils::set_panic_hook();

    #[cfg(not(target_family = "wasm"))]
    let storage = {
      let mut storage = CompositeStorage::default();
      storage.push("memory", MemoryStorage::default());
      storage.set_default("memory");

      console_debug("[GlueSQL] loaded: memory");
      console_debug("[GlueSQL] default engine: memory");

      storage
    };
    #[cfg(target_family = "wasm")]
    let storage = {
      // Construct a composite storage with memory + web storages available in browsers.
      let mut storage = CompositeStorage::default();
      storage.push("memory", MemoryStorage::default());

      // Add localStorage and sessionStorage wrappers.
      storage.push("localStorage", WebStorage::new(WebStorageType::Local));
      storage.push("sessionStorage", WebStorage::new(WebStorageType::Session));
      storage.set_default("memory");

      console_debug("[GlueSQL] loaded: memory, localStorage, sessionStorage");
      console_debug("[GlueSQL] default engine: memory");

      storage
    };

    let storage = Arc::new(Mutex::new(Some(storage)));

    console_debug("[GlueSQL] hello :)");

    Self { storage }
  }

  /// Set default engine. On wasm targets this supports memory, localStorage,
  /// and sessionStorage. On non-wasm targets only "memory" is supported.
  #[napi]
  pub fn set_default_engine(&self, default_engine: String) -> Result<()> {
    let mut storage_guard = self.storage.lock().unwrap();
    let mut storage = storage_guard.take().unwrap();

    let result = if cfg!(target_family = "wasm") {
      if !["memory", "localStorage", "sessionStorage"]
        .iter()
        .any(|engine| engine == &default_engine.as_str())
      {
        Err(Error::new(
          Status::InvalidArg,
          format!("{default_engine} is not supported (options: memory, localStorage, sessionStorage)"),
        ))
      } else {
        storage.set_default(default_engine);
        Ok(())
      }
    } else if default_engine != "memory" {
      Err(Error::new(
        Status::InvalidArg,
        format!("{default_engine} is not supported (options: memory)"),
      ))
    } else {
      storage.set_default(default_engine);
      Ok(())
    };

    *storage_guard = Some(storage);
    result
  }

  #[napi]
  pub async fn query(&self, sql: String) -> Result<serde_json::Value> {
    let queries =
      parse(&sql).map_err(|error| Error::new(Status::GenericFailure, format!("{error}")))?;

    let mut payloads = vec![];

    // Take the storage out of the mutex for async operation
    let mut storage = {
      let mut storage_guard = self.storage.lock().unwrap();
      storage_guard.take().unwrap()
    };

    for query in queries.iter() {
      let statement = translate(query);
      let statement = match statement {
        Ok(statement) => statement,
        Err(error) => {
          // Put storage back before returning error
          let mut storage_guard = self.storage.lock().unwrap();
          *storage_guard = Some(storage);
          return Err(Error::new(Status::GenericFailure, format!("{error}")));
        }
      };
      let statement = plan(&storage, statement).await;
      let statement = match statement {
        Ok(statement) => statement,
        Err(error) => {
          // Put storage back before returning error
          let mut storage_guard = self.storage.lock().unwrap();
          *storage_guard = Some(storage);
          return Err(Error::new(Status::GenericFailure, format!("{error}")));
        }
      };

      let result = execute(&mut storage, &statement)
        .await
        .map_err(|error| Error::new(Status::GenericFailure, format!("{error}")));

      match result {
        Ok(payload) => {
          payloads.push(payload);
        }
        Err(error) => {
          // Put storage back before returning error
          let mut storage_guard = self.storage.lock().unwrap();
          *storage_guard = Some(storage);
          return Err(error);
        }
      };
    }

    // Put storage back
    let mut storage_guard = self.storage.lock().unwrap();
    *storage_guard = Some(storage);

    convert(payloads)
  }
}

pub use payload::convert;

#[cfg(test)]
mod tests {
  use super::*;
  use gluesql_composite_storage::CompositeStorage;
  use gluesql_core::prelude::Glue;
  use gluesql_memory_storage::MemoryStorage;

  #[tokio::test]
  async fn test_basic_queries() {
    let storage = MemoryStorage::default();
    let mut glue = Glue::new(storage);

    let test_cases = [
      (
        "
                CREATE TABLE Foo (id INTEGER);
                CREATE TABLE Bar;
                ",
        serde_json::json!([
            { "type": "CREATE TABLE" },
            { "type": "CREATE TABLE" },
        ]),
      ),
      (
        "INSERT INTO Foo VALUES (1), (2), (3)",
        serde_json::json!([{
            "type": "INSERT",
            "affected": 3
        }]),
      ),
      (
        "SELECT * FROM Foo",
        serde_json::json!([{
            "type": "SELECT",
            "rows": [
                { "id": 1 },
                { "id": 2 },
                { "id": 3 }
            ]
        }]),
      ),
    ];

    for (sql, expected) in test_cases {
      let results = glue.execute(sql).await.unwrap();
      let payload = convert(results).unwrap();

      assert_eq!(payload, expected, "Failed for SQL: {}", sql);
    }
  }

  #[tokio::test]
  async fn test_composite_storage() {
    let mut composite = CompositeStorage::default();
    composite.push("memory", MemoryStorage::default());
    composite.set_default("memory");

    let mut glue = Glue::new(composite);

    // Test basic table creation and operations
    let results = glue
      .execute("CREATE TABLE test (id INTEGER)")
      .await
      .unwrap();
    let payload = convert(results).unwrap();
    assert_eq!(payload[0]["type"], "CREATE TABLE");

    // Test data insertion
    let results = glue
      .execute("INSERT INTO test VALUES (1), (2), (3)")
      .await
      .unwrap();
    let payload = convert(results).unwrap();
    assert_eq!(payload[0]["type"], "INSERT");
    assert_eq!(payload[0]["affected"], 3);
  }

  #[tokio::test]
  async fn test_napi_glue_engine_setting() {
    // Use the NAPI struct, not the core struct
    let napi_glue = crate::Glue::new();

    // Test setting valid engine
    let result = napi_glue.set_default_engine("memory".to_string());
    assert!(result.is_ok(), "Should successfully set memory engine");

    // Test setting an invalid engine
    let result = napi_glue.set_default_engine("invalid-engine".to_string());
    assert!(result.is_err(), "Should return error for invalid engine");
  }

  #[tokio::test]
  async fn test_sql_syntax_errors() {
    let storage = MemoryStorage::default();
    let mut glue = Glue::new(storage);

    // Test invalid SQL syntax
    let result = glue.execute("INVALID SQL STATEMENT").await;
    assert!(result.is_err(), "Query should fail for invalid SQL");
  }
}
