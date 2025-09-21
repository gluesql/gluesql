#![deny(clippy::all)]

mod payload;
mod utils;

use {
    gluesql_core::prelude::{execute, parse, plan, translate},
    gluesql_memory_storage::MemoryStorage,
    napi::bindgen_prelude::*,
    napi_derive::napi,
    payload::convert,
    std::{sync::{Arc, Mutex}},
};

#[cfg(not(target_family = "wasm"))]
use {
    gluesql_composite_storage::CompositeStorage,
};

#[napi]
pub struct Glue {
    #[cfg(not(target_family = "wasm"))]
    storage: Arc<Mutex<Option<CompositeStorage>>>,

    #[cfg(target_family = "wasm")]
    storage: Arc<Mutex<Option<MemoryStorage>>>,
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
            println!("[GlueSQL] loaded: memory");
            println!("[GlueSQL] default engine: memory");

            storage
        };
        #[cfg(target_family = "wasm")]
        let storage = MemoryStorage::default();

        let storage = Arc::new(Mutex::new(Some(storage)));

        println!("[GlueSQL] hello :)");

        Self { storage }
    }

    #[cfg(not(target_family = "wasm"))]
    #[napi]
    pub fn set_default_engine(&self, default_engine: String) -> Result<()> {
        let mut storage_guard = self.storage.lock().unwrap();
        let mut storage = storage_guard.take().unwrap();

        let result = {
            if !["memory"]
                .iter()
                .any(|engine| engine == &default_engine.as_str())
            {
                Err(Error::new(
                    Status::InvalidArg,
                    format!("{default_engine} is not supported (options: memory)")
                ))
            } else {
                storage.set_default(default_engine);
                Ok(())
            }
        };

        *storage_guard = Some(storage);
        result
    }

    #[napi]
    pub async fn query(&self, sql: String) -> Result<serde_json::Value> {
        let queries = parse(&sql).map_err(|error| {
            Error::new(Status::GenericFailure, format!("{error}"))
        })?;

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
