#![cfg(target_arch = "wasm32")]

mod payload;
mod utils;

use {
    gluesql_core::prelude::{execute, parse, plan, translate},
    js_sys::Promise,
    memory_storage::MemoryStorage,
    payload::convert,
    std::{cell::RefCell, rc::Rc},
    wasm_bindgen::prelude::*,
    wasm_bindgen_futures::future_to_promise,
};

#[cfg(not(feature = "nodejs"))]
use {
    composite_storage::CompositeStorage,
    idb_storage::IdbStorage,
    web_storage::{WebStorage, WebStorageType},
};

#[wasm_bindgen]
extern "C" {
    #[wasm_bindgen(js_namespace = console)]
    fn debug(s: &str);
}

#[wasm_bindgen]
pub struct Glue {
    #[cfg(not(feature = "nodejs"))]
    storage: Rc<RefCell<Option<CompositeStorage>>>,

    #[cfg(feature = "nodejs")]
    storage: Rc<RefCell<Option<MemoryStorage>>>,
}

impl Default for Glue {
    fn default() -> Self {
        Self::new()
    }
}

#[allow(clippy::unused_unit)]
#[wasm_bindgen]
impl Glue {
    #[wasm_bindgen(constructor)]
    pub fn new() -> Self {
        utils::set_panic_hook();

        #[cfg(not(feature = "nodejs"))]
        let storage = {
            let mut storage = CompositeStorage::default();
            storage.push("memory", MemoryStorage::default());
            storage.push("localStorage", WebStorage::new(WebStorageType::Local));
            storage.push("sessionStorage", WebStorage::new(WebStorageType::Session));
            storage.set_default("memory");
            debug("[GlueSQL] loaded: memory, localStorage, sessionStorage");
            debug("[GlueSQL] default engine: memory");

            storage
        };
        #[cfg(feature = "nodejs")]
        let storage = MemoryStorage::default();

        let storage = Rc::new(RefCell::new(Some(storage)));

        debug("[GlueSQL] hello :)");

        Self { storage }
    }

    #[cfg(not(feature = "nodejs"))]
    #[wasm_bindgen(js_name = loadIndexedDB)]
    pub fn load_indexeddb(&mut self) -> Promise {
        let cell = Rc::clone(&self.storage);

        future_to_promise(async move {
            let mut storage = cell.replace(None).unwrap();

            if storage.storages.contains_key("indexedDB") {
                cell.replace(Some(storage));

                return Err(JsValue::from_str("indexedDB storage is already loaded"));
            }

            let idb_storage = match IdbStorage::new(None).await {
                Ok(storage) => storage,
                Err(error) => {
                    cell.replace(Some(storage));

                    return Err(JsValue::from_str(&format!("{error}")));
                }
            };

            storage.push("indexedDB", idb_storage);
            debug("[GlueSQL] loaded: indexedDB");

            cell.replace(Some(storage));

            Ok(JsValue::NULL)
        })
    }

    #[cfg(not(feature = "nodejs"))]
    #[wasm_bindgen(js_name = setDefaultEngine)]
    pub fn set_default_engine(&mut self, default_engine: String) -> Result<(), JsValue> {
        let cell = Rc::clone(&self.storage);
        let mut storage = cell.replace(None).unwrap();

        let result = {
            if !["memory", "localStorage", "sessionStorage", "indexedDB"]
                .iter()
                .any(|engine| engine == &default_engine.as_str())
            {
                Err(JsValue::from_str(
                    format!("{default_engine} is not supported (options: memory, localStorage, sessionStorage, indexedDB)").as_str()
                ))
            } else if default_engine == "indexedDB" && !storage.storages.contains_key("indexedDB") {
                Err(JsValue::from_str(
                    "indexedDB is not loaded - run loadIndexedDB() first",
                ))
            } else {
                storage.set_default(default_engine);

                Ok(())
            }
        };

        cell.replace(Some(storage));
        result
    }

    pub fn query(&mut self, sql: String) -> Promise {
        let cell = Rc::clone(&self.storage);

        future_to_promise(async move {
            let queries = parse(&sql).map_err(|error| JsValue::from_str(&format!("{error}")))?;

            let mut payloads = vec![];
            let mut storage = cell.replace(None).unwrap();

            for query in queries.iter() {
                let statement = translate(query);
                let statement = match statement {
                    Ok(statement) => statement,
                    Err(error) => {
                        cell.replace(Some(storage));

                        return Err(JsValue::from_str(&format!("{error}")));
                    }
                };
                let statement = plan(&storage, statement).await;
                let statement = match statement {
                    Ok(statement) => statement,
                    Err(error) => {
                        cell.replace(Some(storage));

                        return Err(JsValue::from_str(&format!("{error}")));
                    }
                };

                let result = execute(&mut storage, &statement)
                    .await
                    .map_err(|error| JsValue::from_str(&format!("{error}")));

                match result {
                    Ok(payload) => {
                        payloads.push(payload);
                    }
                    Err(error) => {
                        cell.replace(Some(storage));

                        return Err(error);
                    }
                };
            }

            cell.replace(Some(storage));

            Ok(convert(payloads))
        })
    }
}
