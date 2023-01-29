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

#[wasm_bindgen]
extern "C" {
    #[wasm_bindgen(js_namespace = console)]
    fn log(s: &str);
}

#[wasm_bindgen]
pub struct Glue {
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

        let storage = Some(MemoryStorage::default());
        let storage = Rc::new(RefCell::new(storage));

        log("[GlueSQL] hello :)");

        Self { storage }
    }

    pub fn query(&mut self, sql: String) -> Promise {
        let cell = Rc::clone(&self.storage);

        future_to_promise(async move {
            let queries = parse(&sql).map_err(|error| JsValue::from_str(&format!("{error}")))?;

            let mut payloads = vec![];
            let mut storage: MemoryStorage = cell.replace(None).unwrap();

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
