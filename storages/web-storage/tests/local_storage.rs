#![cfg(target_arch = "wasm32")]

use {
    async_trait::async_trait,
    gluesql_core::prelude::Glue,
    gluesql_web_storage::{WebStorage, WebStorageType},
    test_suite::*,
    wasm_bindgen_test::{wasm_bindgen_test, wasm_bindgen_test_configure},
};

wasm_bindgen_test_configure!(run_in_browser);

struct LocalStorageTester {
    glue: Glue<WebStorage>,
}

#[async_trait(?Send)]
impl Tester<WebStorage> for LocalStorageTester {
    async fn new(_: &str) -> Self {
        let storage = WebStorage::new(WebStorageType::Local);
        storage.raw().clear().unwrap();

        let glue = Glue::new(storage);

        LocalStorageTester { glue }
    }

    fn get_glue(&mut self) -> &mut Glue<WebStorage> {
        &mut self.glue
    }
}

generate_store_tests!(wasm_bindgen_test, LocalStorageTester);
generate_alter_table_tests!(wasm_bindgen_test, LocalStorageTester);
