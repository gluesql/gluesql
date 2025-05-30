#![cfg(target_arch = "wasm32")]

use {
    async_trait::async_trait,
    gluesql_core::prelude::Glue,
    gluesql_idb_storage::IdbStorage,
    test_suite::*,
    wasm_bindgen_test::{wasm_bindgen_test, wasm_bindgen_test_configure},
};

wasm_bindgen_test_configure!(run_in_browser);

struct IdbStorageTester {
    glue: Glue<IdbStorage>,
}

#[async_trait(?Send)]
impl Tester<IdbStorage> for IdbStorageTester {
    async fn new(namespace: &str) -> Self {
        let storage = IdbStorage::new(Some(namespace.to_owned())).await.unwrap();
        let glue = Glue::new(storage);

        Self { glue }
    }

    fn get_glue(&mut self) -> &mut Glue<IdbStorage> {
        &mut self.glue
    }
}

generate_store_tests!(wasm_bindgen_test, IdbStorageTester);
generate_alter_table_tests!(wasm_bindgen_test, IdbStorageTester);
