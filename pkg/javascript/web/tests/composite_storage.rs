#![cfg(target_arch = "wasm32")]

use {
    async_trait::async_trait, composite_storage::CompositeStorage, gluesql_core::prelude::Glue,
    memory_storage::MemoryStorage, test_suite::*, wasm_bindgen_test::*,
};

wasm_bindgen_test_configure!(run_in_browser);

struct CompositeTester {
    glue: Glue<CompositeStorage>,
}

#[async_trait(?Send)]
impl Tester<CompositeStorage> for CompositeTester {
    async fn new(_: &str) -> Self {
        let mut storage = CompositeStorage::default();
        storage.push("memory", MemoryStorage::default());
        storage.set_default("memory");

        let glue = Glue::new(storage);

        Self { glue }
    }

    fn get_glue(&mut self) -> &mut Glue<CompositeStorage> {
        &mut self.glue
    }
}

generate_store_tests!(wasm_bindgen_test, CompositeTester);
