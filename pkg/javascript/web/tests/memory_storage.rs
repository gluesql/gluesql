#![cfg(target_arch = "wasm32")]

use {
    gluesql_core::prelude::Glue, memory_storage::MemoryStorage, test_suite::*, wasm_bindgen_test::*,
};

wasm_bindgen_test_configure!(run_in_browser);

struct MemoryTester {
    glue: Glue<MemoryStorage>,
}

impl Tester<MemoryStorage> for MemoryTester {
    fn new(_: &str) -> Self {
        let storage = MemoryStorage::default();
        let glue = Glue::new(storage);

        MemoryTester { glue }
    }

    fn get_glue(&mut self) -> &mut Glue<MemoryStorage> {
        &mut self.glue
    }
}

generate_store_tests!(wasm_bindgen_test, MemoryTester);
generate_alter_table_tests!(wasm_bindgen_test, MemoryTester);
