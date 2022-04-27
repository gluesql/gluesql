#![cfg(target_arch = "wasm32")]

use {
    memory_storage::{Key, MemoryStorage},
    std::{cell::RefCell, rc::Rc},
    test_suite::*,
    wasm_bindgen_test::*,
};

wasm_bindgen_test_configure!(run_in_browser);

struct MemoryTester {
    storage: Rc<RefCell<Option<MemoryStorage>>>,
}

impl Tester<Key, MemoryStorage> for MemoryTester {
    fn new(_: &str) -> Self {
        let storage = Some(MemoryStorage::default());
        let storage = Rc::new(RefCell::new(storage));

        MemoryTester { storage }
    }

    fn get_cell(&mut self) -> Rc<RefCell<Option<MemoryStorage>>> {
        Rc::clone(&self.storage)
    }
}

generate_store_tests!(wasm_bindgen_test, MemoryTester);
generate_metadata_tests!(wasm_bindgen_test, MemoryTester);
generate_alter_table_tests!(wasm_bindgen_test, MemoryTester);
