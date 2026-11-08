use {
    gluesql_core::prelude::Glue,
    gluesql_slatedb_storage::{SlatedbStorage, object_store::memory::InMemory},
    std::sync::Arc,
    test_suite::*,
};

struct SlatedbTester {
    glue: Glue<SlatedbStorage>,
}

impl Tester<SlatedbStorage> for SlatedbTester {
    fn new(namespace: &str) -> Self {
        let storage = SlatedbStorage::new(Arc::new(InMemory::new()), namespace).unwrap();
        Self {
            glue: Glue::new(storage),
        }
    }

    fn get_glue(&mut self) -> &mut Glue<SlatedbStorage> {
        &mut self.glue
    }
}

generate_store_tests!(test, SlatedbTester);
generate_transaction_tests!(test, SlatedbTester);
generate_alter_table_tests!(test, SlatedbTester);
generate_transaction_alter_table_tests!(test, SlatedbTester);
