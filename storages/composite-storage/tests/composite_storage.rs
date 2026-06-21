use {
    gluesql_composite_storage::CompositeStorage, gluesql_core::prelude::Glue,
    gluesql_memory_storage::MemoryStorage, test_suite::*,
};

struct CompositeTester {
    glue: Glue<CompositeStorage>,
}

impl Tester<CompositeStorage> for CompositeTester {
    fn new(_: &str) -> Self {
        let mut storage = CompositeStorage::default();
        storage.push("MEMORY", MemoryStorage::default());
        storage.set_default("MEMORY");

        let glue = Glue::new(storage);

        Self { glue }
    }

    fn get_glue(&mut self) -> &mut Glue<CompositeStorage> {
        &mut self.glue
    }
}

generate_store_tests!(test, CompositeTester);
generate_alter_table_tests!(test, CompositeTester);
