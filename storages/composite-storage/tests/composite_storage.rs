use {
    async_trait::async_trait, gluesql_composite_storage::CompositeStorage,
    gluesql_core::prelude::Glue, memory_storage::MemoryStorage, test_suite::*,
};

struct CompositeTester {
    glue: Glue<CompositeStorage>,
}

#[async_trait(?Send)]
impl Tester<CompositeStorage> for CompositeTester {
    async fn new(_: &str) -> Self {
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

generate_store_tests!(tokio::test, CompositeTester);
