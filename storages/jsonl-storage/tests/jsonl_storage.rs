use std::fs::remove_dir_all;

use {
    async_trait::async_trait, gluesql_core::prelude::Glue, gluesql_jsonl_storage::JsonlStorage,
    test_suite::*,
};

struct JsonlTester {
    glue: Glue<JsonlStorage>,
}

#[async_trait(?Send)]
impl Tester<JsonlStorage> for JsonlTester {
    async fn new(_: &str) -> Self {
        let storage = JsonlStorage::default();
        let path = format!("{}/*", storage.path.display());
        if let Err(e) = remove_dir_all(path) {
            // println!("fs::remove_file {:?}", e);
        };
        let glue = Glue::new(storage);

        JsonlTester { glue }
    }

    fn get_glue(&mut self) -> &mut Glue<JsonlStorage> {
        &mut self.glue
    }
}

generate_store_tests!(tokio::test, JsonlTester);
