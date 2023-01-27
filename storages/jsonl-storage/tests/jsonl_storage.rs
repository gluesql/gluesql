use {
    async_trait::async_trait, gluesql_core::prelude::Glue, gluesql_jsonl_storage::JsonlStorage,
    std::fs::remove_dir_all, test_suite::*,
};

struct JsonlTester {
    glue: Glue<JsonlStorage>,
}

#[async_trait(?Send)]
impl Tester<JsonlStorage> for JsonlTester {
    async fn new(namespace: &str) -> Self {
        let path = format!("data/{}", namespace);

        if let Err(e) = remove_dir_all(&path) {
            println!("fs::remove_file {:?}", e);
        };

        println!("{path}");
        let storage = JsonlStorage::new(&path).expect("JsonlStorage::new");
        let glue = Glue::new(storage);
        JsonlTester { glue }
    }

    fn get_glue(&mut self) -> &mut Glue<JsonlStorage> {
        &mut self.glue
    }
}

generate_store_tests!(tokio::test, JsonlTester);
