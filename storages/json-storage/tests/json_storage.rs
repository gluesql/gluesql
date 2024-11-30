use {
    async_trait::async_trait, gluesql_core::prelude::Glue, gluesql_json_storage::JsonStorage,
    std::fs::remove_dir_all, test_suite::*,
};

struct JsonTester {
    glue: Glue<JsonStorage>,
}

#[async_trait(?Send)]
impl Tester<JsonStorage> for JsonTester {
    async fn new(namespace: &str) -> Self {
        let path = format!("tmp/{namespace}");

        if let Err(e) = remove_dir_all(&path) {
            println!("fs::remove_file {:?}", e);
        };

        let storage = JsonStorage::new(&path).expect("JsonStorage::new");
        let glue = Glue::new(storage);
        JsonTester { glue }
    }

    fn get_glue(&mut self) -> &mut Glue<JsonStorage> {
        &mut self.glue
    }
}

generate_store_tests!(tokio::test, JsonTester);
generate_alter_table_tests!(tokio::test, JsonTester);
