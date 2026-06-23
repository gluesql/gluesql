#![cfg(feature = "test-mongo")]

use {gluesql_core::prelude::Glue, gluesql_mongo_storage::MongoStorage, test_suite::*};

struct MongoTester {
    glue: Glue<MongoStorage>,
}

impl Tester<MongoStorage> for MongoTester {
    fn new(namespace: &str) -> Self {
        let conn_str = "mongodb://localhost:27017";
        let storage = MongoStorage::new(conn_str, namespace).expect("MongoStorage::new");
        storage.drop_database().expect("database dropped");
        let glue = Glue::new(storage);

        MongoTester { glue }
    }

    fn get_glue(&mut self) -> &mut Glue<MongoStorage> {
        &mut self.glue
    }
}

generate_store_tests!(test, MongoTester);
generate_alter_table_tests!(test, MongoTester);
