use gluesql_core::store::Store;

use {
    async_trait::async_trait, gluesql_core::prelude::Glue, gluesql_mongo_storage::MongoStorage,
    test_suite::*,
};

struct MongoTester {
    glue: Glue<MongoStorage>,
}

#[async_trait(?Send)]
impl Tester<MongoStorage> for MongoTester {
    async fn new(namespace: &str) -> Self {
        let conn_str = "mongodb://localhost:27017";

        let storage = MongoStorage::new(conn_str, namespace)
            .await
            .expect("MongoStorage::new");
        let glue = Glue::new(storage);
        MongoTester { glue }
    }

    fn get_glue(&mut self) -> &mut Glue<MongoStorage> {
        &mut self.glue
    }
}

generate_store_tests!(tokio::test, MongoTester);

// #[tokio::test]
// async fn mongo() {
//     let conn_str = "mongodb://localhost:27017";
//     let namespace = "unit";
//     let storage = MongoStorage::new(conn_str, namespace)
//         .await
//         .expect("MongoStorage::new");

//     let mut glue = Glue::new(storage);
//     glue.execute("CREATE TABLE Test;").await.expect("execute");

//     let schema = glue
//         .storage
//         .fetch_schema("Test")
//         .await
//         .expect("fetch_schema");

//     assert_eq!(schema, None);
// }
