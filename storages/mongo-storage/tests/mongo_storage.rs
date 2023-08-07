use composite_storage::CompositeStorage;
use gluesql_core::prelude::{Error, Glue, Value::I64};

use {async_trait::async_trait, gluesql_mongo_storage::MongoStorage, test_suite::*};

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
        storage.drop_database().await.expect("database dropped");
        let glue = Glue::new(storage);

        MongoTester { glue }
    }

    fn get_glue(&mut self) -> &mut Glue<MongoStorage> {
        &mut self.glue
    }
}

generate_store_tests!(tokio::test, MongoTester);

// #[tokio::test]
// async fn mongo_migrate() {
//     let source = MongoStorage::new("mongodb://localhost:27017", "src")
//         .await
//         .expect("MongoStorage::source");
//     let target = MongoStorage::new("mongodb://localhost:27018", "target")
//         .await
//         .expect("MongoStorage::target");

//     let mut storage = CompositeStorage::new();
//     storage.push("source", source);
//     storage.push("target", target);

//     let mut glue = Glue::new(storage);
//     glue.storage.set_default("source");
//     assert_eq!(
//         glue.execute(
//             "SELECT *
//             FROM GLUE_TABLES;
//         "
//         )
//         .await
//         .unwrap()
//         .into_iter()
//         .next()
//         .unwrap(),
//         select!(
//             fid | bid;
//             I64 | I64;
//             1     5;
//             1     7;
//             2     5;
//             2     7
//         )
//     );
// }
