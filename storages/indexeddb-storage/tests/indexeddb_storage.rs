use async_trait::async_trait;
use wasm_bindgen_test::{console_log, wasm_bindgen_test};

use {glueseql_indexeddb_storage::IndexeddbStorage, gluesql_core::prelude::Glue, test_suite::*};

struct IndexeddbTester {
    glue: Glue<IndexeddbStorage>,
}

// impl Tester<IndexeddbStorage> for IndexeddbTester {
//     fn new(_: &str) -> Self {
//         // TODO Wrap in wasm_bindgen_futures::spawn_local?
//         let storage = IndexeddbStorage::new("test").await.unwrap();
//         let glue = Glue::new(storage);

//         IndexeddbTester { glue }
//     }

//     fn get_glue(&mut self) -> &mut Glue<IndexeddbStorage> {
//         &mut self.glue
//     }
// }

// generate_store_tests!(tokio::test, IndexeddbTester);

#[wasm_bindgen_test]
async fn first_test() {
    // use futures::executor::block_on;
    use gluesql_core::{
        prelude::Glue,
        result::{Error, Result},
        store::{Index, Store},
    };

    let storage = IndexeddbStorage::new("test").await.unwrap();

    let mut glue = Glue::new(storage);

    let sqls = vec![
        "DROP TABLE IF EXISTS Glue;",
        "CREATE TABLE Glue (id INTEGER);",
        "INSERT INTO Glue VALUES (100);",
        "INSERT INTO Glue VALUES (200);",
        "SELECT * FROM Glue WHERE id > 10;",
    ];

    for sql in sqls {
        let output = glue.execute_async(sql).await.unwrap();
        console_log!("{:?}", output)
    }
}

// #[wasm_bindgen_test]
// fn memory_storage_transaction() {
//     use gluesql_core::{prelude::Glue, result::Error};

//     let storage = IndexeddbStorage::default();
//     let mut glue = Glue::new(storage);

//     exec!(glue "CREATE TABLE TxTest (id INTEGER);");
//     test!(glue "BEGIN", Err(Error::StorageMsg("[IndexeddbStorage] transaction is not supported".to_owned())));
//     test!(glue "COMMIT", Err(Error::StorageMsg("[IndexeddbStorage] transaction is not supported".to_owned())));
//     test!(glue "ROLLBACK", Err(Error::StorageMsg("[IndexeddbStorage] transaction is not supported".to_owned())));
// }
