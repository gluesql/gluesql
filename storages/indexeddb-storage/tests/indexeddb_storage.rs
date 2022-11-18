#![cfg(target_arch = "wasm32")]
#![allow(clippy::future_not_send)]

use {
    async_trait::async_trait,
    gluesql_core::prelude::Glue,
    gluesql_indexeddb_storage::IndexeddbStorage,
    serde::ser::Serialize,
    serde_wasm_bindgen::Serializer,
    test_suite::*,
    test_suite::{generate_store_tests, Tester},
    wasm_bindgen_test::{console_log, wasm_bindgen_test},
};

struct IndexeddbTester {
    glue: Glue<IndexeddbStorage>,
}

#[async_trait(?Send)]
impl Tester<IndexeddbStorage> for IndexeddbTester {
    async fn new(namespace: &str) -> Self {
        remove_old(namespace).await;

        let storage = IndexeddbStorage::new(namespace).await.unwrap();

        let glue = Glue::new(storage);

        IndexeddbTester { glue }
    }

    fn get_glue(&mut self) -> &mut Glue<IndexeddbStorage> {
        &mut self.glue
    }
}

async fn remove_old(database: &str) {
    let factory = idb::Factory::new().unwrap();
    factory.delete(database).await.ok();
}

#[macro_export]
macro_rules! declare_test_fn {
    ($test: meta, $storage: ident, $title: ident, $func: path) => {
        #[wasm_bindgen_test]
        async fn $title() {
            let path = stringify!($title);
            let storage = $storage::new(path).await;

            $func(storage).await;
        }
    };
}

#[cfg(any(feature = "alter-table", feature = "index", feature = "transaction"))]
macro_rules! exec {
    ($glue: ident $sql: literal) => {
        $glue.execute_async($sql).await.unwrap();
    };
}

#[cfg(any(feature = "alter-table", feature = "index", feature = "transaction"))]
macro_rules! test {
    ($glue: ident $sql: literal, $result: expr) => {
        assert_eq!($glue.execute_async($sql).await, $result);
    };
}

generate_store_tests!(tokio::test, IndexeddbTester);

#[wasm_bindgen_test]
async fn first_test() {
    // use futures::executor::block_on;
    use gluesql_core::prelude::Glue;

    let serializer = Serializer::new().serialize_large_number_types_as_bigints(true);

    let x = 9223372036854775807_i64;
    let x = x.serialize(&serializer);
    console_log!("Result: {:?}", x);

    let storage = IndexeddbStorage::new("test").await.unwrap();

    let mut glue = Glue::new(storage);

    let sqls = vec![
        "DROP TABLE IF EXISTS Glue;",
        "CREATE TABLE Glue (id INTEGER);",
        "INSERT INTO Glue VALUES (100);",
        "INSERT INTO Glue VALUES (200);",
        "SELECT * FROM Glue WHERE id > 10;",
    ];

    // let sqls = vec![
    //     "DROP TABLE IF EXISTS Glue;",
    //     "CREATE TABLE Glue (id INTEGER);",
    //     // "INSERT INTO Glue VALUES (100);",
    //     "DELETE FROM Glue;",
    // ];

    for sql in sqls {
        let output = glue.execute_async(sql).await.unwrap();
        console_log!("{:?}", output);
    }
}

#[cfg(feature = "transaction")]
#[wasm_bindgen_test]
async fn indexeddb_storage_transaction() {
    use gluesql_core::{prelude::Glue, result::Error};

    remove_old("transaction").await;
    let storage = IndexeddbStorage::new("transaction").await.unwrap();
    let mut glue = Glue::new(storage);

    exec!(glue "CREATE TABLE TxTest (id INTEGER);");
    test!(glue "BEGIN", Err(Error::StorageMsg("[IndexeddbStorage] transaction is not supported".to_owned())));
    test!(glue "COMMIT", Err(Error::StorageMsg("[IndexeddbStorage] transaction is not supported".to_owned())));
    test!(glue "ROLLBACK", Err(Error::StorageMsg("[IndexeddbStorage] transaction is not supported".to_owned())));
}

#[cfg(feature = "alter-table")]
#[wasm_bindgen_test]
async fn indexeddb_storage_alter_table() {
    use gluesql_core::{prelude::Glue, result::Error};

    remove_old("alter_table").await;
    let storage = IndexeddbStorage::new("alter_table").await.unwrap();
    let mut glue = Glue::new(storage);

    exec!(glue "CREATE TABLE ATTest (id INTEGER);");
    test!(glue "ALTER TABLE ATTest ADD COLUMN c INT", Err(Error::StorageMsg("[IndexeddbStorage] AlterTable is not supported".to_owned())));
    test!(glue "ALTER TABLE ATTest RENAME COLUMN c TO d", Err(Error::StorageMsg("[IndexeddbStorage] AlterTable is not supported".to_owned())));
    test!(glue "ALTER TABLE ATTest DROP COLUMN d", Err(Error::StorageMsg("[IndexeddbStorage] AlterTable is not supported".to_owned())));
}

#[cfg(feature = "index")]
#[wasm_bindgen_test]
async fn indexeddb_storage_index() {
    use gluesql_core::{prelude::Glue, result::Error};

    remove_old("index").await;
    let storage = IndexeddbStorage::new("index").await.unwrap();
    let mut glue = Glue::new(storage);

    exec!(glue "CREATE TABLE ITest (id INTEGER);");
    test!(glue "Create INDEX ITTest_id ON ITest (id)", Err(Error::StorageMsg("[IndexeddbStorage] index is not supported".to_owned())));
    test!(glue "DROP INDEX ITest.ITTest_id", Err(Error::StorageMsg("[IndexeddbStorage] index is not supported".to_owned())));
}
