#![allow(clippy::future_not_send)]

mod alter_table;
mod index;
mod key;
mod query;
mod storage_error;
mod store;
mod store_mut;
mod transaction;

use {
    gluesql_core::result::Result,
    idb::{Database, Factory, ObjectStoreParams},
};

pub struct IndexeddbStorage {
    database: Database,
    id_ctr: u32,
}

const SCHEMA_STORE: &str = "schemas";
const DATA_STORE: &str = "data";

impl IndexeddbStorage {
    /// # Errors
    /// TODO
    /// # Panics
    /// TODO
    pub async fn new(name: &str) -> Result<Self> {
        let factory = Factory::new().unwrap();

        let mut open_request = factory.open(name, 1).unwrap();

        open_request.on_upgrade_needed(|event| {
            let database = event.database().unwrap(); // TODO: error handling

            let mut schemas_params = ObjectStoreParams::new();
            schemas_params.auto_increment(true);
            schemas_params.key_path(Some(idb::KeyPath::new_single("table_name")));

            database
                .create_object_store(SCHEMA_STORE, schemas_params)
                .unwrap();

            let mut data_params = ObjectStoreParams::new();
            data_params.auto_increment(false); // TODO Check default

            database
                .create_object_store(DATA_STORE, data_params)
                .unwrap();
        });

        let database = open_request.await.unwrap();

        let id_ctr = store_count(&database, DATA_STORE).await.unwrap();

        Ok(Self { database, id_ctr })
    }
}

async fn store_count(database: &Database, store: &str) -> Result<u32> {
    let transaction = database
        .transaction(&[store], idb::TransactionMode::ReadOnly)
        .unwrap();

    let store = transaction.object_store(store).unwrap();

    let count = store.count(None).await.unwrap();

    transaction.done().await.unwrap();

    Ok(count)
}
