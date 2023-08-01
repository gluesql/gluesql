mod alter_table;
pub mod error;
mod function;
mod index;
mod store;
mod store_mut;
mod transaction;
mod utils;
mod value;

use {
    error::ResultExt,
    gluesql_core::{error::Result, store::Metadata},
};

use mongodb::{options::ClientOptions, Client, Database};

pub struct MongoStorage {
    pub db: Database,
}

impl MongoStorage {
    pub async fn new(conn_str: &str, db_name: &str) -> Result<Self> {
        let client_options = ClientOptions::parse(conn_str).await.map_storage_err()?;
        let client = Client::with_options(client_options).map_storage_err()?;

        client
            .database(&db_name)
            .drop(None)
            .await
            .map_storage_err()?;
        // clien
        //     .database(&db_name).crea
        //     .create(None)
        //     .await
        //     .map_storage_err()?;
        let db = client.database(db_name); // should be by parameter

        Ok(Self { db })
    }
}

impl Metadata for MongoStorage {}
