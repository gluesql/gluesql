mod alter_table;
pub mod error;
mod function;
mod index;
mod store;
mod store_mut;
mod transaction;
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
    pub async fn new(conn_str: &str) -> Result<Self> {
        let client_options = ClientOptions::parse(conn_str).await.map_storage_err()?;

        let client = Client::with_options(client_options).map_storage_err()?;
        let db = client.database("gluedb"); // should be by parameter

        Ok(Self { db })
    }
}

impl Metadata for MongoStorage {}
