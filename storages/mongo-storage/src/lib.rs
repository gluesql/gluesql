mod description;
pub mod error;
pub mod row;
mod store;
mod store_mut;
pub mod utils;

use {
    error::ResultExt,
    gluesql_core::{
        error::Result,
        store::{
            AlterTable, CustomFunction, CustomFunctionMut, Index, IndexMut, Metadata, Transaction,
        },
    },
    mongodb::{options::ClientOptions, Client, Database},
};

pub struct MongoStorage {
    pub db: Database,
}

impl MongoStorage {
    pub async fn new(conn_str: &str, db_name: &str) -> Result<Self> {
        let client_options = ClientOptions::parse(conn_str).await.map_storage_err()?;
        let client = Client::with_options(client_options).map_storage_err()?;
        let db = client.database(db_name);

        Ok(Self { db })
    }

    pub async fn drop_database(&self) -> Result<()> {
        self.db.drop(None).await.map_storage_err()
    }
}

impl Metadata for MongoStorage {}
impl AlterTable for MongoStorage {}
impl CustomFunction for MongoStorage {}
impl CustomFunctionMut for MongoStorage {}
impl Index for MongoStorage {}
impl IndexMut for MongoStorage {}
impl Transaction for MongoStorage {}
