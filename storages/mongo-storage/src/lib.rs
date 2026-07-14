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
            AlterTable, CustomFunction, CustomFunctionMut, Index, IndexMut, Metadata, Planner,
            Transaction,
        },
    },
    mongodb::sync::{Client, Database},
};

pub struct MongoStorage {
    pub db: Database,
}

impl MongoStorage {
    pub fn new(conn_str: &str, db_name: &str) -> Result<Self> {
        let client = Client::with_uri_str(conn_str).map_storage_err()?;
        let db = client.database(db_name);

        Ok(Self { db })
    }

    pub fn drop_database(&self) -> Result<()> {
        self.db.drop(None).map_storage_err()
    }
}

impl Metadata for MongoStorage {}
impl AlterTable for MongoStorage {}
impl CustomFunction for MongoStorage {}
impl CustomFunctionMut for MongoStorage {}
impl Index for MongoStorage {}
impl IndexMut for MongoStorage {}
impl Transaction for MongoStorage {}
impl Planner for MongoStorage {}
