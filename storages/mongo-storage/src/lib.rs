mod alter_table;
pub mod error;
mod function;
mod index;
mod store;
mod store_mut;
mod transaction;

use {
    async_io::block_on,
    error::{JsonStorageError, OptionExt, ResultExt},
    gluesql_core::{
        ast::ColumnUniqueOption,
        data::{value::HashMapJsonExt, Key, Schema},
        error::{Error, Result},
        store::{DataRow, Metadata, RowIter},
    },
    iter_enum::Iterator,
    serde_json::Value as JsonValue,
    std::{
        collections::HashMap,
        fs::{self, File},
        io::{self, BufRead, Read},
        path::{Path, PathBuf},
    },
};

use mongodb::{options::ClientOptions, Client, Database};

pub struct MongoStorage {
    pub db: Database,
}

impl MongoStorage {
    pub async fn new(conn_str: &str) -> Result<Self> {
        let client_options = ClientOptions::parse("mongodb://localhost:27017")
            .await
            .map_storage_err()?;

        let client = Client::with_options(client_options).map_storage_err()?;
        let db = client.database("gluedb");

        Ok(Self { db })
    }
}

impl Metadata for MongoStorage {}
