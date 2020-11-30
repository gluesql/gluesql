mod alter_table;
mod error;
mod store;
mod store_mut;
#[cfg(not(feature = "alter-table"))]
impl crate::AlterTable for SledStorage {}

use sled::{self, Config, Db};
use std::convert::TryFrom;

use crate::{Error, Result, Schema};
use error::err_into;

#[derive(Debug, Clone)]
pub struct SledStorage {
    tree: Db,
}

impl SledStorage {
    pub fn new(filename: &str) -> Result<Self> {
        let tree = sled::open(filename).map_err(err_into)?;

        Ok(Self { tree })
    }
}

impl TryFrom<Config> for SledStorage {
    type Error = Error;

    fn try_from(config: Config) -> Result<Self> {
        let tree = config.open().map_err(err_into)?;

        Ok(Self { tree })
    }
}

fn fetch_schema(tree: &Db, table_name: &str) -> Result<(String, Option<Schema>)> {
    let key = format!("schema/{}", table_name);
    let value = tree.get(&key.as_bytes()).map_err(err_into)?;
    let schema = value
        .map(|v| bincode::deserialize(&v))
        .transpose()
        .map_err(err_into)?;

    Ok((key, schema))
}
