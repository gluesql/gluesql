mod alter_table;
mod error;
#[cfg(feature = "index")]
mod index_sync;
mod store;
mod store_mut;
#[cfg(not(feature = "alter-table"))]
impl crate::AlterTable for SledStorage {}

use {
    crate::{Error, Result, RowIter, Schema},
    error::err_into,
    sled::{self, Config, Db, IVec},
    std::convert::TryFrom,
};

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

fn scan_data(tree: &Db, table_name: &str) -> RowIter<IVec> {
    let prefix = format!("data/{}/", table_name);

    let result_set = tree.scan_prefix(prefix.as_bytes()).map(move |item| {
        let (key, value) = item.map_err(err_into)?;
        let value = bincode::deserialize(&value).map_err(err_into)?;

        Ok((key, value))
    });

    Box::new(result_set)
}
