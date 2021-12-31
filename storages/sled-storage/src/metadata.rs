#![cfg(feature = "metadata")]

use {
    super::{err_into, lock, SledStorage, Snapshot, State},
    async_trait::async_trait,
    gluesql_core::{
        data::Schema,
        result::{Error, Result},
        store::Metadata,
    },
    std::str,
};

const SCHEMA_PREFIX: &str = "schema/";

#[async_trait(?Send)]
impl Metadata for SledStorage {
    async fn schema_names(&self) -> Result<Vec<String>> {
        let (txid, created_at) = match self.state {
            State::Transaction {
                txid, created_at, ..
            } => (txid, created_at),
            State::Idle => {
                return Err(Error::StorageMsg(
                    "conflict - schema_names failed, lock does not exist".to_owned(),
                ));
            }
        };
        let lock_txid = lock::fetch(&self.tree, txid, created_at, self.tx_timeout)?;

        self.tree
            .scan_prefix(SCHEMA_PREFIX)
            .map(move |item| {
                let (key, value) = item.map_err(err_into)?;
                let snapshot: Snapshot<Schema> = bincode::deserialize(&value).map_err(err_into)?;
                let schema = snapshot.extract(txid, lock_txid);
                if schema.is_none() {
                    return Ok(None);
                }

                str::from_utf8(key.as_ref())
                    .map_err(err_into)?
                    .strip_prefix(SCHEMA_PREFIX)
                    .map(|prefix| Some(prefix.to_owned()))
                    .ok_or_else(|| {
                        Error::StorageMsg(
                            "conflict - schema_names failed, strip_prefix not matched".to_owned(),
                        )
                    })
            })
            .filter_map(|item| item.transpose())
            .collect::<Result<Vec<_>>>()
    }
}
