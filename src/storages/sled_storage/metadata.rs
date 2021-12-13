#![cfg(feature = "metadata")]

use {
    super::{err_into, lock, SledStorage, Snapshot, State},
    crate::{
        data::Schema,
        result::{Error, Result},
        store::Metadata,
    },
    async_trait::async_trait,
    std::str,
};

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
        let prefix = "schema/";

        self.tree
            .scan_prefix(prefix)
            .map(move |item| {
                let (key, value) = item.map_err(err_into)?;
                let snapshot: Snapshot<Schema> = bincode::deserialize(&value).map_err(err_into)?;
                let schema = snapshot.extract(txid, lock_txid);

                schema
                    .map(|_| {
                        str::from_utf8(key.as_ref())
                            .map(|s| s.replacen(prefix, "", 1))
                            .map_err(err_into)
                    })
                    .transpose()
            })
            .filter_map(|item| item.transpose())
            .collect::<Result<Vec<_>>>()
    }
}
