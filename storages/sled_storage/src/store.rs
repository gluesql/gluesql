use {
    super::{err_into, lock, SledStorage, Snapshot, State},
    crate::{
        data::{Row, Schema},
        result::{Error, Result},
        RowIter, Store,
    },
    async_trait::async_trait,
    sled::IVec,
};

#[async_trait(?Send)]
impl Store<IVec> for SledStorage {
    async fn fetch_schema(&self, table_name: &str) -> Result<Option<Schema>> {
        let (txid, created_at, temp) = match self.state {
            State::Transaction {
                txid, created_at, ..
            } => (txid, created_at, false),
            State::Idle => {
                lock::register(&self.tree).map(|(txid, created_at)| (txid, created_at, true))?
            }
        };
        let lock_txid = lock::fetch(&self.tree, txid, created_at, self.tx_timeout)?;

        let key = format!("schema/{}", table_name);
        let schema = self
            .tree
            .get(key.as_bytes())
            .map_err(err_into)?
            .map(|v| bincode::deserialize(&v))
            .transpose()
            .map_err(err_into)?
            .map(|snapshot: Snapshot<Schema>| snapshot.extract(txid, lock_txid))
            .flatten();

        if temp {
            lock::unregister(&self.tree, txid)?;
        }

        Ok(schema)
    }

    async fn scan_data(&self, table_name: &str) -> Result<RowIter<IVec>> {
        let (txid, created_at) = match self.state {
            State::Transaction {
                txid, created_at, ..
            } => (txid, created_at),
            State::Idle => {
                return Err(Error::StorageMsg(
                    "conflict - scan_data failed, lock does not exist".to_owned(),
                ));
            }
        };
        let lock_txid = lock::fetch(&self.tree, txid, created_at, self.tx_timeout)?;

        let prefix = format!("data/{}/", table_name);
        let result_set = self
            .tree
            .scan_prefix(prefix.as_bytes())
            .map(move |item| {
                let (key, value) = item.map_err(err_into)?;
                let snapshot: Snapshot<Row> = bincode::deserialize(&value).map_err(err_into)?;
                let row = snapshot.extract(txid, lock_txid);
                let item = row.map(|row| (key, row));

                Ok(item)
            })
            .filter_map(|item| item.transpose());

        Ok(Box::new(result_set))
    }
}
