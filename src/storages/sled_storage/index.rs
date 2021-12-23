use {
    super::{
        err_into,
        index_sync::{build_index_key, build_index_key_prefix},
        lock, SledStorage, Snapshot, State,
    },
    crate::{
        ast::IndexOperator,
        data::Row,
        prelude::Value,
        result::{Error, Result},
        store::{Index, IndexError, RowIter},
        utils::Vector,
    },
    async_trait::async_trait,
    iter_enum::{DoubleEndedIterator, Iterator},
    sled::IVec,
    std::iter::{empty, once},
};

#[async_trait(?Send)]
impl Index<IVec> for SledStorage {
    async fn scan_indexed_data(
        &self,
        table_name: &str,
        index_name: &str,
        asc: Option<bool>,
        cmp_value: Option<(&IndexOperator, Value)>,
    ) -> Result<RowIter<IVec>> {
        let data_keys = {
            #[derive(Iterator, DoubleEndedIterator)]
            enum DataIds<I1, I2, I3, I4> {
                Empty(I1),
                Once(I2),
                Range(I3),
                Full(I4),
            }

            let map = |item: std::result::Result<_, _>| item.map(|(_, v)| v);

            match cmp_value {
                None => {
                    let prefix = build_index_key_prefix(table_name, index_name);

                    DataIds::Full(self.tree.scan_prefix(prefix).map(map))
                }
                Some((op, value)) => {
                    let incr = |key: Vec<u8>| {
                        key.into_iter()
                            .rev()
                            .fold((false, Vector::new()), |(added, upper), v| {
                                match (added, v) {
                                    (true, _) => (added, upper.push(v)),
                                    (false, u8::MAX) => (added, upper.push(v)),
                                    (false, _) => (true, upper.push(v + 1)),
                                }
                            })
                            .1
                            .reverse()
                            .into()
                    };
                    let lower = || build_index_key_prefix(table_name, index_name);
                    let upper = || incr(build_index_key_prefix(table_name, index_name));
                    let key = build_index_key(table_name, index_name, value)?;

                    match op {
                        IndexOperator::Eq => match self.tree.get(&key).transpose() {
                            Some(v) => DataIds::Once(once(v)),
                            None => DataIds::Empty(empty()),
                        },
                        IndexOperator::Gt => {
                            DataIds::Range(self.tree.range(incr(key)..upper()).map(map))
                        }
                        IndexOperator::GtEq => {
                            DataIds::Range(self.tree.range(key..upper()).map(map))
                        }
                        IndexOperator::Lt => DataIds::Range(self.tree.range(lower()..key).map(map)),
                        IndexOperator::LtEq => {
                            DataIds::Range(self.tree.range(lower()..=key).map(map))
                        }
                    }
                }
            }
        };

        let (txid, created_at) = match self.state {
            State::Transaction {
                txid, created_at, ..
            } => (txid, created_at),
            State::Idle => {
                return Err(Error::StorageMsg(
                    "conflict - scan_indexed_data failed, lock does not exist".to_owned(),
                ));
            }
        };
        let lock_txid = lock::fetch(&self.tree, txid, created_at, self.tx_timeout)?;

        let tree = self.tree.clone();
        let flat_map = move |keys: Result<IVec>| {
            #[derive(Iterator)]
            enum Rows<I1, I2> {
                Ok(I1),
                Err(I2),
            }

            macro_rules! try_into {
                ($expr: expr) => {
                    match $expr {
                        Ok(v) => v,
                        Err(e) => {
                            return Rows::Err(once(Err(e)));
                        }
                    }
                };
            }

            let keys = try_into!(keys);
            let keys: Vec<Snapshot<Vec<u8>>> =
                try_into!(bincode::deserialize(&keys).map_err(err_into));

            let tree2 = tree.clone();
            let rows = keys
                .into_iter()
                .map(move |key_snapshot| -> Result<_> {
                    let key = match key_snapshot.extract(txid, lock_txid) {
                        Some(key) => key,
                        None => {
                            return Ok(None);
                        }
                    };

                    let value = tree2
                        .get(&key)
                        .map_err(err_into)?
                        .ok_or(IndexError::ConflictOnEmptyIndexValueScan)?;
                    let snapshot: Snapshot<Row> = bincode::deserialize(&value).map_err(err_into)?;
                    let row = snapshot.extract(txid, lock_txid);
                    let item = row.map(|row| (IVec::from(key), row));

                    Ok(item)
                })
                .filter_map(|item| item.transpose());

            Rows::Ok(rows)
        };

        let data_keys = data_keys.map(|v| v.map_err(err_into));

        Ok(match asc {
            Some(true) | None => Box::new(data_keys.flat_map(flat_map)),
            Some(false) => Box::new(data_keys.rev().flat_map(flat_map)),
        })
    }
}
