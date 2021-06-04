#![cfg(feature = "index")]

use {
    super::{
        err_into,
        index_sync::{build_index_key, build_index_key_prefix},
        SledStorage,
    },
    crate::{
        ast::IndexOperator,
        result::Result,
        store::{Index, RowIter},
        utils::Vector,
        IndexError, Value,
    },
    async_trait::async_trait,
    iter_enum::Iterator,
    sled::IVec,
    std::iter::{empty, once},
};

#[async_trait(?Send)]
impl Index<IVec> for SledStorage {
    async fn scan_indexed_data(
        &self,
        table_name: &str,
        index_name: &str,
        op: &IndexOperator,
        value: Value,
    ) -> Result<RowIter<IVec>> {
        let index_data_ids = {
            #[derive(Iterator)]
            enum DataIds<I1, I2, I3, I4> {
                Empty(I1),
                Once(I2),
                RangeGt(I3),
                Range(I4),
            }

            let map = |item: std::result::Result<_, _>| item.map(|(_, v)| v);
            let lower = || build_index_key_prefix(table_name, index_name);
            let upper = || {
                build_index_key_prefix(table_name, index_name)
                    .into_iter()
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
            let key = build_index_key(table_name, index_name, value);

            match op {
                IndexOperator::Eq => match self.tree.get(&key).transpose() {
                    Some(v) => DataIds::Once(once(v)),
                    None => DataIds::Empty(empty()),
                },
                IndexOperator::Gt => {
                    DataIds::RangeGt(self.tree.range(key..upper()).skip(1).map(map))
                }
                IndexOperator::GtEq => DataIds::Range(self.tree.range(key..upper()).map(map)),
                IndexOperator::Lt => DataIds::Range(self.tree.range(lower()..key).map(map)),
                IndexOperator::LtEq => DataIds::Range(self.tree.range(lower()..=key).map(map)),
            }
        };

        let tree = self.tree.clone();
        let rows = index_data_ids.map(|v| v.map_err(err_into)).flat_map(
            move |index_data_id: Result<IVec>| {
                #[derive(Iterator)]
                enum Rows<I1, I2> {
                    Ok(I1),
                    Err(I2),
                }

                let index_data_id: IVec = match index_data_id {
                    Ok(id) => id,
                    Err(e) => {
                        return Rows::Err(once(Err(e)));
                    }
                };

                let bytes = "indexdata/"
                    .to_owned()
                    .into_bytes()
                    .into_iter()
                    .chain(index_data_id.iter().copied())
                    .collect::<Vec<_>>();
                let index_data_prefix = IVec::from(bytes);

                let tree2 = tree.clone();
                let rows = tree
                    .scan_prefix(&index_data_prefix)
                    .map(move |item| -> Result<_> {
                        let (_, data_key) = item.map_err(err_into)?;
                        let value = tree2
                            .get(&data_key)
                            .map_err(err_into)?
                            .ok_or(IndexError::ConflictOnEmptyIndexValueScan)?;
                        let value = bincode::deserialize(&value).map_err(err_into)?;

                        Ok((data_key, value))
                    });

                Rows::Ok(rows)
            },
        );

        Ok(Box::new(rows))
    }
}
