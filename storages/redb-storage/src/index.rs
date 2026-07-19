use {
    crate::{RedbStorage, core::StorageCore, error::StorageError, index_sync::index_table_name},
    bincode::deserialize,
    gluesql_core::{
        ast::IndexOperator,
        data::{Key, Value},
        error::{Error, IndexError, Result},
        store::{Index, RowIter},
    },
    redb::{
        MultimapRange, MultimapTableDefinition, ReadableMultimapTable, ReadableTable,
        TableDefinition,
    },
};

fn index_table_def(table_name: &str) -> MultimapTableDefinition<'_, &'static [u8], &'static [u8]> {
    MultimapTableDefinition::new(table_name)
}

fn collect_indexed_rows<M, D>(
    index_table: &M,
    data_table: &D,
    asc: Option<bool>,
    cmp_value: Option<(&IndexOperator, Value)>,
) -> Result<Vec<(Key, Vec<Value>)>>
where
    M: ReadableMultimapTable<&'static [u8], &'static [u8]>,
    D: ReadableTable<&'static [u8], Vec<u8>>,
{
    let collect_range = |range: MultimapRange<'_, &'static [u8], &'static [u8]>| {
        range
            .map(|entry| {
                let (_, values) = entry.map_err(StorageError::from)?;

                values
                    .map(|value| {
                        value
                            .map(|value| value.value().to_vec())
                            .map_err(StorageError::from)
                            .map_err(Error::from)
                    })
                    .collect::<Result<Vec<_>>>()
            })
            .collect::<Result<Vec<_>>>()
            .map(|groups| groups.concat())
    };

    let mut row_keys: Vec<Vec<u8>> = match cmp_value {
        None => collect_range(index_table.iter().map_err(StorageError::from)?)?,
        Some((operator, value)) => {
            let value = value.to_cmp_be_bytes()?;
            let value = value.as_slice();

            match operator {
                IndexOperator::Eq => index_table
                    .get(value)
                    .map_err(StorageError::from)?
                    .map(|value| {
                        value
                            .map(|value| value.value().to_vec())
                            .map_err(StorageError::from)
                            .map_err(Error::from)
                    })
                    .collect::<Result<Vec<_>>>()?,
                IndexOperator::Gt => {
                    let range = index_table
                        .range::<&[u8]>((
                            std::ops::Bound::Excluded(value),
                            std::ops::Bound::Unbounded,
                        ))
                        .map_err(StorageError::from)?;

                    collect_range(range)?
                }
                IndexOperator::GtEq => {
                    let range = index_table
                        .range::<&[u8]>(value..)
                        .map_err(StorageError::from)?;

                    collect_range(range)?
                }
                IndexOperator::Lt => {
                    let range = index_table
                        .range::<&[u8]>(..value)
                        .map_err(StorageError::from)?;

                    collect_range(range)?
                }
                IndexOperator::LtEq => {
                    let range = index_table
                        .range::<&[u8]>(..=value)
                        .map_err(StorageError::from)?;

                    collect_range(range)?
                }
            }
        }
    };

    if asc == Some(false) {
        row_keys.reverse();
    }

    row_keys
        .into_iter()
        .map(|row_key| {
            let value = data_table
                .get(row_key.as_slice())
                .map_err(StorageError::from)?
                .ok_or(IndexError::ConflictOnEmptyIndexValueScan)?
                .value();
            let row = deserialize(&value).map_err(StorageError::from)?;

            Ok(row)
        })
        .collect()
}

impl Index for RedbStorage {
    fn scan_indexed_data<'a>(
        &'a self,
        table_name: &str,
        index_name: &str,
        asc: Option<bool>,
        cmp_value: Option<(&IndexOperator, Value)>,
    ) -> Result<RowIter<'a>> {
        let index_table_name = index_table_name(table_name, index_name);
        let index_table_def = index_table_def(&index_table_name);
        let data_table_def: TableDefinition<'_, &'static [u8], Vec<u8>> =
            StorageCore::data_table_def(table_name).map_err(Error::from)?;

        let rows = if let Some(txn) = self.0.explicit_txn() {
            let index_table = txn
                .open_multimap_table(index_table_def)
                .map_err(StorageError::from)?;
            let data_table = txn.open_table(data_table_def).map_err(StorageError::from)?;

            collect_indexed_rows(&index_table, &data_table, asc, cmp_value)?
        } else {
            let txn = self.0.database().begin_read().map_err(StorageError::from)?;
            let index_table = txn
                .open_multimap_table(index_table_def)
                .map_err(StorageError::from)?;
            let data_table = txn.open_table(data_table_def).map_err(StorageError::from)?;

            collect_indexed_rows(&index_table, &data_table, asc, cmp_value)?
        };

        Ok(Box::new(rows.into_iter().map(Ok)))
    }
}
