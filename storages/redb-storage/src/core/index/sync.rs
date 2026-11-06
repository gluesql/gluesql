use {
    super::{evaluate_index_key, index_columns, index_table_def, index_table_name},
    crate::core::{Result, StorageCore},
    bincode::{deserialize, serialize},
    gluesql_core::{
        data::{Key, Schema, Value},
        plan::ExprPlan,
    },
    redb::{ReadableTable, WriteTransaction},
    std::collections::HashMap,
    uuid::Uuid,
};

impl StorageCore {
    pub(in crate::core) fn append_indexed_data(
        &mut self,
        schema: &Schema,
        rows: Vec<Vec<Value>>,
    ) -> Result<()> {
        let indexes = IndexSync::new(schema);
        let rows = rows
            .into_iter()
            .map(|row| {
                let key = Key::Uuid(Uuid::now_v7().as_u128());
                let index_keys = indexes.keys(&row)?;
                Ok((key.to_cmp_be_bytes()?, serialize(&(&key, row))?, index_keys))
            })
            .collect::<Result<Vec<_>>>()?;
        let txn = self.txn()?;
        let mut data = txn.open_table(Self::data_table_def(&schema.table_name)?)?;
        for (key, payload, index_keys) in rows {
            indexes.insert(txn, &key, &index_keys)?;
            data.insert(key.as_slice(), payload)?;
        }
        Ok(())
    }

    pub(in crate::core) fn insert_indexed_data(
        &mut self,
        schema: &Schema,
        rows: Vec<(Key, Vec<Value>)>,
    ) -> Result<()> {
        let indexes = IndexSync::new(schema);
        let txn = self.txn()?;
        let mut data = txn.open_table(Self::data_table_def(&schema.table_name)?)?;
        // The previous input for a repeated key is the row being replaced.
        let mut previous = HashMap::new();
        let mut updates = Vec::with_capacity(rows.len());
        for (key, row) in rows {
            let encoded_key = key.to_cmp_be_bytes()?;
            let old_keys = match previous.remove(&encoded_key) {
                Some(keys) => Some(keys),
                None => data
                    .get(encoded_key.as_slice())?
                    .map(|payload| {
                        let (_, row): (Key, Vec<Value>) = deserialize(&payload.value())?;
                        indexes.keys(&row)
                    })
                    .transpose()?,
            };
            let new_keys = indexes.keys(&row)?;
            let payload = serialize(&(&key, row))?;
            previous.insert(encoded_key.clone(), new_keys.clone());
            updates.push((encoded_key, payload, old_keys, new_keys));
        }
        for (key, payload, old_keys, new_keys) in updates {
            if let Some(old_keys) = old_keys {
                indexes.remove(txn, &key, &old_keys)?;
            }
            indexes.insert(txn, &key, &new_keys)?;
            data.insert(key.as_slice(), payload)?;
        }
        Ok(())
    }

    pub(in crate::core) fn delete_indexed_data(
        &mut self,
        schema: &Schema,
        keys: Vec<Key>,
    ) -> Result<()> {
        let indexes = IndexSync::new(schema);
        let txn = self.txn()?;
        let mut data = txn.open_table(Self::data_table_def(&schema.table_name)?)?;
        let mut removals = Vec::with_capacity(keys.len());
        for key in keys {
            let key = key.to_cmp_be_bytes()?;
            if let Some(payload) = data.get(key.as_slice())? {
                let (_, row): (Key, Vec<Value>) = deserialize(&payload.value())?;
                removals.push((key, indexes.keys(&row)?));
            }
        }
        for (key, index_keys) in removals {
            indexes.remove(txn, &key, &index_keys)?;
            data.remove(key.as_slice())?;
        }
        Ok(())
    }
}

struct IndexSync {
    columns: Vec<String>,
    indexes: Vec<(String, ExprPlan)>,
}

impl IndexSync {
    fn new(schema: &Schema) -> Self {
        Self {
            columns: index_columns(schema),
            indexes: schema
                .indexes
                .iter()
                .map(|index| {
                    (
                        index_table_name(&schema.table_name, &index.name),
                        index.expr.clone().into(),
                    )
                })
                .collect(),
        }
    }

    fn keys(&self, row: &[Value]) -> Result<Vec<Vec<u8>>> {
        self.indexes
            .iter()
            .map(|(_, expr)| evaluate_index_key(expr, &self.columns, row))
            .collect()
    }

    fn insert(&self, txn: &WriteTransaction, key: &[u8], values: &[Vec<u8>]) -> Result<()> {
        for ((name, _), value) in self.indexes.iter().zip(values) {
            txn.open_multimap_table(index_table_def(name))?
                .insert(value.as_slice(), key)?;
        }
        Ok(())
    }

    fn remove(&self, txn: &WriteTransaction, key: &[u8], values: &[Vec<u8>]) -> Result<()> {
        for ((name, _), value) in self.indexes.iter().zip(values) {
            txn.open_multimap_table(index_table_def(name))?
                .remove(value.as_slice(), key)?;
        }
        Ok(())
    }
}
