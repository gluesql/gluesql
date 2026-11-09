use {
    crate::{
        SlatedbStorage,
        error::StorageError,
        key::{data_prefix, row_key, schema_key},
    },
    bincode::serialize,
    gluesql_core::{
        data::{Key, Schema, Value},
        error::Result,
        store::StoreMut,
    },
    uuid::Uuid,
};

impl StoreMut for SlatedbStorage {
    fn insert_schema(&mut self, schema: &Schema) -> Result<()> {
        let key = schema_key(&schema.table_name)?;
        let txn = self.txn()?;
        let value = serialize(schema).map_err(StorageError::Bincode)?;

        txn.put(key, value).map_err(StorageError::SlateDb)?;
        Ok(())
    }

    fn delete_schema(&mut self, table_name: &str) -> Result<()> {
        let key = schema_key(table_name)?;
        let prefix = data_prefix(table_name)?;
        let txn = self.txn()?;

        let mut iter = self
            .runtime
            .block_on(txn.scan_prefix(prefix, ..))
            .map_err(StorageError::SlateDb)?;

        while let Some(entry) = self
            .runtime
            .block_on(iter.next())
            .map_err(StorageError::SlateDb)?
        {
            txn.delete(entry.key).map_err(StorageError::SlateDb)?;
        }

        txn.delete(key).map_err(StorageError::SlateDb)?;
        Ok(())
    }

    fn append_data(&mut self, table_name: &str, rows: Vec<Vec<Value>>) -> Result<()> {
        let rows = rows
            .into_iter()
            .map(|row| {
                let key = Key::Uuid(Uuid::now_v7().as_u128());
                (key, row)
            })
            .collect();

        self.insert_data(table_name, rows)
    }

    fn insert_data(&mut self, table_name: &str, rows: Vec<(Key, Vec<Value>)>) -> Result<()> {
        let prefix = data_prefix(table_name)?;
        let txn = self.txn()?;

        for (key, row) in rows {
            let encoded_key = row_key(&prefix, &key)?;
            let value = serialize(&(key, row)).map_err(StorageError::Bincode)?;

            txn.put(encoded_key, value).map_err(StorageError::SlateDb)?;
        }

        Ok(())
    }

    fn delete_data(&mut self, table_name: &str, keys: Vec<Key>) -> Result<()> {
        let prefix = data_prefix(table_name)?;
        let txn = self.txn()?;

        for key in keys {
            let key = row_key(&prefix, &key)?;
            txn.delete(key).map_err(StorageError::SlateDb)?;
        }

        Ok(())
    }
}
