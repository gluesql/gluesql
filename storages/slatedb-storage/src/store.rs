use {
    crate::{
        SlatedbStorage,
        error::StorageError,
        key::{SCHEMA_PREFIX, data_prefix, row_key, schema_key},
    },
    bincode::deserialize,
    gluesql_core::{
        data::{Key, Schema, Value},
        error::Result,
        store::{RowIter, Store},
    },
    slatedb::{DbIterator, bytes::Bytes},
    std::iter,
};

impl SlatedbStorage {
    fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        let result = match &self.txn {
            Some(txn) => self.runtime.block_on(txn.get(key)),
            None => self.runtime.block_on(self.db.get(key)),
        };

        result.map_err(|error| StorageError::SlateDb(error).into())
    }

    fn scan(&self, prefix: &[u8]) -> Result<DbIterator> {
        let result = match &self.txn {
            Some(txn) => self.runtime.block_on(txn.scan_prefix(prefix, ..)),
            None => self.runtime.block_on(self.db.scan_prefix(prefix, ..)),
        };

        result.map_err(|error| StorageError::SlateDb(error).into())
    }
}

impl Store for SlatedbStorage {
    fn fetch_schema(&self, table_name: &str) -> Result<Option<Schema>> {
        let key = schema_key(table_name)?;
        let Some(value) = self.get(&key)? else {
            return Ok(None);
        };

        let schema = deserialize(&value).map_err(StorageError::Bincode)?;
        Ok(Some(schema))
    }

    fn fetch_all_schemas(&self) -> Result<Vec<Schema>> {
        let mut iter = self.scan(SCHEMA_PREFIX)?;
        let mut schemas = Vec::new();

        while let Some(entry) = self
            .runtime
            .block_on(iter.next())
            .map_err(StorageError::SlateDb)?
        {
            let schema = deserialize(&entry.value).map_err(StorageError::Bincode)?;
            schemas.push(schema);
        }

        Ok(schemas)
    }

    fn fetch_data(&self, table_name: &str, key: &Key) -> Result<Option<Vec<Value>>> {
        let prefix = data_prefix(table_name)?;
        let key = row_key(&prefix, key)?;

        let Some(value) = self.get(&key)? else {
            return Ok(None);
        };

        let (_, row): (Key, Vec<Value>) = deserialize(&value).map_err(StorageError::Bincode)?;
        Ok(Some(row))
    }

    fn scan_data<'a>(&'a self, table_name: &str) -> Result<RowIter<'a>> {
        let prefix = data_prefix(table_name)?;
        let entries = self.scan(&prefix)?;
        let mut entries = Some(entries);

        let rows = iter::from_fn(move || {
            let iter = entries.as_mut()?;
            let entry = match self.runtime.block_on(iter.next()) {
                Ok(Some(entry)) => entry,
                Ok(None) => {
                    entries = None;
                    return None;
                }
                Err(error) => {
                    entries = None;
                    return Some(Err(StorageError::SlateDb(error).into()));
                }
            };

            match deserialize(&entry.value) {
                Ok(row) => Some(Ok(row)),
                Err(error) => {
                    entries = None;
                    Some(Err(StorageError::Bincode(error).into()))
                }
            }
        });

        Ok(Box::new(rows))
    }
}
