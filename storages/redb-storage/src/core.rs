use {
    super::error::StorageError,
    bincode::{deserialize, serialize},
    gluesql_core::{
        data::{Key, Schema},
        store::DataRow,
    },
    redb::{Database, ReadableTable, TableDefinition},
    std::path::Path,
    uuid::Uuid,
};

const SCHEMA_TABLE: TableDefinition<&str, Vec<u8>> = TableDefinition::new("__SCHEMA__");

type Result<T> = std::result::Result<T, StorageError>;

pub struct StorageCore {
    db: Database,
    txn: Option<redb::WriteTransaction>,
}

impl StorageCore {
    pub fn new<P: AsRef<Path>>(filename: P) -> Result<Self> {
        let db = Database::create(filename)?;

        Ok(Self { db, txn: None })
    }

    fn data_table_def(table_name: &str) -> Result<TableDefinition<&'static [u8], Vec<u8>>> {
        // let table_name = format!("data_{}", table_name);
        // todo
        Ok(TableDefinition::new(table_name))
    }
}

// Store
impl StorageCore {
    pub fn fetch_all_schemas(&self) -> Result<Vec<Schema>> {
        let txn = self.txn.as_ref().ok_or(StorageError::TransactionNotFound)?;
        let table = txn.open_table(SCHEMA_TABLE)?;

        table
            .iter()?
            .map(|entry| {
                let value = entry?.1.value();
                let schema = deserialize(&value)?;
                Ok(schema)
            })
            .collect()
    }

    pub fn fetch_schema(&self, table_name: &str) -> Result<Option<Schema>> {
        let schema = match self.txn.as_ref() {
            Some(txn) => txn
                .open_table(SCHEMA_TABLE)?
                .get(table_name)?
                .map(|v| deserialize(&v.value())),
            None => self
                .db
                .begin_write()?
                .open_table(SCHEMA_TABLE)?
                .get(table_name)?
                .map(|v| deserialize(&v.value())),
        }
        .transpose()?;

        Ok(schema)
    }

    pub fn fetch_data(&self, table_name: &str, key: &Key) -> Result<Option<DataRow>> {
        let txn = self.txn.as_ref().ok_or(StorageError::TransactionNotFound)?;
        let table_def = StorageCore::data_table_def(table_name)?;
        let table = txn.open_table(table_def)?;

        let key = key.to_cmp_be_bytes()?;
        let key = key.as_slice();
        let row = table
            .get(key)?
            .map(|v| deserialize(&v.value()))
            .transpose()?
            .map(|(_, row): (Key, DataRow)| row);

        Ok(row)
    }

    // todo: should not be collected
    pub fn scan_data(&self, table_name: &str) -> Result<Vec<(Key, DataRow)>> {
        let txn = self.txn.as_ref().ok_or(StorageError::TransactionNotFound)?;
        let table_def = StorageCore::data_table_def(table_name)?;
        let table = txn.open_table(table_def)?;

        let rows = table
            .iter()?
            .map(|entry| {
                let value = entry?.1.value();
                let (key, row) = deserialize(value.as_ref())?;

                Ok((key, row))
            })
            .collect::<Result<_>>();

        let rows = rows?;
        Ok(rows)
    }
}

// StoreMut
impl StorageCore {
    pub async fn insert_schema(&mut self, schema: &Schema) -> Result<()> {
        let txn = self.txn.as_mut().ok_or(StorageError::TransactionNotFound)?;
        let mut table = txn.open_table(SCHEMA_TABLE)?;
        let value = serialize(&schema)?;
        table.insert(schema.table_name.as_str(), value)?;

        Ok(())
    }

    pub async fn delete_schema(&mut self, table_name: &str) -> Result<()> {
        let txn = self.txn.as_mut().ok_or(StorageError::TransactionNotFound)?;
        let mut table = txn.open_table(SCHEMA_TABLE)?;
        table.remove(table_name)?;

        let table_def = StorageCore::data_table_def(table_name)?;
        txn.delete_table(table_def)?;

        Ok(())
    }

    pub async fn append_data(&mut self, table_name: &str, rows: Vec<DataRow>) -> Result<()> {
        let txn = self.txn.as_mut().ok_or(StorageError::TransactionNotFound)?;
        let table_def = StorageCore::data_table_def(table_name)?;
        let mut table = txn.open_table(table_def)?;

        for row in rows {
            let key = Key::Uuid(Uuid::now_v7().as_u128());
            let value = serialize(&(key.clone(), row))?;
            let table_key = key.to_cmp_be_bytes()?;
            let table_key = table_key.as_slice();
            table.insert(table_key, value)?;
        }

        Ok(())
    }

    pub async fn insert_data(&mut self, table_name: &str, rows: Vec<(Key, DataRow)>) -> Result<()> {
        let txn = self.txn.as_mut().ok_or(StorageError::TransactionNotFound)?;
        let table_def = StorageCore::data_table_def(table_name)?;
        let mut table = txn.open_table(table_def)?;

        for (key, row) in rows {
            let value = serialize(&(key.clone(), row))?;
            let table_key = key.to_cmp_be_bytes()?;
            let table_key = table_key.as_slice();
            table.insert(table_key, value)?;
        }

        Ok(())
    }

    pub async fn delete_data(&mut self, table_name: &str, keys: Vec<Key>) -> Result<()> {
        let txn = self.txn.as_mut().ok_or(StorageError::TransactionNotFound)?;
        let table_def = StorageCore::data_table_def(table_name)?;
        let mut table = txn.open_table(table_def)?;

        for key in keys {
            let table_key = key.to_cmp_be_bytes()?;
            let table_key = table_key.as_slice();
            table.remove(table_key)?;
        }

        Ok(())
    }
}

// Transaction
impl StorageCore {
    pub fn begin(&mut self, autocommit: bool) -> Result<bool> {
        match (self.txn.is_some(), autocommit) {
            (true, true) => Ok(false),
            (true, false) => Err(StorageError::NestedTransactionNotSupported),
            (false, _) => {
                let write_txn = self.db.begin_write()?;
                self.txn = Some(write_txn);

                Ok(autocommit)
            }
        }
    }

    pub fn rollback(&mut self) -> Result<()> {
        if let Some(txn) = self.txn.take() {
            txn.abort()?;
        }

        Ok(())
    }

    pub fn commit(&mut self) -> Result<()> {
        if let Some(txn) = self.txn.take() {
            txn.commit()?;
        }

        Ok(())
    }
}
