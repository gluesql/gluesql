use {
    super::error::StorageError,
    async_stream::try_stream,
    bincode::{deserialize, serialize},
    futures::stream::iter,
    gluesql_core::{
        data::{Key, Schema},
        store::{DataRow, RowIter},
    },
    redb::{Database, ReadableTable, TableDefinition, WriteTransaction},
    std::path::Path,
    uuid::Uuid,
};

const SCHEMA_TABLE_NAME: &str = "__SCHEMA__";
const SCHEMA_TABLE: TableDefinition<&str, Vec<u8>> = TableDefinition::new(SCHEMA_TABLE_NAME);

type Result<T> = std::result::Result<T, StorageError>;

pub enum TransactionState {
    None,
    Active {
        txn: WriteTransaction,
        autocommit: bool,
    },
}

pub struct StorageCore {
    db: Database,
    state: TransactionState,
}

impl StorageCore {
    pub fn new<P: AsRef<Path>>(filename: P) -> Result<Self> {
        let db = Database::create(filename)?;

        Ok(Self {
            db,
            state: TransactionState::None,
        })
    }

    pub fn from_database(db: Database) -> Self {
        Self {
            db,
            state: TransactionState::None,
        }
    }

    fn data_table_def<'a>(
        &self,
        table_name: &'a str,
    ) -> Result<TableDefinition<'a, &'static [u8], Vec<u8>>> {
        if table_name == SCHEMA_TABLE_NAME {
            return Err(StorageError::ReservedTableName(table_name.to_owned()));
        }

        Ok(TableDefinition::new(table_name))
    }

    fn txn(&self) -> Result<&WriteTransaction> {
        match &self.state {
            TransactionState::Active { txn, .. } => Ok(txn),
            TransactionState::None => Err(StorageError::TransactionNotFound),
        }
    }

    fn txn_mut(&mut self) -> Result<&mut WriteTransaction> {
        match &mut self.state {
            TransactionState::Active { txn, .. } => Ok(txn),
            TransactionState::None => Err(StorageError::TransactionNotFound),
        }
    }

    fn take_txn(&mut self) -> Option<WriteTransaction> {
        match std::mem::replace(&mut self.state, TransactionState::None) {
            TransactionState::Active { txn, .. } => Some(txn),
            TransactionState::None => None,
        }
    }
}

// Store
impl StorageCore {
    pub fn fetch_all_schemas(&self) -> Result<Vec<Schema>> {
        let txn = self.txn()?;
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
        let schema = match &self.state {
            TransactionState::Active { txn, .. } => txn
                .open_table(SCHEMA_TABLE)?
                .get(table_name)?
                .map(|v| deserialize(&v.value())),
            TransactionState::None => self
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
        let txn = self.txn()?;
        let table_def = self.data_table_def(table_name)?;
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

    pub fn scan_data<'a>(&'a self, table_name: &str) -> Result<RowIter<'a>> {
        if let TransactionState::Active { autocommit, txn } = &self.state {
            if !autocommit {
                let table_def = self.data_table_def(table_name)?;
                let table = txn.open_table(table_def)?;

                let rows: Vec<_> = table
                    .iter()?
                    .map(|entry| {
                        let value = entry?.1.value();
                        let (key, row): (Key, DataRow) = deserialize(&value)?;

                        Ok((key, row))
                    })
                    .collect::<Result<_>>()?;

                return Ok(Box::pin(iter(rows.into_iter().map(Ok))));
            }
        }

        let read_txn = self.db.begin_read()?;
        let table_def = self.data_table_def(table_name)?;
        let table = read_txn.open_table(table_def)?;

        let rows = try_stream! {
            for entry in table.iter().map_err(Into::<StorageError>::into)? {
                let value = entry.map_err(Into::<StorageError>::into)?.1.value();
                let (key, row): (Key, DataRow) = deserialize(&value).map_err(Into::<StorageError>::into)?;

                yield (key, row);
            }
        };

        Ok(Box::pin(rows))
    }
}

// StoreMut
impl StorageCore {
    pub async fn insert_schema(&mut self, schema: &Schema) -> Result<()> {
        let data_def = self.data_table_def(&schema.table_name)?;
        let txn = self.txn_mut()?;
        let mut table = txn.open_table(SCHEMA_TABLE)?;
        let value = serialize(&schema)?;
        table.insert(schema.table_name.as_str(), value)?;
        txn.open_table(data_def)?;

        Ok(())
    }

    pub async fn delete_schema(&mut self, table_name: &str) -> Result<()> {
        let table_def = self.data_table_def(table_name)?;
        let txn = self.txn_mut()?;
        let mut table = txn.open_table(SCHEMA_TABLE)?;
        table.remove(table_name)?;
        txn.delete_table(table_def)?;

        Ok(())
    }

    pub async fn append_data(&mut self, table_name: &str, rows: Vec<DataRow>) -> Result<()> {
        let table_def = self.data_table_def(table_name)?;
        let txn = self.txn_mut()?;
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
        let table_def = self.data_table_def(table_name)?;
        let txn = self.txn_mut()?;
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
        let table_def = self.data_table_def(table_name)?;
        let txn = self.txn_mut()?;
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
        match (&self.state, autocommit) {
            (TransactionState::Active { .. }, true) => Ok(false),
            (TransactionState::Active { .. }, false) => {
                Err(StorageError::NestedTransactionNotSupported)
            }
            (TransactionState::None, _) => {
                let write_txn = self.db.begin_write()?;
                self.state = TransactionState::Active {
                    txn: write_txn,
                    autocommit,
                };

                Ok(autocommit)
            }
        }
    }

    pub fn rollback(&mut self) -> Result<()> {
        if let Some(txn) = self.take_txn() {
            txn.abort()?;
        }

        Ok(())
    }

    pub fn commit(&mut self) -> Result<()> {
        if let Some(txn) = self.take_txn() {
            txn.commit()?;
        }

        Ok(())
    }
}
