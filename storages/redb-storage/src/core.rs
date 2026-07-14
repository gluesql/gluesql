use {
    super::{
        error::StorageError,
        index_sync::{delete_index_table, prepare_delete, prepare_insert, prepare_update},
        migration::{ensure_storage_format_version_supported, initialize_storage_format_version},
    },
    bincode::{deserialize, serialize},
    gluesql_core::{
        data::{Key, Schema, Value},
        error::IndexError,
    },
    redb::{Builder, Database, ReadableTable, TableDefinition, WriteTransaction},
    std::{collections::HashMap, path::Path},
    uuid::Uuid,
};

pub(super) const SCHEMA_TABLE_NAME: &str = "__SCHEMA__";
pub(super) const STORAGE_META_TABLE_NAME: &str = "__GLUESQL_META__";
const SCHEMA_TABLE: TableDefinition<&str, Vec<u8>> = TableDefinition::new(SCHEMA_TABLE_NAME);

type Result<T> = std::result::Result<T, StorageError>;
type RedbRowIter<'a> = Box<dyn Iterator<Item = Result<(Key, Vec<Value>)>> + 'a>;

pub enum TransactionState {
    None,
    Active {
        txn: Box<WriteTransaction>,
        autocommit: bool,
    },
}

pub struct StorageCore {
    db: Database,
    state: TransactionState,
}

impl StorageCore {
    pub fn new<P: AsRef<Path>>(filename: P) -> Result<Self> {
        let path = filename.as_ref();
        let db = if path.exists() {
            let db = Database::open(path)?;
            ensure_storage_format_version_supported(&db)?;
            db
        } else {
            let db = Builder::new()
                .create_with_file_format_v3(true)
                .create(path)?;
            initialize_storage_format_version(&db)?;
            db
        };

        Ok(Self {
            db,
            state: TransactionState::None,
        })
    }

    pub fn from_database(db: Database) -> Result<Self> {
        ensure_storage_format_version_supported(&db)?;

        Ok(Self {
            db,
            state: TransactionState::None,
        })
    }

    pub(super) fn data_table_def(
        table_name: &str,
    ) -> Result<TableDefinition<'_, &'static [u8], Vec<u8>>> {
        if matches!(table_name, SCHEMA_TABLE_NAME | STORAGE_META_TABLE_NAME) {
            return Err(StorageError::ReservedTableName(table_name.to_owned()));
        }

        Ok(TableDefinition::new(table_name))
    }

    pub(super) fn txn(&self) -> Result<&WriteTransaction> {
        match &self.state {
            TransactionState::Active { txn, .. } => Ok(txn),
            TransactionState::None => Err(StorageError::TransactionNotFound),
        }
    }

    pub(super) fn txn_mut(&mut self) -> Result<&mut WriteTransaction> {
        match &mut self.state {
            TransactionState::Active { txn, .. } => Ok(txn),
            TransactionState::None => Err(StorageError::TransactionNotFound),
        }
    }

    fn take_txn(&mut self) -> Option<WriteTransaction> {
        match std::mem::replace(&mut self.state, TransactionState::None) {
            TransactionState::Active { txn, .. } => Some(*txn),
            TransactionState::None => None,
        }
    }

    pub(super) fn explicit_txn(&self) -> Option<&WriteTransaction> {
        match &self.state {
            TransactionState::Active {
                txn,
                autocommit: false,
            } => Some(txn),
            TransactionState::Active {
                autocommit: true, ..
            }
            | TransactionState::None => None,
        }
    }

    pub(super) fn database(&self) -> &Database {
        &self.db
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

    pub fn fetch_data(&self, table_name: &str, key: &Key) -> Result<Option<Vec<Value>>> {
        let txn = self.txn()?;
        let table_def = Self::data_table_def(table_name)?;
        let table = txn.open_table(table_def)?;

        let key = key.to_cmp_be_bytes()?;
        let key = key.as_slice();
        let row = table
            .get(key)?
            .map(|v| deserialize(&v.value()))
            .transpose()?
            .map(|(_, row): (Key, Vec<Value>)| row);

        Ok(row)
    }

    pub fn scan_data<'a>(&'a self, table_name: &str) -> Result<RedbRowIter<'a>> {
        if let TransactionState::Active { autocommit, txn } = &self.state
            && !autocommit
        {
            let table_def = Self::data_table_def(table_name)?;
            let table = txn.open_table(table_def)?;

            let rows: Vec<_> = table
                .iter()?
                .map(|entry| {
                    let value = entry?.1.value();
                    let (key, row): (Key, Vec<Value>) = deserialize(&value)?;

                    Ok((key, row))
                })
                .collect::<Result<_>>()?;

            return Ok(Box::new(rows.into_iter().map(Ok)));
        }

        let read_txn = self.db.begin_read()?;
        let table_def = Self::data_table_def(table_name)?;
        let table = read_txn.open_table(table_def)?;

        let rows = table.range::<&[u8]>(..)?.map(|entry| -> Result<_> {
            let value = entry?.1.value();
            let (key, row): (Key, Vec<Value>) = deserialize(&value)?;

            Ok((key, row))
        });

        Ok(Box::new(rows))
    }
}

// StoreMut
impl StorageCore {
    pub fn insert_schema(&mut self, schema: &Schema) -> Result<()> {
        let data_def = Self::data_table_def(&schema.table_name)?;
        let txn = self.txn_mut()?;
        let mut table = txn.open_table(SCHEMA_TABLE)?;
        let value = serialize(&schema)?;
        table.insert(schema.table_name.as_str(), value)?;
        txn.open_table(data_def)?;

        Ok(())
    }

    pub fn delete_schema(&mut self, table_name: &str) -> Result<()> {
        let schema = self.fetch_schema(table_name)?;
        let table_def = Self::data_table_def(table_name)?;
        let txn = self.txn_mut()?;

        if let Some(schema) = schema {
            for index in schema.indexes {
                delete_index_table(txn, table_name, &index.name).map_err(StorageError::from)?;
            }
        }

        let mut table = txn.open_table(SCHEMA_TABLE)?;
        table.remove(table_name)?;
        drop(table);
        txn.delete_table(table_def)?;

        Ok(())
    }

    pub fn append_data(&mut self, table_name: &str, rows: Vec<Vec<Value>>) -> Result<()> {
        let schema = self.fetch_schema(table_name)?.ok_or_else(|| {
            StorageError::Glue(IndexError::ConflictTableNotFound(table_name.to_owned()).into())
        })?;
        let rows = rows
            .into_iter()
            .map(|row| {
                let key = Key::Uuid(Uuid::now_v7().as_u128());
                let table_key = key.to_cmp_be_bytes()?;

                Ok((table_key, row, key))
            })
            .collect::<Result<Vec<_>>>()?;
        let index_rows = rows
            .iter()
            .map(|(table_key, row, _)| (table_key.clone(), row.clone()))
            .collect::<Vec<_>>();
        let index_changes = prepare_insert(&schema, &index_rows).map_err(StorageError::from)?;
        let table_def = Self::data_table_def(table_name)?;
        let txn = self.txn_mut()?;
        index_changes.apply(txn).map_err(StorageError::from)?;
        let mut table = txn.open_table(table_def)?;

        for (table_key, row, key) in rows {
            let value = serialize(&(&key, row))?;
            table.insert(table_key.as_slice(), value)?;
        }

        Ok(())
    }

    pub fn insert_data(&mut self, table_name: &str, rows: Vec<(Key, Vec<Value>)>) -> Result<()> {
        let schema = self.fetch_schema(table_name)?.ok_or_else(|| {
            StorageError::Glue(IndexError::ConflictTableNotFound(table_name.to_owned()).into())
        })?;
        let table_def = Self::data_table_def(table_name)?;
        let txn = self.txn_mut()?;
        let table = txn.open_table(table_def)?;
        let mut pending_rows: HashMap<Vec<u8>, Vec<Value>> = HashMap::new();
        let rows = rows
            .into_iter()
            .map(|(key, row)| {
                let table_key = key.to_cmp_be_bytes()?;
                let old_row = if let Some(pending_row) = pending_rows.get(&table_key) {
                    Some(pending_row.clone())
                } else {
                    table
                        .get(table_key.as_slice())?
                        .map(|value| deserialize(&value.value()))
                        .transpose()?
                        .map(|(_, row): (Key, Vec<Value>)| row)
                };
                pending_rows.insert(table_key.clone(), row.clone());

                Ok((table_key, old_row, row, key))
            })
            .collect::<Result<Vec<_>>>()?;
        drop(table);
        let index_rows = rows
            .iter()
            .map(|(table_key, old_row, row, _)| (table_key.clone(), old_row.clone(), row.clone()))
            .collect::<Vec<_>>();
        let index_changes = prepare_update(&schema, &index_rows).map_err(StorageError::from)?;
        index_changes.apply(txn).map_err(StorageError::from)?;
        let mut table = txn.open_table(table_def)?;

        for (table_key, _, row, key) in rows {
            let value = serialize(&(&key, row))?;
            table.insert(table_key.as_slice(), value)?;
        }

        Ok(())
    }

    pub fn delete_data(&mut self, table_name: &str, keys: Vec<Key>) -> Result<()> {
        let schema = self.fetch_schema(table_name)?.ok_or_else(|| {
            StorageError::Glue(IndexError::ConflictTableNotFound(table_name.to_owned()).into())
        })?;
        let table_def = Self::data_table_def(table_name)?;
        let txn = self.txn_mut()?;
        let table = txn.open_table(table_def)?;
        let mut table_keys = Vec::with_capacity(keys.len());
        let mut index_rows = Vec::with_capacity(keys.len());

        for key in keys {
            let table_key = key.to_cmp_be_bytes()?;
            let old_row = table
                .get(table_key.as_slice())?
                .map(|value| deserialize(&value.value()))
                .transpose()?
                .map(|(_, row): (Key, Vec<Value>)| row);

            if let Some(row) = old_row {
                index_rows.push((table_key.clone(), row));
            }
            table_keys.push(table_key);
        }
        drop(table);
        let index_changes = prepare_delete(&schema, &index_rows).map_err(StorageError::from)?;
        index_changes.apply(txn).map_err(StorageError::from)?;
        let mut table = txn.open_table(table_def)?;

        for table_key in table_keys {
            table.remove(table_key.as_slice())?;
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
                    txn: Box::new(write_txn),
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
