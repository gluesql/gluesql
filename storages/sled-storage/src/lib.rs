#![deny(clippy::str_to_string)]

mod alter_table;
mod error;
mod gc;
mod index;
mod index_mut;
mod index_sync;
mod key;
mod lock;
mod snapshot;
mod store;
mod store_mut;
mod transaction;

// re-export
pub use sled;

use {
    self::snapshot::Snapshot,
    error::{err_into, tx_err_into},
    gluesql_core::{
        data::{Schema, VectorIndex, VectorIndexType, FloatVector},
        error::{Error, Result},
        store::Metadata,
    },
    sled::{
        Config, Db,
        transaction::{
            ConflictableTransactionError, ConflictableTransactionResult, TransactionalTree,
        },
    },
};

/// default transaction timeout : 1 hour
const DEFAULT_TX_TIMEOUT: u128 = 3600 * 1000;

#[derive(Debug, Clone)]
pub enum State {
    Idle,
    Transaction {
        txid: u64,
        created_at: u128,
        autocommit: bool,
    },
}

#[derive(Debug, Clone)]
pub struct SledStorage {
    pub tree: Db,
    pub id_offset: u64,
    pub state: State,
    /// transaction timeout in milliseconds
    pub tx_timeout: Option<u128>,
}

type ExportData<T> = (u64, Vec<(Vec<u8>, Vec<u8>, T)>);

impl SledStorage {
    pub fn new<P: AsRef<std::path::Path>>(filename: P) -> Result<Self> {
        let tree = sled::open(filename).map_err(err_into)?;
        let id_offset = get_id_offset(&tree)?;
        let state = State::Idle;
        let tx_timeout = Some(DEFAULT_TX_TIMEOUT);

        Ok(Self {
            tree,
            id_offset,
            state,
            tx_timeout,
        })
    }

    pub fn set_transaction_timeout(&mut self, tx_timeout: Option<u128>) {
        self.tx_timeout = tx_timeout;
    }

    pub fn export(&self) -> Result<ExportData<impl Iterator<Item = Vec<Vec<u8>>>>> {
        let id_offset = self.id_offset + self.tree.generate_id().map_err(err_into)?;
        let data = self.tree.export();

        Ok((id_offset, data))
    }

    pub fn import(&mut self, export: ExportData<impl Iterator<Item = Vec<Vec<u8>>>>) -> Result<()> {
        let (new_id_offset, data) = export;
        let old_id_offset = get_id_offset(&self.tree)?;

        self.tree.import(data);

        if new_id_offset > old_id_offset {
            self.tree
                .insert("id_offset", &new_id_offset.to_be_bytes())
                .map_err(err_into)?;

            self.id_offset = new_id_offset;
        }

        Ok(())
    }

    /// Create a vector index for a specific column with persistent storage
    pub fn create_vector_index(
        &mut self,
        table_name: &str,
        column_name: &str,
        index_type: VectorIndexType,
    ) -> Result<()> {
        // Build a unique key for this vector index
        let index_key = format!("vector_index/{}/{}", table_name, column_name);
        
        // Get table schema to validate column exists and is FloatVector type
        let schema = match self.tree.get(format!("schema/{}", table_name).as_bytes()).map_err(err_into)? {
            Some(schema_data) => {
                let snapshot: Snapshot<Schema> = bincode::deserialize(&schema_data).map_err(err_into)?;
                // For simplicity, get latest version (in real scenario should use transaction id)
                snapshot.extract(0, None).ok_or_else(|| Error::StorageMsg(format!("Table '{}' not found", table_name)))?
            }
            None => return Err(Error::StorageMsg(format!("Table '{}' not found", table_name))),
        };

        // Check if the column exists and is a FloatVector type
        let column_defs = schema.column_defs.as_ref().ok_or_else(|| {
            Error::StorageMsg(format!("No column definitions found for table '{}'", table_name))
        })?;
        
        let column_index = column_defs.iter().position(|col| {
            col.name == column_name && col.data_type == gluesql_core::ast::DataType::FloatVector
        }).ok_or_else(|| {
            Error::StorageMsg(format!("FloatVector column '{}' not found in table '{}'", column_name, table_name))
        })?;

        // Determine vector dimension from existing data (if any)
        let data_prefix = format!("data/{}/", table_name);
        let vector_dimension = if let Some(first_row) = self.tree.scan_prefix(data_prefix.as_bytes()).next() {
            let (_, value) = first_row.map_err(err_into)?;
            let snapshot: Snapshot<gluesql_core::store::DataRow> = bincode::deserialize(&value).map_err(err_into)?;
            if let Some(row) = snapshot.extract(0, None) {
                match &row {
                    gluesql_core::store::DataRow::Vec(values) => {
                        if let Some(gluesql_core::data::Value::FloatVector(vec)) = values.get(column_index) {
                            vec.dimension()
                        } else {
                            128 // Default dimension
                        }
                    }
                    gluesql_core::store::DataRow::Map(map) => {
                        if let Some(gluesql_core::data::Value::FloatVector(vec)) = map.get(column_name) {
                            vec.dimension()
                        } else {
                            128 // Default dimension
                        }
                    }
                }
            } else {
                128 // Default dimension
            }
        } else {
            128 // Default dimension if table is empty
        };

        let vector_index = VectorIndex::new(index_type, vector_dimension);

        // Serialize and store the vector index
        let serialized_index = bincode::serialize(&vector_index).map_err(err_into)?;
        self.tree.insert(index_key.as_bytes(), serialized_index).map_err(err_into)?;

        // TODO: In a real implementation, we would also populate the index with existing data
        // This would require iterating through all existing rows and adding vectors to the index

        Ok(())
    }

    /// Get vector index candidates for similarity search (persistent storage)
    pub fn find_vector_similarity_candidates(
        &self,
        table_name: &str,
        column_name: &str,
        query_vector: &FloatVector,
    ) -> Result<Vec<String>> {
        let index_key = format!("vector_index/{}/{}", table_name, column_name);
        
        if let Some(index_data) = self.tree.get(index_key.as_bytes()).map_err(err_into)? {
            let vector_index: VectorIndex = bincode::deserialize(&index_data).map_err(err_into)?;
            vector_index.find_similarity_candidates(query_vector)
                .map_err(|e| Error::StorageMsg(format!("Vector index error: {}", e)))
        } else {
            // No index exists - return empty (caller should do full scan)
            Ok(Vec::new())
        }
    }

    /// Get vector index candidates for distance-based search (persistent storage)
    pub fn find_vector_distance_candidates(
        &self,
        table_name: &str,
        column_name: &str,
        query_vector: &FloatVector,
        max_distance: f32,
    ) -> Vec<String> {
        let index_key = format!("vector_index/{}/{}", table_name, column_name);
        
        if let Some(index_data) = self.tree.get(index_key.as_bytes()).ok().flatten() {
            if let Ok(vector_index) = bincode::deserialize::<VectorIndex>(&index_data) {
                return vector_index.find_distance_candidates(query_vector, max_distance);
            }
        }
        Vec::new()
    }
}

impl TryFrom<Config> for SledStorage {
    type Error = Error;

    fn try_from(config: Config) -> Result<Self> {
        let tree = config.open().map_err(err_into)?;
        let id_offset = get_id_offset(&tree)?;
        let state = State::Idle;
        let tx_timeout = Some(DEFAULT_TX_TIMEOUT);

        Ok(Self {
            tree,
            id_offset,
            state,
            tx_timeout,
        })
    }
}

fn get_id_offset(tree: &Db) -> Result<u64> {
    tree.get("id_offset")
        .map_err(err_into)?
        .map(|id| {
            id.as_ref()
                .try_into()
                .map_err(err_into)
                .map(u64::from_be_bytes)
        })
        .unwrap_or(Ok(0))
}

fn fetch_schema(
    tree: &TransactionalTree,
    table_name: &str,
) -> ConflictableTransactionResult<(String, Option<Snapshot<Schema>>), Error> {
    let key = format!("schema/{table_name}");
    let value = tree.get(key.as_bytes())?;
    let schema_snapshot = value
        .map(|v| bincode::deserialize(&v))
        .transpose()
        .map_err(err_into)
        .map_err(ConflictableTransactionError::Abort)?;

    Ok((key, schema_snapshot))
}

impl Metadata for SledStorage {}
impl gluesql_core::store::CustomFunction for SledStorage {}
impl gluesql_core::store::CustomFunctionMut for SledStorage {}
