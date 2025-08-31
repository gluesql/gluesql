#![deny(clippy::str_to_string)]

mod alter_table;
mod index;
mod metadata;
mod transaction;

use {
    async_trait::async_trait,
    futures::stream::iter,
    gluesql_core::{
        chrono::Utc,
        data::{CustomFunction as StructCustomFunction, Key, Schema, Value, VectorIndex, VectorIndexType, FloatVector},
        error::Result,
        store::{CustomFunction, CustomFunctionMut, DataRow, RowIter, Store, StoreMut},
    },
    serde::{Deserialize, Serialize},
    std::collections::{BTreeMap, HashMap},
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Item {
    pub schema: Schema,
    pub rows: BTreeMap<Key, DataRow>,
    pub vector_indexes: HashMap<String, VectorIndex>, // column_name -> VectorIndex
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct MemoryStorage {
    pub id_counter: i64,
    pub items: HashMap<String, Item>,
    pub metadata: HashMap<String, BTreeMap<String, Value>>,
    pub functions: HashMap<String, StructCustomFunction>,
}

impl MemoryStorage {
    pub fn scan_data(&self, table_name: &str) -> Vec<(Key, DataRow)> {
        match self.items.get(table_name) {
            Some(item) => item.rows.clone().into_iter().collect(),
            None => vec![],
        }
    }

    /// Create a vector index for a specific column
    pub fn create_vector_index(
        &mut self,
        table_name: &str,
        column_name: &str,
        index_type: VectorIndexType,
    ) -> Result<()> {
        use gluesql_core::{ast::DataType, store::DataRow};

        let item = self.items.get_mut(table_name).ok_or_else(|| {
            gluesql_core::error::Error::StorageMsg(format!("Table '{}' not found", table_name))
        })?;

        // Check if the column exists and is a FloatVector type
        let column_defs = item.schema.column_defs.as_ref().ok_or_else(|| {
            gluesql_core::error::Error::StorageMsg(format!("No column definitions found for table '{}'", table_name))
        })?;
        
        let column_index = column_defs.iter().position(|col| {
            col.name == column_name && col.data_type == DataType::FloatVector
        }).ok_or_else(|| {
            gluesql_core::error::Error::StorageMsg(format!("FloatVector column '{}' not found in table '{}'", column_name, table_name))
        })?;

        // Determine vector dimension from existing data (if any)
        let vector_dimension = if let Some((_, first_row)) = item.rows.iter().next() {
            match first_row {
                DataRow::Vec(values) => {
                    if let Some(Value::FloatVector(vec)) = values.get(column_index) {
                        vec.dimension()
                    } else {
                        128 // Default dimension
                    }
                }
                DataRow::Map(map) => {
                    if let Some(Value::FloatVector(vec)) = map.get(column_name) {
                        vec.dimension()
                    } else {
                        128 // Default dimension
                    }
                }
            }
        } else {
            128 // Default dimension if table is empty
        };

        let mut vector_index = VectorIndex::new(index_type, vector_dimension);

        // Add existing vectors to the index
        for (key, row) in &item.rows {
            let vector_opt = match row {
                DataRow::Vec(values) => values.get(column_index),
                DataRow::Map(map) => map.get(column_name),
            };
            
            if let Some(Value::FloatVector(vector)) = vector_opt {
                let key_str = format!("{:?}", key); // Convert Key to String representation
                if let Err(e) = vector_index.add_vector(key_str, vector) {
                    return Err(gluesql_core::error::Error::StorageMsg(format!("Failed to add vector to index: {}", e)));
                }
            }
        }

        item.vector_indexes.insert(column_name.to_string(), vector_index);
        Ok(())
    }

    /// Get vector index candidates for similarity search
    pub fn find_vector_similarity_candidates(
        &self,
        table_name: &str,
        column_name: &str,
        query_vector: &FloatVector,
    ) -> Result<Vec<String>> {
        let item = self.items.get(table_name).ok_or_else(|| {
            gluesql_core::error::Error::StorageMsg(format!("Table '{}' not found", table_name))
        })?;

        if let Some(vector_index) = item.vector_indexes.get(column_name) {
            vector_index.find_similarity_candidates(query_vector)
                .map_err(|e| gluesql_core::error::Error::StorageMsg(format!("Vector index error: {}", e)))
        } else {
            // No index exists - return empty (caller should do full scan)
            Ok(Vec::new())
        }
    }

    /// Get vector index candidates for distance-based search
    pub fn find_vector_distance_candidates(
        &self,
        table_name: &str,
        column_name: &str,
        query_vector: &FloatVector,
        max_distance: f32,
    ) -> Vec<String> {
        if let Some(item) = self.items.get(table_name) {
            if let Some(vector_index) = item.vector_indexes.get(column_name) {
                return vector_index.find_distance_candidates(query_vector, max_distance);
            }
        }
        Vec::new()
    }

    /// Update vector index when a row is inserted/updated
    pub fn update_vector_indexes_on_insert(
        &mut self,
        table_name: &str,
        key: &Key,
        row: &DataRow,
    ) -> Result<()> {
        use gluesql_core::{ast::DataType, store::DataRow};

        let item = self.items.get_mut(table_name).ok_or_else(|| {
            gluesql_core::error::Error::StorageMsg(format!("Table '{}' not found", table_name))
        })?;

        // Update vector indexes for FloatVector columns
        if let Some(column_defs) = &item.schema.column_defs {
            for (col_idx, column_def) in column_defs.iter().enumerate() {
                if column_def.data_type == DataType::FloatVector {
                    if let Some(vector_index) = item.vector_indexes.get_mut(&column_def.name) {
                        let vector_opt = match row {
                            DataRow::Vec(values) => values.get(col_idx),
                            DataRow::Map(map) => map.get(&column_def.name),
                        };
                        
                        if let Some(Value::FloatVector(vector)) = vector_opt {
                            let key_str = format!("{:?}", key);
                            if let Err(e) = vector_index.add_vector(key_str, vector) {
                                return Err(gluesql_core::error::Error::StorageMsg(format!("Failed to update vector index: {}", e)));
                            }
                        }
                    }
                }
            }
        }

        Ok(())
    }

    /// Update vector index when a row is deleted
    pub fn update_vector_indexes_on_delete(
        &mut self,
        table_name: &str,
        key: &Key,
        row: &DataRow,
    ) -> Result<()> {
        use gluesql_core::{ast::DataType, store::DataRow};

        let item = self.items.get_mut(table_name).ok_or_else(|| {
            gluesql_core::error::Error::StorageMsg(format!("Table '{}' not found", table_name))
        })?;

        // Update vector indexes for FloatVector columns
        if let Some(column_defs) = &item.schema.column_defs {
            for (col_idx, column_def) in column_defs.iter().enumerate() {
                if column_def.data_type == DataType::FloatVector {
                    if let Some(vector_index) = item.vector_indexes.get_mut(&column_def.name) {
                        let vector_opt = match row {
                            DataRow::Vec(values) => values.get(col_idx),
                            DataRow::Map(map) => map.get(&column_def.name),
                        };
                        
                        if let Some(Value::FloatVector(vector)) = vector_opt {
                            let key_str = format!("{:?}", key);
                            if let Err(e) = vector_index.remove_vector(&key_str, vector) {
                                return Err(gluesql_core::error::Error::StorageMsg(format!("Failed to update vector index: {}", e)));
                            }
                        }
                    }
                }
            }
        }

        Ok(())
    }
}

#[async_trait]
impl CustomFunction for MemoryStorage {
    async fn fetch_function<'a>(
        &'a self,
        func_name: &str,
    ) -> Result<Option<&'a StructCustomFunction>> {
        Ok(self.functions.get(&func_name.to_uppercase()))
    }
    async fn fetch_all_functions<'a>(&'a self) -> Result<Vec<&'a StructCustomFunction>> {
        Ok(self.functions.values().collect())
    }
}

#[async_trait]
impl CustomFunctionMut for MemoryStorage {
    async fn insert_function(&mut self, func: StructCustomFunction) -> Result<()> {
        self.functions.insert(func.func_name.to_uppercase(), func);
        Ok(())
    }

    async fn delete_function(&mut self, func_name: &str) -> Result<()> {
        self.functions.remove(&func_name.to_uppercase());
        Ok(())
    }
}

#[async_trait]
impl Store for MemoryStorage {
    async fn fetch_all_schemas(&self) -> Result<Vec<Schema>> {
        let mut schemas = self
            .items
            .values()
            .map(|item| item.schema.clone())
            .collect::<Vec<_>>();
        schemas.sort_by(|a, b| a.table_name.cmp(&b.table_name));

        Ok(schemas)
    }
    async fn fetch_schema(&self, table_name: &str) -> Result<Option<Schema>> {
        self.items
            .get(table_name)
            .map(|item| Ok(item.schema.clone()))
            .transpose()
    }

    async fn fetch_data(&self, table_name: &str, key: &Key) -> Result<Option<DataRow>> {
        let row = self
            .items
            .get(table_name)
            .and_then(|item| item.rows.get(key).cloned());

        Ok(row)
    }

    async fn scan_data<'a>(&'a self, table_name: &str) -> Result<RowIter<'a>> {
        let rows = MemoryStorage::scan_data(self, table_name)
            .into_iter()
            .map(Ok);

        Ok(Box::pin(iter(rows)))
    }
}

#[async_trait]
impl StoreMut for MemoryStorage {
    async fn insert_schema(&mut self, schema: &Schema) -> Result<()> {
        let created = BTreeMap::from([(
            "CREATED".to_owned(),
            Value::Timestamp(Utc::now().naive_utc()),
        )]);
        let meta = HashMap::from([(schema.table_name.clone(), created)]);
        self.metadata.extend(meta);

        let table_name = schema.table_name.clone();
        let item = Item {
            schema: schema.clone(),
            rows: BTreeMap::new(),
            vector_indexes: HashMap::new(),
        };
        self.items.insert(table_name, item);

        Ok(())
    }

    async fn delete_schema(&mut self, table_name: &str) -> Result<()> {
        self.items.remove(table_name);
        self.metadata.remove(table_name);

        Ok(())
    }

    async fn append_data(&mut self, table_name: &str, rows: Vec<DataRow>) -> Result<()> {
        if let Some(item) = self.items.get_mut(table_name) {
            for row in rows {
                self.id_counter += 1;

                item.rows.insert(Key::I64(self.id_counter), row);
            }
        }

        Ok(())
    }

    async fn insert_data(&mut self, table_name: &str, rows: Vec<(Key, DataRow)>) -> Result<()> {
        if let Some(_) = self.items.get(table_name) {
            for (key, row) in rows {
                // Update vector indexes before inserting
                self.update_vector_indexes_on_insert(table_name, &key, &row)?;
                
                // Insert the row
                if let Some(item) = self.items.get_mut(table_name) {
                    item.rows.insert(key, row);
                }
            }
        }

        Ok(())
    }

    async fn delete_data(&mut self, table_name: &str, keys: Vec<Key>) -> Result<()> {
        // Collect rows to delete first to avoid borrowing issues
        let rows_to_delete: Vec<(Key, DataRow)> = if let Some(item) = self.items.get(table_name) {
            keys.iter()
                .filter_map(|key| item.rows.get(key).map(|row| (key.clone(), row.clone())))
                .collect()
        } else {
            Vec::new()
        };

        // Update vector indexes for each deleted row
        for (key, row) in &rows_to_delete {
            self.update_vector_indexes_on_delete(table_name, key, row)?;
        }

        // Now remove the rows
        if let Some(item) = self.items.get_mut(table_name) {
            for key in keys {
                item.rows.remove(&key);
            }
        }

        Ok(())
    }
}
