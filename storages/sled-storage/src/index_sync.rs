use {
    super::{Snapshot, err_into, fetch_schema, key},
    gluesql_core::{
        ast::{Expr, DataType},
        data::schema::{Schema, SchemaIndex},
        data::VectorIndex,
        error::{Error, IndexError, Result},
        executor::evaluate_stateless,
        prelude::Value,
        store::DataRow,
    },
    sled::{
        IVec,
        transaction::{
            ConflictableTransactionError, ConflictableTransactionResult, TransactionalTree,
        },
    },
    std::borrow::Cow,
    utils::Vector,
};

pub struct IndexSync<'a> {
    tree: &'a TransactionalTree,
    txid: u64,
    table_name: &'a str,
    columns: Option<Vec<String>>,
    indexes: Cow<'a, [SchemaIndex]>,
}

impl<'a> IndexSync<'a> {
    pub fn from_schema(tree: &'a TransactionalTree, txid: u64, schema: &'a Schema) -> Self {
        let Schema {
            table_name,
            column_defs,
            indexes,
            ..
        } = schema;

        let columns = column_defs.as_ref().map(|column_defs| {
            column_defs
                .iter()
                .map(|column_def| column_def.name.to_owned())
                .collect::<Vec<_>>()
        });

        let indexes = Cow::Borrowed(indexes.as_slice());

        Self {
            tree,
            txid,
            table_name,
            columns,
            indexes,
        }
    }

    pub fn new(
        tree: &'a TransactionalTree,
        txid: u64,
        table_name: &'a str,
    ) -> sled::transaction::ConflictableTransactionResult<Self, Error> {
        let Schema {
            column_defs,
            indexes,
            ..
        } = fetch_schema(tree, table_name)
            .map(|(_, snapshot)| snapshot)?
            .and_then(|snapshot| snapshot.extract(txid, None))
            .ok_or_else(|| IndexError::ConflictTableNotFound(table_name.to_owned()))
            .map_err(err_into)
            .map_err(ConflictableTransactionError::Abort)?;

        let columns = column_defs.map(|column_defs| {
            column_defs
                .into_iter()
                .map(|column_def| column_def.name)
                .collect::<Vec<_>>()
        });

        Ok(Self {
            tree,
            txid,
            table_name,
            columns,
            indexes: Cow::Owned(indexes),
        })
    }

    pub async fn insert(
        &self,
        data_key: &IVec,
        row: &DataRow,
    ) -> ConflictableTransactionResult<(), Error> {
        for index in self.indexes.iter() {
            self.insert_index(index, data_key, row).await?;
        }

        // Handle vector indexes
        self.update_vector_indexes_on_insert(data_key, row)?;

        Ok(())
    }

    pub async fn insert_index(
        &self,
        index: &SchemaIndex,
        data_key: &IVec,
        row: &DataRow,
    ) -> ConflictableTransactionResult<(), Error> {
        let SchemaIndex {
            name: index_name,
            expr: index_expr,
            ..
        } = index;

        let index_key = &evaluate_index_key(
            self.table_name,
            index_name,
            index_expr,
            self.columns.as_deref(),
            row,
        )
        .await?;

        self.insert_index_data(index_key, data_key)?;

        Ok(())
    }

    pub async fn update(
        &self,
        data_key: &IVec,
        old_row: &DataRow,
        new_row: &DataRow,
    ) -> ConflictableTransactionResult<(), Error> {
        for index in self.indexes.iter() {
            let SchemaIndex {
                name: index_name,
                expr: index_expr,
                ..
            } = index;

            let old_index_key = &evaluate_index_key(
                self.table_name,
                index_name,
                index_expr,
                self.columns.as_deref(),
                old_row,
            )
            .await?;

            let new_index_key = &evaluate_index_key(
                self.table_name,
                index_name,
                index_expr,
                self.columns.as_deref(),
                new_row,
            )
            .await?;

            self.delete_index_data(old_index_key, data_key)?;
            self.insert_index_data(new_index_key, data_key)?;
        }

        // Handle vector indexes
        self.update_vector_indexes_on_update(data_key, old_row, new_row)?;

        Ok(())
    }

    pub async fn delete(
        &self,
        data_key: &IVec,
        row: &DataRow,
    ) -> ConflictableTransactionResult<(), Error> {
        for index in self.indexes.iter() {
            self.delete_index(index, data_key, row).await?;
        }

        // Handle vector indexes
        self.update_vector_indexes_on_delete(data_key, row)?;

        Ok(())
    }

    pub async fn delete_index(
        &self,
        index: &SchemaIndex,
        data_key: &IVec,
        row: &DataRow,
    ) -> ConflictableTransactionResult<(), Error> {
        let SchemaIndex {
            name: index_name,
            expr: index_expr,
            ..
        } = index;

        let index_key = &evaluate_index_key(
            self.table_name,
            index_name,
            index_expr,
            self.columns.as_deref(),
            row,
        )
        .await?;

        self.delete_index_data(index_key, data_key)?;

        Ok(())
    }

    fn insert_index_data(
        &self,
        index_key: &[u8],
        data_key: &IVec,
    ) -> ConflictableTransactionResult<(), Error> {
        let data_keys: Vec<Snapshot<Vec<u8>>> = self
            .tree
            .get(index_key)?
            .map(|v| bincode::deserialize(&v))
            .transpose()
            .map_err(err_into)
            .map_err(ConflictableTransactionError::Abort)?
            .unwrap_or_default();

        let key_snapshot = Snapshot::<Vec<u8>>::new(self.txid, data_key.to_vec());
        let data_keys = Vector::from(data_keys).push(key_snapshot);
        let data_keys = bincode::serialize(&Vec::from(data_keys))
            .map_err(err_into)
            .map_err(ConflictableTransactionError::Abort)?;

        let temp_key = key::temp_index(self.txid, index_key);

        self.tree.insert(index_key, data_keys)?;
        self.tree.insert(temp_key, index_key)?;

        Ok(())
    }

    fn delete_index_data(
        &self,
        index_key: &[u8],
        data_key: &IVec,
    ) -> ConflictableTransactionResult<(), Error> {
        let data_keys: Vec<Snapshot<Vec<u8>>> = self
            .tree
            .get(index_key)?
            .map(|v| bincode::deserialize(&v))
            .ok_or_else(|| IndexError::ConflictOnIndexDataDeleteSync.into())
            .map_err(ConflictableTransactionError::Abort)?
            .map_err(err_into)
            .map_err(ConflictableTransactionError::Abort)?;

        let data_keys = data_keys
            .into_iter()
            .map(|snapshot| {
                let key = snapshot.get(self.txid, None);

                if Some(data_key) == key.map(IVec::from).as_ref() {
                    snapshot.delete(self.txid).0
                } else {
                    snapshot
                }
            })
            .collect::<Vec<_>>();

        let data_keys = bincode::serialize(&data_keys)
            .map_err(err_into)
            .map_err(ConflictableTransactionError::Abort)?;

        let temp_key = key::temp_index(self.txid, index_key);

        self.tree.insert(index_key, data_keys)?;
        self.tree.insert(temp_key, index_key)?;

        Ok(())
    }

    /// Update vector indexes when a row is inserted
    fn update_vector_indexes_on_insert(
        &self,
        data_key: &IVec,
        row: &DataRow,
    ) -> ConflictableTransactionResult<(), Error> {
        // Get schema to identify FloatVector columns
        let (_, schema_snapshot) = fetch_schema(self.tree, self.table_name)?;
        let schema = schema_snapshot
            .and_then(|snapshot| snapshot.extract(self.txid, None))
            .ok_or_else(|| IndexError::ConflictTableNotFound(self.table_name.to_owned()))
            .map_err(err_into)
            .map_err(ConflictableTransactionError::Abort)?;

        if let Some(column_defs) = &schema.column_defs {
            for (col_idx, column_def) in column_defs.iter().enumerate() {
                if column_def.data_type == DataType::FloatVector {
                    // Check if there's a vector index for this column
                    let index_key = format!("vector_index/{}/{}", self.table_name, column_def.name);
                    
                    if let Some(index_data) = self.tree.get(index_key.as_bytes())? {
                        // Load the vector index
                        if let Ok(mut vector_index) = bincode::deserialize::<VectorIndex>(&index_data) {
                            // Get the vector value from the row
                            let vector_opt = match row {
                                DataRow::Vec(values) => values.get(col_idx),
                                DataRow::Map(map) => map.get(&column_def.name),
                            };
                            
                            if let Some(Value::FloatVector(vector)) = vector_opt {
                                let key_str = format!("{:?}", data_key);
                                if let Err(_) = vector_index.add_vector(key_str, vector) {
                                    // Ignore errors for now - in production we might want to handle this
                                }
                                
                                // Save the updated index back to storage
                                if let Ok(serialized) = bincode::serialize(&vector_index) {
                                    let _ = self.tree.insert(index_key.as_bytes(), serialized);
                                }
                            }
                        }
                    }
                }
            }
        }

        Ok(())
    }

    /// Update vector indexes when a row is updated
    fn update_vector_indexes_on_update(
        &self,
        data_key: &IVec,
        old_row: &DataRow,
        new_row: &DataRow,
    ) -> ConflictableTransactionResult<(), Error> {
        // First remove old vectors, then add new ones
        self.update_vector_indexes_on_delete(data_key, old_row)?;
        self.update_vector_indexes_on_insert(data_key, new_row)?;
        Ok(())
    }

    /// Update vector indexes when a row is deleted
    fn update_vector_indexes_on_delete(
        &self,
        data_key: &IVec,
        row: &DataRow,
    ) -> ConflictableTransactionResult<(), Error> {
        // Get schema to identify FloatVector columns
        let (_, schema_snapshot) = fetch_schema(self.tree, self.table_name)?;
        let schema = schema_snapshot
            .and_then(|snapshot| snapshot.extract(self.txid, None))
            .ok_or_else(|| IndexError::ConflictTableNotFound(self.table_name.to_owned()))
            .map_err(err_into)
            .map_err(ConflictableTransactionError::Abort)?;

        if let Some(column_defs) = &schema.column_defs {
            for (col_idx, column_def) in column_defs.iter().enumerate() {
                if column_def.data_type == DataType::FloatVector {
                    // Check if there's a vector index for this column
                    let index_key = format!("vector_index/{}/{}", self.table_name, column_def.name);
                    
                    if let Some(index_data) = self.tree.get(index_key.as_bytes())? {
                        // Load the vector index
                        if let Ok(mut vector_index) = bincode::deserialize::<VectorIndex>(&index_data) {
                            // Get the vector value from the row
                            let vector_opt = match row {
                                DataRow::Vec(values) => values.get(col_idx),
                                DataRow::Map(map) => map.get(&column_def.name),
                            };
                            
                            if let Some(Value::FloatVector(vector)) = vector_opt {
                                let key_str = format!("{:?}", data_key);
                                if let Err(_) = vector_index.remove_vector(&key_str, vector) {
                                    // Ignore errors for now - in production we might want to handle this
                                }
                                
                                // Save the updated index back to storage
                                if let Ok(serialized) = bincode::serialize(&vector_index) {
                                    let _ = self.tree.insert(index_key.as_bytes(), serialized);
                                }
                            }
                        }
                    }
                }
            }
        }

        Ok(())
    }
}

async fn evaluate_index_key(
    table_name: &str,
    index_name: &str,
    index_expr: &Expr,
    columns: Option<&[String]>,
    row: &DataRow,
) -> ConflictableTransactionResult<Vec<u8>, Error> {
    let context = Some(row.as_context(columns));
    let evaluated = evaluate_stateless(context, index_expr)
        .await
        .map_err(ConflictableTransactionError::Abort)?;
    let value: Value = evaluated
        .try_into()
        .map_err(ConflictableTransactionError::Abort)?;

    build_index_key(table_name, index_name, value).map_err(ConflictableTransactionError::Abort)
}

pub fn build_index_key_prefix(table_name: &str, index_name: &str) -> Vec<u8> {
    format!("index/{table_name}/{index_name}/").into_bytes()
}

pub fn build_index_key(table_name: &str, index_name: &str, value: Value) -> Result<Vec<u8>> {
    Ok(build_index_key_prefix(table_name, index_name)
        .into_iter()
        .chain(value.to_cmp_be_bytes()?)
        .collect::<Vec<_>>())
}
