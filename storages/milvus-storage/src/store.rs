use async_trait::async_trait;
use futures::{future::try_join_all, stream};
use gluesql_core::{
    ast::{ColumnDef, ColumnUniqueOption, DataType},
    data::{Key, Schema, Value},
    error::{Error, Result},
    store::{DataRow, RowIter, Store}
};
use milvus::{
    collection::Collection,
    data::FieldColumn,
    proto::schema::{DataType as MilvusDataType, CollectionSchema as ProtoCollectionSchema, FieldSchema}
};

use crate::{error::ResultExt, utils::{get_primary_key, key_to_milvus_expression}, MilvusStorage};

#[async_trait]
impl Store for MilvusStorage {
    async fn fetch_schema(&self, table_name: &str) -> Result<Option<Schema>> {
        if !self.client.has_collection(table_name).await.map_storage_err()? {
            return Ok(None);
        }
        let collection = self.client.get_collection(table_name).await.map_storage_err()?;
        let schema = self.schema_from_collection(collection)?;
        Ok(Some(schema))
    }

    async fn fetch_all_schemas(&self) -> Result<Vec<Schema>> {
        let collection_names = self.client.list_collections().await.map_storage_err()?;
        if collection_names.is_empty() {
            return Ok(vec![])
        }
        let tasks = collection_names
            .iter()
            .map(|name| self.fetch_schema(&name));

        let schemas = try_join_all(tasks).await?
            .into_iter()
            .flatten()
            .collect();
    
        Ok(schemas)
    }

    async fn fetch_data(&self, table_name: &str, key: &Key) -> Result<Option<DataRow>> {
        if !self.client.has_collection(table_name).await.map_storage_err()? {
            return Ok(None);
        }

        let collection = self.client.get_collection(table_name).await.map_storage_err()?;
        let schema = self.schema_from_collection(collection.clone())?;

        let column_defs = schema.column_defs.as_ref()
            .ok_or_else(|| Error::StorageMsg("No column definitions found".to_string()))?;
        let primary_key_field = get_primary_key(column_defs)
            .ok_or_else(|| Error::StorageMsg("No primary key found".to_string()))?;

        let expr = key_to_milvus_expression(key, &primary_key_field.name)?;

        let results = collection.query(expr, Vec::<String>::new())
            .await
            .map_storage_err()?;

        if results.is_empty() {
            return Ok(None);
        }

        let data_row = self.convert_field_columns_to_datarow(&results, &schema, 0)?;
        Ok(Some(data_row))
    }

    async fn scan_data<'a>(&'a self, table_name: &str) -> Result<RowIter<'a>> {
        // WARNING: Milvus full table scan limitations
        //
        // Recommendations:
        // - Use this method only for development/testing with small datasets
        // - For production, use SELECT with WHERE clause for filtered queries
        // - Vector databases are optimized for similarity search, not full scans
        //
        // Future: When SDK adds QueryOptions support, add limit parameter here

        if !self.client.has_collection(table_name).await.map_storage_err()? {
            return Ok(Box::pin(stream::empty()));
        }

        let collection = self.client.get_collection(table_name).await.map_storage_err()?;
        let schema = self.schema_from_collection(collection)?;

        let collection = self.client.get_collection(table_name).await.map_storage_err()?;
        let results = collection.query("".to_string(), Vec::<String>::new())
            .await
            .map_storage_err()?;

        let rows = if results.is_empty() {
            vec![]
        } else {
            let column_defs = schema.column_defs.as_ref()
                .ok_or_else(|| Error::StorageMsg("No column definitions found".to_string()))?;

            let primary_key_field = get_primary_key(column_defs)
                .ok_or_else(|| Error::StorageMsg("No primary key found".to_string()))?;

            let pk_index = column_defs
                .iter()
                .position(|cd| cd.name == primary_key_field.name)
                .ok_or_else(|| Error::StorageMsg("Primary key not found in columns".to_string()))?;

            let row_count = results.first()
                .map(|fc| fc.len())
                .unwrap_or(0);

            (0..row_count)
                .map(|idx| {
                    let data_row = self.convert_field_columns_to_datarow(&results, &schema, idx)?;
                    let key = self.extract_key_from_datarow(&data_row, pk_index)?;
                    Ok((key, data_row))
                })
                .collect()
        };

        Ok(Box::pin(stream::iter(rows)))
    }
}

impl MilvusStorage {
    fn convert_field_columns_to_datarow(
        &self,
        field_columns: &[FieldColumn],
        schema: &Schema,
        row_index: usize
    ) -> Result<DataRow> {
        let column_defs = schema.column_defs.as_ref()
            .ok_or_else(|| Error::StorageMsg("No column definitions".to_string()))?;

        let values = column_defs
            .iter()
            .map(|col_def| {
                let field_col = field_columns
                    .iter()
                    .find(|fc| fc.name == col_def.name)
                    .ok_or_else(|| Error::StorageMsg(format!("Field {} not found in results", col_def.name)))?;

                let milvus_value = field_col.get(row_index)
                    .ok_or_else(|| Error::StorageMsg(format!("No value at index {} for field {}", row_index, col_def.name)))?;

                self.milvus_value_to_glue_value(milvus_value, &col_def.data_type)
            })
            .collect::<Result<Vec<Value>>>()?;

        Ok(DataRow::Vec(values))
    }

    fn extract_key_from_datarow(&self, data_row: &DataRow, pk_index: usize) -> Result<Key> {
        let values = match data_row {
            DataRow::Vec(v) => v,
            _ => return Err(Error::StorageMsg("Expected DataRow::Vec".to_string())),
        };

        match &values[pk_index] {
            Value::I8(v) => Ok(Key::I8(*v)),
            Value::I16(v) => Ok(Key::I16(*v)),
            Value::I32(v) => Ok(Key::I32(*v)),
            Value::I64(v) => Ok(Key::I64(*v)),
            Value::Str(v) => Ok(Key::Str(v.clone())),
            _ => Err(Error::StorageMsg("Unsupported primary key type".to_string())),
        }
    }

    fn milvus_value_to_glue_value(&self, milvus_val: milvus::value::Value, data_type: &DataType) -> Result<Value> {
        use milvus::value::Value as MV;

        match (milvus_val, data_type) {
            (MV::Bool(v), DataType::Boolean) => Ok(Value::Bool(v)),
            (MV::Int8(v), DataType::Int8) => Ok(Value::I8(v)),
            (MV::Int16(v), DataType::Int16) => Ok(Value::I16(v)),
            (MV::Int32(v), DataType::Int32) => Ok(Value::I32(v)),
            (MV::Long(v), DataType::Int) => Ok(Value::I64(v)),
            (MV::Float(v), DataType::Float32) => Ok(Value::F32(v)),
            (MV::Double(v), DataType::Float) => Ok(Value::F64(v)),
            (MV::String(v), DataType::Text) => Ok(Value::Str(v.to_string())),
            (MV::Binary(v), DataType::Bytea) => Ok(Value::Bytea(v.to_vec())),
            (MV::FloatArray(v), DataType::Bytea) => {
                // Convert float array to bytes
                let bytes: Vec<u8> = v.iter()
                    .flat_map(|f| f.to_le_bytes())
                    .collect();
                Ok(Value::Bytea(bytes))
            },
            (_, dt) => Err(Error::StorageMsg(format!(
                "Type mismatch: Milvus value cannot be converted to {:?}",
                dt
            )))
        }
    }
    
    fn schema_from_collection(&self, collection: Collection) -> Result<Schema> {
        let proto_schema: ProtoCollectionSchema = collection.schema().clone().into();
        let column_defs = proto_schema
            .fields
            .iter()
            .map(|field| self.field_to_column_def(field))
            .collect::<Result<Vec<_>>>()?;
    
        Ok(Schema {
            table_name: proto_schema.name.clone(),
            column_defs: Some(column_defs),
            indexes: Vec::new(),
            engine: Some("milvus".to_owned()),
            foreign_keys: Vec::new(),
            comment: (!proto_schema.description.is_empty()).then(|| proto_schema.description.clone()),
        })
    }
    
    fn field_to_column_def(&self, field: &FieldSchema) -> Result<ColumnDef> {
        let data_type = match field.data_type() {
            MilvusDataType::Bool => DataType::Boolean,
            MilvusDataType::Int8 => DataType::Int8,
            MilvusDataType::Int16 => DataType::Int16,
            MilvusDataType::Int32 => DataType::Int32,
            MilvusDataType::Int64 => DataType::Int,
            MilvusDataType::Float => DataType::Float32,
            MilvusDataType::Double => DataType::Float,
            MilvusDataType::String | MilvusDataType::VarChar => DataType::Text,
            MilvusDataType::BinaryVector | MilvusDataType::FloatVector => DataType::Bytea,
            other => {
                return Err(Error::StorageMsg(format!(
                    "unsupported Milvus field type: {other:?}"
                )))
            }
        };
    
        Ok(ColumnDef {
            name: field.name.clone(),
            data_type,
            nullable: !field.is_primary_key,
            default: None,
            unique: field.is_primary_key
                .then_some(ColumnUniqueOption { is_primary: true }),
            comment: (!field.description.is_empty()).then(|| field.description.clone()),
        })
    }
}