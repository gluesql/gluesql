use async_trait::async_trait;
use futures::future::try_join_all;

use crate::{error::ResultExt, MilvusStorage};

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

    }

    async fn scan_data<'a>(&'a self, table_name: &str) -> Result<RowIter<'a>> {

    }
}

impl MilvusStorage {
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
        let data_type = match field.dtype {
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
            nullable: !field.is_primary,
            default: None,
            unique: field
                .is_primary
                .then_some(ColumnUniqueOption { is_primary: true }),
            comment: (!field.description.is_empty()).then(|| field.description.clone()),
        })
    }
}