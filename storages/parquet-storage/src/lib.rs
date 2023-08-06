use column_def::ParquetSchemaType;
use error::{OptionExt, ParquetStorageError, ResultExt};
use gluesql_core::{
    ast::ColumnDef,
    data::Schema,
    error::{Error, Result},
    prelude::{Key, Value},
    store::{DataRow, Metadata, RowIter},
};
use parquet::{
    file::{reader::FileReader, serialized_reader::SerializedFileReader},
    record::Row,
};
use std::{
    fs::{self, File},
    path::PathBuf,
};
use value::ParquetField;

mod alter_table;
mod column_def;
mod data_type;
mod error;
mod function;
mod index;
mod store;
mod store_mut;
mod transaction;
mod value;

#[derive(Debug, Clone)]
pub struct ParquetStorage {
    pub path: PathBuf,
}

impl ParquetStorage {
    pub fn new(path: &str) -> Result<Self> {
        fs::create_dir_all(path).map_storage_err()?;
        let path = PathBuf::from(path);

        Ok(Self { path })
    }

    // parquet file doesn't have table name so default table name is parquet file name except extension name
    fn fetch_schema(&self, table_name: &str) -> Result<Option<Schema>> {
        let schema_path = self.data_path(table_name);
        let is_schema_path_exist = schema_path.exists();
        if !is_schema_path_exist {
            return Ok(None);
        }
        let file = File::open(&schema_path).map_storage_err()?;
        let reader = SerializedFileReader::new(file).map_storage_err()?;
        let parquet_metadata = reader.metadata(); //get parquet file meta data
        let schema = parquet_metadata.file_metadata().schema(); //get root schema of SchemaDescPtr

        let column_defs: Option<Vec<ColumnDef>> = match schema.get_fields().is_empty() {
            true => None,
            false => Some(
                schema
                    .get_fields()
                    .iter()
                    .map(|field| ColumnDef::try_from(ParquetSchemaType(field)))
                    .collect::<Result<Vec<ColumnDef>, _>>()?,
            ),
        };

        Ok(Some(Schema {
            table_name: table_name.to_owned(),
            column_defs,
            indexes: vec![],
            engine: None,
        }))
    }

    fn data_path(&self, table_name: &str) -> PathBuf {
        self.path_by(table_name, "parquet")
    }

    fn path_by(&self, table_name: &str, extension: &str) -> PathBuf {
        let path = self.path.as_path();
        let mut path = path.join(table_name);
        path.set_extension(extension);

        path
    }

    fn scan_data(&self, table_name: &str) -> Result<(RowIter, Schema)> {
        let schema = self.fetch_schema(table_name)?.map_storage_err(
            ParquetStorageError::TableDoesNotExist(table_name.to_owned()),
        )?;

        let file = File::open(self.data_path(table_name)).map_storage_err()?;

        let parquet_reader = SerializedFileReader::new(file).map_storage_err()?;
        let row_iter = parquet_reader.get_row_iter(None).map_storage_err()?;

        let mut rows = Vec::new();

        for record in row_iter {
            let record: Row = record.map_storage_err()?;
            let row = Self::row_to_values(&record)?;
            rows.push(Ok((Key::Str(rows.len().to_string()), DataRow::Vec(row))));
        }
        println!(
            "fetch gluesql schema completed ================================ {:?}",
            schema
        );
        println!();
        println!(
            "fetch gluesql data completed ================================ {:?}",
            rows
        );
        println!();
        Ok((Box::new(rows.into_iter()), schema))
    }

    fn row_to_values(row: &Row) -> Result<Vec<Value>, Error> {
        let mut values = Vec::new();

        for (_, field) in row.get_column_iter() {
            let value = Value::try_from(ParquetField(field.clone()))?;
            values.push(value);
        }
        Ok(values)
    }
}

impl Metadata for ParquetStorage {}
