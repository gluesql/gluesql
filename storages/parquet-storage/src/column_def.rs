use crate::data_type::ParquetBasicPhysicalType;
use gluesql_core::{
    ast::{ColumnDef, ColumnUniqueOption},
    parse_sql::parse_expr,
    prelude::{DataType, Error},
    translate::translate_expr,
};
use lazy_static::lazy_static;
use parquet::{format::KeyValue, schema::types::Type as SchemaType};
use std::{collections::HashMap, convert::TryFrom};

lazy_static! {
    static ref PARQUET_TO_GLUESQL_DATA_TYPE_MAPPING: HashMap<&'static str, DataType> = {
        let mut m = HashMap::new();
        m.insert("Boolean", DataType::Boolean);
        m.insert("Int8", DataType::Int8);
        m.insert("Int16", DataType::Int16);
        m.insert("Int32", DataType::Int32);
        m.insert("Int", DataType::Int);
        m.insert("Int128", DataType::Int128);
        m.insert("Uint8", DataType::Uint8);
        m.insert("Uint16", DataType::Uint16);
        m.insert("Uint32", DataType::Uint32);
        m.insert("Uint64", DataType::Uint64);
        m.insert("Uint128", DataType::Uint128);
        m.insert("Float32", DataType::Float32);
        m.insert("Float", DataType::Float);
        m.insert("Text", DataType::Text);
        m.insert("Bytea", DataType::Bytea);
        m.insert("Inet", DataType::Inet);
        m.insert("Date", DataType::Date);
        m.insert("Timestamp", DataType::Timestamp);
        m.insert("Time", DataType::Time);
        m.insert("Interval", DataType::Interval);
        m.insert("Uuid", DataType::Uuid);
        m.insert("Map", DataType::Map);
        m.insert("List", DataType::List);
        m.insert("Decimal", DataType::Decimal);
        m.insert("Point", DataType::Point);
        m
    };
}

#[derive(Debug)]
pub struct ParquetSchemaType<'a> {
    pub inner: &'a SchemaType,
    pub metadata: Option<&'a Vec<KeyValue>>, // this will hold the metadata for unique & primary key concepts
}

impl<'a> TryFrom<ParquetSchemaType<'a>> for ColumnDef {
    type Error = Error;

    fn try_from(parquet_col_def: ParquetSchemaType<'a>) -> Result<Self, Self::Error> {
        let inner = parquet_col_def.inner();

        let name = inner.name().to_owned();
        let physical_type = inner.get_physical_type();
        let mut data_type = ParquetBasicPhysicalType::convert_to_data_type(&physical_type)?;
        let nullable = inner.is_optional();
        let mut unique = None;
        let mut default = None;

        if let Some(metadata) = parquet_col_def.get_metadata().as_deref() {
            for kv in metadata.iter() {
                if kv.key == format!("unique_option{}", name) {
                    match kv.value.as_deref() {
                        Some("primary_key") => {
                            unique = Some(ColumnUniqueOption { is_primary: true });
                        }
                        Some("unique") => unique = Some(ColumnUniqueOption { is_primary: false }),
                        _ => {}
                    }
                } else if kv.key == format!("data_type{}", name) {
                    if let Some(value) = kv.value.as_deref() {
                        if let Some(mapped_data_type) =
                            PARQUET_TO_GLUESQL_DATA_TYPE_MAPPING.get(value)
                        {
                            data_type = mapped_data_type.clone();
                        }
                    }
                } else if kv.key == format!("default_{}", name) {
                    if let Some(value) = &kv.value {
                        let parsed = parse_expr(value.clone())?;
                        let tran = translate_expr(&parsed)?;

                        default = Some(tran);
                    }
                }
            }
        }
        Ok(ColumnDef {
            name,
            data_type,
            nullable,
            default,
            unique,
        })
    }
}

impl<'a> ParquetSchemaType<'a> {
    pub fn inner(&self) -> &'a SchemaType {
        self.inner
    }

    pub fn get_metadata(&self) -> &Option<&'a Vec<KeyValue>> {
        &self.metadata
    }
}
