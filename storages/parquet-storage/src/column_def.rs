use {
    gluesql_core::{
        ast::{ColumnDef, ColumnUniqueOption},
        parse_sql::parse_expr,
        prelude::{DataType, Error},
        translate::translate_expr,
    },
    lazy_static::lazy_static,
    parquet::{basic::Type as PhysicalType, format::KeyValue, schema::types::Type as SchemaType},
    std::{collections::HashMap, convert::TryFrom},
};

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

pub fn map_parquet_to_gluesql(data_type: &str) -> Option<&'static DataType> {
    PARQUET_TO_GLUESQL_DATA_TYPE_MAPPING.get(data_type)
}

#[derive(Debug)]
pub struct ParquetSchemaType<'a> {
    pub inner: &'a SchemaType,
    pub metadata: Option<&'a Vec<KeyValue>>, // this will hold the metadata for unique & primary key concepts
}

impl<'a> ParquetSchemaType<'a> {
    pub fn inner(&self) -> &'a SchemaType {
        self.inner
    }

    pub fn get_metadata(&self) -> &Option<&'a Vec<KeyValue>> {
        &self.metadata
    }
}

impl<'a> TryFrom<ParquetSchemaType<'a>> for ColumnDef {
    type Error = Error;

    fn try_from(parquet_col_def: ParquetSchemaType<'a>) -> Result<Self, Self::Error> {
        let inner = parquet_col_def.inner();

        let name = inner.name().to_owned();
        let mut data_type = match inner {
            SchemaType::PrimitiveType { physical_type, .. } => convert_to_data_type(physical_type),
            SchemaType::GroupType { .. } => DataType::Map,
        };
        let nullable = inner.is_optional();
        let mut unique = None;
        let mut default = None;
        let mut comment = None;

        if let Some(metadata) = parquet_col_def.get_metadata().as_deref() {
            for kv in metadata.iter() {
                match kv.key.as_str() {
                    k if k == format!("unique_option{}", name) => match kv.value.as_deref() {
                        Some("primary_key") => {
                            unique = Some(ColumnUniqueOption { is_primary: true });
                        }
                        _ => unique = Some(ColumnUniqueOption { is_primary: false }),
                    },
                    k if k == format!("data_type{}", name) => {
                        if let Some(value) = kv.value.as_deref() {
                            if let Some(mapped_data_type) = map_parquet_to_gluesql(value) {
                                data_type = mapped_data_type.clone();
                            }
                        }
                    }
                    k if k == format!("default_{}", name) => {
                        if let Some(value) = &kv.value {
                            let parsed = parse_expr(value.clone())?;
                            let tran = translate_expr(&parsed)?;

                            default = Some(tran);
                        }
                    }
                    k if k == format!("comment_{}", name) => {
                        if let Some(value) = &kv.value {
                            comment = Some(value.clone());
                        }
                    }
                    _ => {}
                }
            }
        }
        Ok(ColumnDef {
            name,
            data_type,
            nullable,
            default,
            unique,
            comment,
        })
    }
}

fn convert_to_data_type(pt: &PhysicalType) -> DataType {
    match pt {
        PhysicalType::BOOLEAN => DataType::Boolean,
        PhysicalType::INT32 => DataType::Int32,
        PhysicalType::INT64 => DataType::Int,
        PhysicalType::FLOAT => DataType::Float32,
        PhysicalType::DOUBLE => DataType::Float,
        PhysicalType::INT96 => DataType::Int128,
        PhysicalType::BYTE_ARRAY | PhysicalType::FIXED_LEN_BYTE_ARRAY => DataType::Bytea,
    }
}
