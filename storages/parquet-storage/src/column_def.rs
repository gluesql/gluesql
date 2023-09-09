use gluesql_core::{
    ast::{ColumnDef, ColumnUniqueOption},
    parse_sql::parse_expr,
    prelude::{DataType, Error},
    translate::translate_expr,
};
use parquet::{basic::ConvertedType, format::KeyValue, schema::types::Type as SchemaType};
use std::convert::TryFrom;

use crate::data_type::{ParquetBasicConvertedType, ParquetBasicPhysicalType};

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
        // First, try to convert using logical type.
        let converted_type = inner.get_basic_info().converted_type();
        let physical_type = inner.get_physical_type();
        let mut data_type = match converted_type {
            ConvertedType::NONE => DataType::try_from(ParquetBasicPhysicalType(physical_type)),
            _ => match physical_type {
                parquet::basic::Type::BOOLEAN
                | parquet::basic::Type::FLOAT
                | parquet::basic::Type::DOUBLE => {
                    DataType::try_from(ParquetBasicPhysicalType(physical_type))
                }
                _ => DataType::try_from(ParquetBasicConvertedType(
                    inner.get_basic_info().converted_type(),
                )),
            },
        }?;
        let nullable = inner.is_optional();

        let mut unique = None;
        let mut default = None;
        println!();
        println!(
            "try from ParquetSchemaType to ColumnDef : get data type before metadata => {:?}",
            data_type
        );
        println!();
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
                    if kv.value.as_deref() == Some("Uuid") {
                        data_type = DataType::Uuid;
                    } else if kv.value.as_deref() == Some("Uint128") {
                        data_type = DataType::Uint128;
                    } else if kv.value.as_deref() == Some("Int128") {
                        data_type = DataType::Int128;
                    } else if kv.value.as_deref() == Some("Time") {
                        data_type = DataType::Time;
                    } else if kv.value.as_deref() == Some("Map") {
                        data_type = DataType::Map;
                    } else if kv.value.as_deref() == Some("List") {
                        data_type = DataType::List;
                    } else if kv.value.as_deref() == Some("Inet") {
                        data_type = DataType::Inet;
                    } else if kv.value.as_deref() == Some("Point") {
                        data_type = DataType::Point;
                    } else if kv.value.as_deref() == Some("Interval") {
                        data_type = DataType::Interval;
                    } else if kv.value.as_deref() == Some("Decimal") {
                        data_type = DataType::Decimal;
                    } else if kv.value.as_deref() == Some("Timestamp") {
                        data_type = DataType::Timestamp;
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
