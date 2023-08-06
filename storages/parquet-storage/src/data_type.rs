use gluesql_core::{ast::DataType, prelude::Error};
use parquet::basic::Type;

use crate::error::ParquetStorageError;

#[derive(Debug)]
pub struct ParquetBasicPhysicalType(pub Type);

impl ParquetBasicPhysicalType {
    pub fn as_physical_type(&self) -> &Type {
        &self.0
    }
}

impl TryFrom<ParquetBasicPhysicalType> for DataType {
    type Error = Error;

    fn try_from(wrapped_data_type: ParquetBasicPhysicalType) -> Result<Self, Self::Error> {
        let data_type = *wrapped_data_type.as_physical_type();
        match data_type {
            Type::BOOLEAN => Ok(DataType::Boolean),
            Type::INT32 => Ok(DataType::Int32),
            Type::INT64 => Ok(DataType::Int),
            Type::INT96 => Ok(DataType::Int128),
            Type::FLOAT => Ok(DataType::Float32),
            Type::DOUBLE => Ok(DataType::Float),
            Type::BYTE_ARRAY => Ok(DataType::Bytea),
            _ => Err(Error::StorageMsg(
                //todo: Type::FIXED_LEN_BYTE_ARRAY => Ok(GlueDataType::Bytea)
                ParquetStorageError::UnmappedParquetType(data_type).to_string(),
            )),
        }
    }
}

impl TryFrom<DataType> for ParquetBasicPhysicalType {
    type Error = Error;

    fn try_from(data_type: DataType) -> Result<Self, Self::Error> {
        let parquet_type = match data_type {
            DataType::Boolean => Ok(Type::BOOLEAN),
            DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Uint8
            | DataType::Uint16
            | DataType::Uint32 => Ok(Type::INT32),
            DataType::Int | DataType::Int128 | DataType::Uint64 | DataType::Uint128 => {
                Ok(Type::INT64)
            }
            DataType::Float => Ok(Type::DOUBLE),
            DataType::Float32 => Ok(Type::FLOAT),
            DataType::Bytea
            | DataType::Text
            | DataType::Date
            | DataType::Timestamp
            | DataType::Time
            | DataType::Interval
            | DataType::Uuid
            | DataType::Map
            | DataType::List
            | DataType::Decimal
            | DataType::Point => Ok(Type::BYTE_ARRAY),
            //todo: inet to parquet type
            _ => Err(Error::StorageMsg(
                ParquetStorageError::UnmappedGlueDataType(data_type).to_string(),
            )),
        };

        parquet_type.map(ParquetBasicPhysicalType)
    }
}
