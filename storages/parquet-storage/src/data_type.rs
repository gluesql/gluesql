use gluesql_core::{ast::DataType, prelude::Error};
use parquet::basic::{ConvertedType, Type};

use crate::error::ParquetStorageError;

#[derive(Debug)]
pub struct ParquetBasicPhysicalType(pub Type);

#[derive(Debug)]
pub struct ParquetBasicConvertedType(pub ConvertedType);

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
            Type::FLOAT => Ok(DataType::Float32),
            Type::DOUBLE => Ok(DataType::Float),
            Type::INT96 => Ok(DataType::Int128),
            Type::BYTE_ARRAY | Type::FIXED_LEN_BYTE_ARRAY => Ok(DataType::Bytea),
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
            | DataType::Uint32
            | DataType::Date => Ok(Type::INT32),
            DataType::Int | DataType::Time => Ok(Type::INT64),
            DataType::Float => Ok(Type::DOUBLE),
            DataType::Float32 => Ok(Type::FLOAT),
            DataType::Bytea
            | DataType::Text
            | DataType::Map
            | DataType::List
            | DataType::Decimal
            | DataType::Point
            | DataType::Uint64
            | DataType::Uint128
            | DataType::Int128
            | DataType::Inet
            | DataType::Interval
            | DataType::Timestamp => Ok(Type::BYTE_ARRAY),
            DataType::Uuid => Ok(Type::FIXED_LEN_BYTE_ARRAY),
        };

        parquet_type.map(ParquetBasicPhysicalType)
    }
}

impl ParquetBasicConvertedType {
    pub fn as_converted_type(&self) -> &ConvertedType {
        &self.0
    }
}

impl TryFrom<ParquetBasicConvertedType> for DataType {
    type Error = Error;

    fn try_from(wrapped_data_type: ParquetBasicConvertedType) -> Result<Self, Self::Error> {
        let data_type = wrapped_data_type.as_converted_type();
        match data_type {
            ConvertedType::UTF8 => Ok(DataType::Text),
            ConvertedType::DATE => Ok(DataType::Date),
            ConvertedType::JSON => Ok(DataType::Text),
            ConvertedType::BSON => Ok(DataType::Bytea),
            ConvertedType::MAP_KEY_VALUE => Ok(DataType::Map),
            ConvertedType::UINT_8 => Ok(DataType::Uint8),
            ConvertedType::UINT_16 => Ok(DataType::Uint16),
            ConvertedType::UINT_32 => Ok(DataType::Uint32),
            ConvertedType::UINT_64 => Ok(DataType::Uint64),
            ConvertedType::INT_8 => Ok(DataType::Int8),
            ConvertedType::INT_16 => Ok(DataType::Int16),
            ConvertedType::INT_32 => Ok(DataType::Int32),
            ConvertedType::INT_64 => Ok(DataType::Int),
            ConvertedType::TIME_MILLIS => Ok(DataType::Time),
            _ => Err(Error::StorageMsg(
                //todo: Type::FIXED_LEN_BYTE_ARRAY => Ok(GlueDataType::Bytea)
                ParquetStorageError::UnmappedParquetConvertedType(*data_type).to_string(),
            )),
        }
    }
}

impl TryFrom<DataType> for ParquetBasicConvertedType {
    type Error = Error;

    fn try_from(data_type: DataType) -> Result<Self, Self::Error> {
        let converted_type = match data_type {
            DataType::Text => Ok(ConvertedType::UTF8),
            DataType::Date => Ok(ConvertedType::DATE),
            DataType::Uint8 => Ok(ConvertedType::UINT_8),
            DataType::Int => Ok(ConvertedType::INT_64),
            DataType::Int8 => Ok(ConvertedType::INT_8),
            DataType::Int16 => Ok(ConvertedType::INT_16),
            DataType::Int32 => Ok(ConvertedType::INT_32),
            DataType::Uint16 => Ok(ConvertedType::UINT_16),
            DataType::Uint32 => Ok(ConvertedType::UINT_32),
            DataType::Uint64 => Ok(ConvertedType::UINT_64),
            _ => Err(Error::StorageMsg(
                ParquetStorageError::UnmappedGlueDataType(data_type).to_string(),
            )),
        };

        converted_type.map(ParquetBasicConvertedType)
    }
}
