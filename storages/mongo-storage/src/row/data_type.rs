use {
    gluesql_core::prelude::DataType,
    strum_macros::{EnumString, IntoStaticStr},
};

#[derive(IntoStaticStr, EnumString)]
pub enum BsonType {
    #[strum(to_string = "double")]
    Double,
    #[strum(to_string = "string")]
    String,
    #[strum(to_string = "object")]
    Object,
    #[strum(to_string = "array")]
    Array,
    #[strum(to_string = "binData")]
    Binary,
    #[strum(to_string = "undefined")]
    Undefined,
    #[strum(to_string = "objectId")]
    ObjectId,
    #[strum(to_string = "bool")]
    Boolean,
    #[strum(to_string = "date")]
    Date,
    #[strum(to_string = "null")]
    Null,
    #[strum(to_string = "regex")]
    RegularExpression,
    #[strum(to_string = "dbPointer")]
    DbPointer,
    #[strum(to_string = "javascript")]
    JavaScript,
    #[strum(to_string = "symbol")]
    Symbol,
    #[strum(to_string = "javascriptWithScope")]
    JavaScriptCodeWithScope,
    #[strum(to_string = "int")]
    Int32,
    #[strum(to_string = "timestamp")]
    Timestamp,
    #[strum(to_string = "long")]
    Int64,
    #[strum(to_string = "decimal")]
    Decimal128,
    #[strum(to_string = "minKey")]
    MinKey,
    #[strum(to_string = "maxKey")]
    MaxKey,
}

impl From<&DataType> for BsonType {
    fn from(data_type: &DataType) -> BsonType {
        match data_type {
            DataType::Boolean => BsonType::Boolean,
            DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Uint8
            | DataType::Uint16 => BsonType::Int32,
            DataType::Int | DataType::Uint32 => BsonType::Int64,
            DataType::Int128 | DataType::Uint64 | DataType::Uint128 | DataType::Decimal => {
                BsonType::Decimal128
            }
            DataType::Float32 | DataType::Float => BsonType::Double,
            DataType::Text | DataType::Timestamp | DataType::Inet | DataType::Interval => {
                BsonType::String
            }
            DataType::Bytea | DataType::Uuid => BsonType::Binary,
            DataType::Date | DataType::Time => BsonType::Date,
            DataType::Map | DataType::Point => BsonType::Object,
            DataType::List => BsonType::Array,
        }
    }
}

pub const B15: i64 = 2_i64.pow(15);
pub const B7: i64 = 2_i64.pow(7);
pub const B31: i64 = 2_i64.pow(31);
pub const TIME: i64 = 86400000 - 1;

pub trait IntoRange {
    fn get_max(&self) -> Option<i64>;
    fn get_min(&self) -> Option<i64>;
}

impl IntoRange for DataType {
    fn get_max(&self) -> Option<i64> {
        match self {
            DataType::Int8 => Some(B7),
            DataType::Int16 => Some(B15),
            DataType::Int32 | DataType::Float32 => Some(B31),
            DataType::Time => Some(TIME),
            _ => None,
        }
    }

    fn get_min(&self) -> Option<i64> {
        match self {
            DataType::Time => Some(0),
            v => v.get_max().map(|max| -max),
        }
    }
}
