use {
    super::TranslateError,
    crate::{ast::DataType, result::Result},
    sqlparser::ast::{
        DataType as SqlDataType, ExactNumberInfo as SqlExactNumberInfo,
        TimezoneInfo as SqlTimezoneInfo,
    },
};

pub fn translate_data_type(sql_data_type: &SqlDataType) -> Result<DataType> {
    match sql_data_type {
        SqlDataType::Boolean => Ok(DataType::Boolean),
        SqlDataType::Int(None) | SqlDataType::Integer(None) => Ok(DataType::Int),
        SqlDataType::Float(_) => Ok(DataType::Float),
        SqlDataType::Text => Ok(DataType::Text),
        SqlDataType::Bytea => Ok(DataType::Bytea),
        SqlDataType::Date => Ok(DataType::Date),
        SqlDataType::Timestamp(None, SqlTimezoneInfo::None) => Ok(DataType::Timestamp),
        SqlDataType::Time(None, SqlTimezoneInfo::None) => Ok(DataType::Time),
        SqlDataType::Interval => Ok(DataType::Interval),
        SqlDataType::Uuid => Ok(DataType::Uuid),
        SqlDataType::Decimal(SqlExactNumberInfo::None) => Ok(DataType::Decimal),
        SqlDataType::Custom(name, _idents) => {
            let name = name.0.get(0).map(|v| v.value.to_uppercase());

            match name.as_deref() {
                Some("MAP") => Ok(DataType::Map),
                Some("LIST") => Ok(DataType::List),
                Some("INT8") => Ok(DataType::Int8),
                Some("INT16") => Ok(DataType::Int16),
                Some("INT32") => Ok(DataType::Int32),
                Some("INT128") => Ok(DataType::Int128),
                Some("UINT8") => Ok(DataType::Uint8),
                Some("UINT16") => Ok(DataType::Uint16),
                Some("UINT32") => Ok(DataType::Uint32),
                Some("UINT64") => Ok(DataType::Uint64),
                Some("UINT128") => Ok(DataType::Uint128),
                Some("INET") => Ok(DataType::Inet),

                _ => Err(TranslateError::UnsupportedDataType(sql_data_type.to_string()).into()),
            }
        }
        _ => Err(TranslateError::UnsupportedDataType(sql_data_type.to_string()).into()),
    }
}
