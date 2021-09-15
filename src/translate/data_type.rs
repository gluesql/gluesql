use {
    super::TranslateError,
    crate::{ast::DataType, result::Result},
    sqlparser::ast::DataType as SqlDataType,
};

pub fn translate_data_type(sql_data_type: &SqlDataType) -> Result<DataType> {
    match sql_data_type {
        SqlDataType::Boolean => Ok(DataType::Boolean),
        SqlDataType::Int(_) => Ok(DataType::Int),
        SqlDataType::Float(_) => Ok(DataType::Float),
        SqlDataType::Text => Ok(DataType::Text),
        SqlDataType::Date => Ok(DataType::Date),
        SqlDataType::Timestamp => Ok(DataType::Timestamp),
        SqlDataType::Time => Ok(DataType::Time),
        SqlDataType::Interval => Ok(DataType::Interval),
        SqlDataType::Uuid => Ok(DataType::UUID),
        _ => Err(TranslateError::UnsupportedDataType(sql_data_type.to_string()).into()),
    }
}
