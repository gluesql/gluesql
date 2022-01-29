use {
    super::TranslateError,
    crate::{ast::DataType, result::Result},
    sqlparser::ast::DataType as SqlDataType,
};

pub fn translate_data_type(sql_data_type: &SqlDataType) -> Result<DataType> {
    match sql_data_type {
        SqlDataType::Boolean => Ok(DataType::Boolean),
        SqlDataType::Int(Some(8)) => Ok(DataType::Int8),
        SqlDataType::Int(_) => Ok(DataType::Int),
        SqlDataType::Float(_) => Ok(DataType::Float),
        SqlDataType::Text => Ok(DataType::Text),
        SqlDataType::Date => Ok(DataType::Date),
        SqlDataType::Timestamp => Ok(DataType::Timestamp),
        SqlDataType::Time => Ok(DataType::Time),
        SqlDataType::Interval => Ok(DataType::Interval),
        SqlDataType::Uuid => Ok(DataType::Uuid),
        // SqlDataType::Decimal(None, None) => Ok(DataType::Decimal),
        // SqlDataType::Decimal(p, None) => Ok(DataType::DecimalPrecision(p.unwrap())),
        SqlDataType::Decimal(p, s) => Ok(DataType::Decimal(*p, *s)),
        SqlDataType::Custom(name) => {
            let name = name.0.get(0).map(|v| v.value.to_uppercase());

            match name.as_deref() {
                Some("MAP") => Ok(DataType::Map),
                Some("LIST") => Ok(DataType::List),
                _ => Err(TranslateError::UnsupportedDataType(sql_data_type.to_string()).into()),
            }
        }
        _ => Err(TranslateError::UnsupportedDataType(sql_data_type.to_string()).into()),
    }
}
