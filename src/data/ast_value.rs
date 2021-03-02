use {
    super::error::DataError,
    crate::result::Result,
    sqlparser::ast::{DataType, Value as AstValue},
};

pub fn is_same_as_data_type_ast_value(value: &&AstValue, data_type: &DataType) -> bool {
    if !matches!(
        (data_type, value),
        (DataType::Boolean, AstValue::Boolean(_))
            | (DataType::Text, AstValue::SingleQuotedString(_))
            | (DataType::Float(_), AstValue::Number(_))
    ) {
        if let (DataType::Int, AstValue::Number(value)) = (data_type, value) {
            matches!(value.find('.'), None) // YUCK!
        } else {
            false
        }
    } else {
        true
    }
}

pub fn cast_ast_value(value: AstValue, data_type: &DataType) -> Result<AstValue> {
    match (data_type, value) {
        (DataType::Boolean, AstValue::SingleQuotedString(value))
        | (DataType::Boolean, AstValue::Number(value)) => Ok(match value.to_uppercase().as_str() {
            "TRUE" | "1" => Ok(AstValue::Boolean(true)),
            "FALSE" | "0" => Ok(AstValue::Boolean(false)),
            _ => Err(DataError::ImpossibleCast),
        }?),
        (DataType::Int, AstValue::Number(value)) => Ok(AstValue::Number(
            value
                .parse::<f64>()
                .map_err(|_| DataError::UnreachableImpossibleCast)?
                .trunc()
                .to_string(),
        )),
        (DataType::Int, AstValue::SingleQuotedString(value))
        | (DataType::Float(_), AstValue::SingleQuotedString(value)) => Ok(AstValue::Number(value)),
        (DataType::Int, AstValue::Boolean(value))
        | (DataType::Float(_), AstValue::Boolean(value)) => Ok(AstValue::Number(
            (if value { "1" } else { "0" }).to_string(),
        )),
        (DataType::Float(_), AstValue::Number(value)) => Ok(AstValue::Number(value)),
        (DataType::Text, AstValue::Boolean(value)) => Ok(AstValue::SingleQuotedString(
            (if value { "TRUE" } else { "FALSE" }).to_string(),
        )),
        (DataType::Text, AstValue::Number(value)) => Ok(AstValue::SingleQuotedString(value)),
        (_, AstValue::Null) => Ok(AstValue::Null),
        _ => Err(DataError::UnimplementedCast.into()),
    }
}
