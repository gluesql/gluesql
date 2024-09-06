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
        SqlDataType::Int(None) | SqlDataType::Integer(None) | SqlDataType::Int64 => {
            Ok(DataType::Int)
        }
        SqlDataType::Int8(None) => Ok(DataType::Int8),
        SqlDataType::Int16 => Ok(DataType::Int16),
        SqlDataType::Int32 => Ok(DataType::Int32),
        SqlDataType::Int128 => Ok(DataType::Int128),
        SqlDataType::UInt8 => Ok(DataType::Uint8),
        SqlDataType::UInt16 => Ok(DataType::Uint16),
        SqlDataType::UInt32 => Ok(DataType::Uint32),
        SqlDataType::UInt64 => Ok(DataType::Uint64),
        SqlDataType::UInt128 => Ok(DataType::Uint128),

        SqlDataType::Float32 => Ok(DataType::Float32),
        SqlDataType::Float64 => Ok(DataType::Float),

        SqlDataType::UnsignedInt(None) | SqlDataType::UnsignedInteger(None) => Ok(DataType::Uint64),
        SqlDataType::UnsignedInt8(None) => Ok(DataType::Uint8),

        SqlDataType::Float(None) | SqlDataType::Float(Some(64)) => Ok(DataType::Float),

        SqlDataType::Text => Ok(DataType::Text),
        SqlDataType::Bytea => Ok(DataType::Bytea),
        SqlDataType::Date => Ok(DataType::Date),
        SqlDataType::Timestamp(None, SqlTimezoneInfo::None) => Ok(DataType::Timestamp),
        SqlDataType::Time(None, SqlTimezoneInfo::None) => Ok(DataType::Time),
        SqlDataType::Interval => Ok(DataType::Interval),
        SqlDataType::Uuid => Ok(DataType::Uuid),
        SqlDataType::Decimal(SqlExactNumberInfo::None) => Ok(DataType::Decimal),
        SqlDataType::Custom(name, _idents) => {
            let name = name.0.first().map(|v| v.value.to_uppercase());

            match name.as_deref() {
                Some("MAP") => Ok(DataType::Map),
                Some("LIST") => Ok(DataType::List),
                Some("POINT") => Ok(DataType::Point),
                Some("INET") => Ok(DataType::Inet),

                _ => Err(TranslateError::UnsupportedDataType(sql_data_type.to_string()).into()),
            }
        }
        _ => Err(TranslateError::UnsupportedDataType(sql_data_type.to_string()).into()),
    }
}

#[cfg(test)]
mod tests {
    use {super::*, crate::parse_sql::parse_data_type, sqlparser::ast::ObjectName};

    #[test]
    fn support_data_type() {
        macro_rules! test {
            ($text:literal => $parser:expr => $gluesql: expr) => {
                assert_eq!(parse_data_type($text), Ok($parser));
                assert_eq!(translate_data_type(&$parser), $gluesql);
            };
        }

        test!("BOOLEAN" => SqlDataType::Boolean => Ok(DataType::Boolean));

        test!("INT" => SqlDataType::Int(None) => Ok(DataType::Int));
        test!("INTEGER" => SqlDataType::Integer(None) => Ok(DataType::Int));
        test!("INT64" => SqlDataType::Int64 => Ok(DataType::Int));

        test!("INT8" => SqlDataType::Int8(None) => Ok(DataType::Int8));

        test!("INT UNSIGNED" => SqlDataType::UnsignedInt(None) => Ok(DataType::Uint64));
        test!("INTEGER UNSIGNED" => SqlDataType::UnsignedInteger(None) => Ok(DataType::Uint64));

        test!("INT8 UNSIGNED" => SqlDataType::UnsignedInt8(None) => Ok(DataType::Uint8));

        test!("FLOAT" => SqlDataType::Float(None) => Ok(DataType::Float));
        test!("FLOAT(64)" => SqlDataType::Float(Some(64)) => Ok(DataType::Float));

        test!("TEXT" => SqlDataType::Text => Ok(DataType::Text));

        test!("BYTEA" => SqlDataType::Bytea => Ok(DataType::Bytea));

        test!("DATE" => SqlDataType::Date => Ok(DataType::Date));
        test!("TIMESTAMP" => SqlDataType::Timestamp(None, SqlTimezoneInfo::None) => Ok(DataType::Timestamp));
        test!("TIME" => SqlDataType::Time(None, SqlTimezoneInfo::None) =>  Ok(DataType::Time));
        test!("INTERVAL" => SqlDataType::Interval => Ok(DataType::Interval));
        test!("UUID" => SqlDataType::Uuid => Ok(DataType::Uuid));
        test!("DECIMAL" => SqlDataType::Decimal(SqlExactNumberInfo::None) => Ok(DataType::Decimal));
    }

    #[test]
    fn support_custom_data_type() {
        macro_rules! test {
            ($text:literal => $gluesql: expr) => {
                assert_eq!(
                    translate_data_type(&SqlDataType::Custom(
                        ObjectName(vec![$text.into()]),
                        vec![]
                    )),
                    $gluesql
                );
            };
        }

        test!("MAP" => Ok(DataType::Map));
        test!("LIST" => Ok(DataType::List));
        test!("POINT" => Ok(DataType::Point));
        test!("INET" => Ok(DataType::Inet));
    }
}
