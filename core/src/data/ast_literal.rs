use {
    crate::{ast::AstLiteral, prelude::Value, result::Error, result::Result},
    bigdecimal::{BigDecimal, FromPrimitive},
    chrono::{DateTime, Utc},
    serde::Serialize,
    serde_json::{Map as JsonMap, Value as JsonValue},
    thiserror::Error,
    uuid::Uuid,
};

#[derive(Error, Serialize, Debug, PartialEq)]
pub enum AstLiteralError {
    #[error("impossible cast")]
    ImpossibleCast,
}

impl TryFrom<Value> for AstLiteral {
    type Error = Error;

    fn try_from(value: Value) -> Result<Self> {
        let ast_literal = match value {
            Value::Bool(v) => AstLiteral::Boolean(v),
            Value::I8(v) => {
                AstLiteral::Number(BigDecimal::from_i8(v).ok_or(AstLiteralError::ImpossibleCast)?)
            }
            Value::I16(v) => {
                AstLiteral::Number(BigDecimal::from_i16(v).ok_or(AstLiteralError::ImpossibleCast)?)
            }
            Value::I32(v) => {
                AstLiteral::Number(BigDecimal::from_i32(v).ok_or(AstLiteralError::ImpossibleCast)?)
            }
            Value::I64(v) => {
                AstLiteral::Number(BigDecimal::from_i64(v).ok_or(AstLiteralError::ImpossibleCast)?)
            }
            Value::I128(v) => {
                AstLiteral::Number(BigDecimal::from_i128(v).ok_or(AstLiteralError::ImpossibleCast)?)
            }
            Value::U8(v) => {
                AstLiteral::Number(BigDecimal::from_u8(v).ok_or(AstLiteralError::ImpossibleCast)?)
            }
            Value::F64(v) => {
                AstLiteral::Number(BigDecimal::from_f64(v).ok_or(AstLiteralError::ImpossibleCast)?)
            }
            Value::Decimal(v) => AstLiteral::Number(
                BigDecimal::from_f64(v.try_into().map_err(|_| AstLiteralError::ImpossibleCast)?)
                    .ok_or(AstLiteralError::ImpossibleCast)?,
            ),
            Value::Str(v) => AstLiteral::QuotedString(v),
            Value::Bytea(v) => AstLiteral::HexString(hex::encode(v)),
            Value::Date(v) => AstLiteral::QuotedString(v.to_string()),
            Value::Timestamp(v) => {
                AstLiteral::QuotedString(DateTime::<Utc>::from_utc(v, Utc).to_string())
            }
            Value::Time(v) => AstLiteral::QuotedString(v.to_string()),
            Value::Interval(v) => AstLiteral::QuotedString(v.into()),
            Value::Uuid(v) => AstLiteral::QuotedString(Uuid::from_u128(v).hyphenated().to_string()),
            Value::Map(v) => {
                let json: JsonValue = v
                    .into_iter()
                    .map(|(key, value)| value.try_into().map(|value| (key, value)))
                    .collect::<Result<Vec<(String, JsonValue)>>>()
                    .map(|v| JsonMap::from_iter(v).into())
                    .map_err(|_| AstLiteralError::ImpossibleCast)?;

                AstLiteral::QuotedString(json.to_string())
            }
            Value::List(v) => {
                let json: JsonValue = v
                    .into_iter()
                    .map(|value| value.try_into())
                    .collect::<Result<Vec<JsonValue>>>()
                    .map(|v| v.into())
                    .map_err(|_| AstLiteralError::ImpossibleCast)?;

                AstLiteral::QuotedString(json.to_string())
            }
            Value::Null => AstLiteral::Null,
        };

        Ok(ast_literal)
    }
}
#[cfg(test)]
mod tests {
    use {
        crate::{ast::AstLiteral, data::Interval, prelude::Value},
        bigdecimal::BigDecimal,
        bigdecimal::FromPrimitive,
        chrono::{NaiveDate, NaiveTime},
        rust_decimal::Decimal,
        std::collections::HashMap,
    };

    #[test]
    fn value_to_literal() {
        assert_eq!(Value::Bool(true).try_into(), Ok(AstLiteral::Boolean(true)));

        assert_eq!(
            Value::I8(127).try_into(),
            Ok(AstLiteral::Number(BigDecimal::from_i8(127).unwrap()))
        );
        assert_eq!(
            Value::I16(32767).try_into(),
            Ok(AstLiteral::Number(BigDecimal::from_i16(32767).unwrap()))
        );
        assert_eq!(
            Value::I32(2147483647).try_into(),
            Ok(AstLiteral::Number(
                BigDecimal::from_i32(2147483647).unwrap()
            ))
        );
        assert_eq!(
            Value::I64(64).try_into(),
            Ok(AstLiteral::Number(BigDecimal::from_i64(64).unwrap()))
        );
        assert_eq!(
            Value::I128(128).try_into(),
            Ok(AstLiteral::Number(BigDecimal::from_i128(128).unwrap()))
        );
        assert_eq!(
            Value::U8(8).try_into(),
            Ok(AstLiteral::Number(BigDecimal::from_u8(8).unwrap()))
        );
        assert_eq!(
            Value::F64(64.4).try_into(),
            Ok(AstLiteral::Number(BigDecimal::from_f64(64.4).unwrap()))
        );
        assert_eq!(
            Value::Decimal(Decimal::new(315, 2)).try_into(),
            Ok(AstLiteral::Number(BigDecimal::from_f64(3.15).unwrap()))
        );
        assert_eq!(
            Value::Str("data".to_owned()).try_into(),
            Ok(AstLiteral::QuotedString("data".to_owned()))
        );
        assert_eq!(
            Value::Bytea(hex::decode("1234").unwrap()).try_into(),
            Ok(AstLiteral::HexString("1234".to_owned()))
        );
        assert_eq!(
            Value::Date(NaiveDate::from_ymd(2022, 11, 3)).try_into(),
            Ok(AstLiteral::QuotedString("2022-11-03".to_owned()))
        );
        assert_eq!(
            Value::Timestamp(NaiveDate::from_ymd(2022, 11, 3).and_hms_milli(8, 5, 30, 900))
                .try_into(),
            Ok(AstLiteral::QuotedString(
                "2022-11-03 08:05:30.900 UTC".to_owned()
            ))
        );
        assert_eq!(
            Value::Time(NaiveTime::from_hms(20, 11, 59)).try_into(),
            Ok(AstLiteral::QuotedString("20:11:59".to_owned()))
        );
        assert_eq!(
            Value::Interval(Interval::Month(1)).try_into(),
            Ok(AstLiteral::QuotedString("\"1\" MONTH".to_owned()))
        );
        assert_eq!(
            Value::Uuid(195965723427462096757863453463987888808).try_into(),
            Ok(AstLiteral::QuotedString(
                "936da01f-9abd-4d9d-80c7-02af85c822a8".to_owned()
            ))
        );
        assert_eq!(
            Value::Map(HashMap::from([("a".to_owned(), Value::Bool(true))])).try_into(),
            Ok(AstLiteral::QuotedString("{\"a\":true}".to_owned()))
        );
        assert_eq!(
            Value::List(vec![
                Value::I64(1),
                Value::Bool(true),
                Value::Str("a".to_owned())
            ])
            .try_into(),
            Ok(AstLiteral::QuotedString("[1,true,\"a\"]".to_owned()))
        );
        assert_eq!(Value::Null.try_into(), Ok(AstLiteral::Null));
    }
}
