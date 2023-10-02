use {
    super::ValueError::ValueToExprConversionFailure,
    crate::{
        ast::{AstLiteral, DateTimeField, Expr},
        chrono::{TimeZone, Utc},
        data::Interval,
        prelude::{DataType, Value},
        result::{Error, Result},
    },
    bigdecimal::{BigDecimal, FromPrimitive},
    serde_json::{Map as JsonMap, Value as JsonValue},
    uuid::Uuid,
};

impl TryFrom<Value> for Expr {
    type Error = Error;

    fn try_from(value: Value) -> Result<Self> {
        const SECOND: i64 = 1_000_000;

        let expr = match value {
            Value::Bool(v) => Expr::Literal(AstLiteral::Boolean(v)),
            Value::I8(v) => Expr::Literal(AstLiteral::Number(
                BigDecimal::from_i8(v).ok_or(ValueToExprConversionFailure)?,
            )),
            Value::I16(v) => Expr::Literal(AstLiteral::Number(
                BigDecimal::from_i16(v).ok_or(ValueToExprConversionFailure)?,
            )),
            Value::I32(v) => Expr::Literal(AstLiteral::Number(
                BigDecimal::from_i32(v).ok_or(ValueToExprConversionFailure)?,
            )),
            Value::I64(v) => Expr::Literal(AstLiteral::Number(
                BigDecimal::from_i64(v).ok_or(ValueToExprConversionFailure)?,
            )),
            Value::I128(v) => Expr::Literal(AstLiteral::Number(
                BigDecimal::from_i128(v).ok_or(ValueToExprConversionFailure)?,
            )),
            Value::U8(v) => Expr::Literal(AstLiteral::Number(
                BigDecimal::from_u8(v).ok_or(ValueToExprConversionFailure)?,
            )),
            Value::U16(v) => Expr::Literal(AstLiteral::Number(
                BigDecimal::from_u16(v).ok_or(ValueToExprConversionFailure)?,
            )),
            Value::U32(v) => Expr::Literal(AstLiteral::Number(
                BigDecimal::from_u32(v).ok_or(ValueToExprConversionFailure)?,
            )),
            Value::U64(v) => Expr::Literal(AstLiteral::Number(
                BigDecimal::from_u64(v).ok_or(ValueToExprConversionFailure)?,
            )),
            Value::U128(v) => Expr::Literal(AstLiteral::Number(
                BigDecimal::from_u128(v).ok_or(ValueToExprConversionFailure)?,
            )),
            Value::F32(v) => Expr::Literal(AstLiteral::Number(
                BigDecimal::from_f32(v).ok_or(ValueToExprConversionFailure)?,
            )),
            Value::F64(v) => Expr::Literal(AstLiteral::Number(
                BigDecimal::from_f64(v).ok_or(ValueToExprConversionFailure)?,
            )),
            Value::Decimal(v) => Expr::Literal(AstLiteral::Number(
                BigDecimal::from_f64(v.try_into().map_err(|_| ValueToExprConversionFailure)?)
                    .ok_or(ValueToExprConversionFailure)?,
            )),
            Value::Str(v) => Expr::Literal(AstLiteral::QuotedString(v)),
            Value::Bytea(v) => Expr::Literal(AstLiteral::HexString(hex::encode(v))),
            Value::Inet(v) => Expr::Literal(AstLiteral::QuotedString(v.to_string())),
            Value::Date(v) => Expr::TypedString {
                data_type: DataType::Date,
                value: v.to_string(),
            },
            Value::Timestamp(v) => Expr::TypedString {
                data_type: DataType::Timestamp,
                value: Utc.from_utc_datetime(&v).to_string(),
            },
            Value::Time(v) => Expr::TypedString {
                data_type: DataType::Time,
                value: v.to_string(),
            },
            Value::Interval(v) => match v {
                Interval::Month(v) => Expr::Interval {
                    expr: Box::new(Expr::Literal(AstLiteral::Number(
                        BigDecimal::from_i32(v).ok_or(ValueToExprConversionFailure)?,
                    ))),
                    leading_field: Some(DateTimeField::Month),
                    last_field: None,
                },
                Interval::Microsecond(v) => Expr::Interval {
                    expr: Box::new(Expr::Literal(AstLiteral::Number(
                        BigDecimal::from_i64(v / SECOND).ok_or(ValueToExprConversionFailure)?,
                    ))),
                    leading_field: Some(DateTimeField::Second),
                    last_field: None,
                },
            },
            Value::Uuid(v) => Expr::Literal(AstLiteral::QuotedString(
                Uuid::from_u128(v).hyphenated().to_string(),
            )),
            Value::Map(v) => {
                let json: JsonValue = v
                    .into_iter()
                    .map(|(key, value)| value.try_into().map(|value| (key, value)))
                    .collect::<Result<Vec<(String, JsonValue)>>>()
                    .map(|v| JsonMap::from_iter(v).into())
                    .map_err(|_| ValueToExprConversionFailure)?;

                Expr::Literal(AstLiteral::QuotedString(json.to_string()))
            }
            Value::List(v) => {
                let json: JsonValue = v
                    .into_iter()
                    .map(|value| value.try_into())
                    .collect::<Result<Vec<JsonValue>>>()
                    .map(|v| v.into())
                    .map_err(|_| ValueToExprConversionFailure)?;

                Expr::Literal(AstLiteral::QuotedString(json.to_string()))
            }
            Value::Point(v) => Expr::Literal(AstLiteral::QuotedString(v.to_string())),
            Value::Null => Expr::Literal(AstLiteral::Null),
        };

        Ok(expr)
    }
}

#[cfg(test)]
mod tests {
    use {
        crate::{
            ast::{AstLiteral, DateTimeField, Expr},
            data::{Interval, Point},
            prelude::{DataType, Value},
        },
        bigdecimal::{BigDecimal, FromPrimitive},
        chrono::{NaiveDate, NaiveTime},
        rust_decimal::Decimal,
        std::collections::HashMap,
    };

    #[test]
    fn value_to_expr() {
        assert_eq!(
            Value::Bool(true).try_into(),
            Ok(Expr::Literal(AstLiteral::Boolean(true)))
        );

        assert_eq!(
            Value::I8(127).try_into(),
            Ok(Expr::Literal(AstLiteral::Number(
                BigDecimal::from_i8(127).unwrap()
            )))
        );
        assert_eq!(
            Value::I16(32767).try_into(),
            Ok(Expr::Literal(AstLiteral::Number(
                BigDecimal::from_i16(32767).unwrap()
            )))
        );
        assert_eq!(
            Value::I32(2147483647).try_into(),
            Ok(Expr::Literal(AstLiteral::Number(
                BigDecimal::from_i32(2147483647).unwrap()
            )))
        );
        assert_eq!(
            Value::I64(64).try_into(),
            Ok(Expr::Literal(AstLiteral::Number(
                BigDecimal::from_i64(64).unwrap()
            )))
        );
        assert_eq!(
            Value::I128(128).try_into(),
            Ok(Expr::Literal(AstLiteral::Number(
                BigDecimal::from_i128(128).unwrap()
            )))
        );
        assert_eq!(
            Value::U8(8).try_into(),
            Ok(Expr::Literal(AstLiteral::Number(
                BigDecimal::from_u8(8).unwrap()
            )))
        );
        assert_eq!(
            Value::U16(16).try_into(),
            Ok(Expr::Literal(AstLiteral::Number(
                BigDecimal::from_u16(16).unwrap()
            )))
        );
        assert_eq!(
            Value::U32(32).try_into(),
            Ok(Expr::Literal(AstLiteral::Number(
                BigDecimal::from_u32(32).unwrap()
            )))
        );
        assert_eq!(
            Value::U64(64).try_into(),
            Ok(Expr::Literal(AstLiteral::Number(
                BigDecimal::from_u64(64).unwrap()
            )))
        );
        assert_eq!(
            Value::U128(128).try_into(),
            Ok(Expr::Literal(AstLiteral::Number(
                BigDecimal::from_u128(128).unwrap()
            )))
        );

        assert_eq!(
            Value::F32(64.4_f32).try_into(),
            Ok(Expr::Literal(AstLiteral::Number(
                BigDecimal::from_f32(64.4).unwrap()
            )))
        );
        assert_eq!(
            Value::F64(64.4).try_into(),
            Ok(Expr::Literal(AstLiteral::Number(
                BigDecimal::from_f64(64.4).unwrap()
            )))
        );
        assert_eq!(
            Value::Decimal(Decimal::new(315, 2)).try_into(),
            Ok(Expr::Literal(AstLiteral::Number(
                BigDecimal::from_f64(3.15).unwrap()
            )))
        );
        assert_eq!(
            Value::Str("data".to_owned()).try_into(),
            Ok(Expr::Literal(AstLiteral::QuotedString("data".to_owned())))
        );
        assert_eq!(
            Value::Bytea(hex::decode("1234").unwrap()).try_into(),
            Ok(Expr::Literal(AstLiteral::HexString("1234".to_owned())))
        );
        assert_eq!(
            Value::Date(NaiveDate::from_ymd_opt(2022, 11, 3).unwrap()).try_into(),
            Ok(Expr::TypedString {
                data_type: DataType::Date,
                value: "2022-11-03".to_owned(),
            })
        );
        assert_eq!(
            Value::Timestamp(
                NaiveDate::from_ymd_opt(2022, 11, 3)
                    .unwrap()
                    .and_hms_milli_opt(8, 5, 30, 900)
                    .unwrap()
            )
            .try_into(),
            Ok(Expr::TypedString {
                data_type: DataType::Timestamp,
                value: "2022-11-03 08:05:30.900 UTC".to_owned(),
            }),
        );
        assert_eq!(
            Value::Time(NaiveTime::from_hms_opt(20, 11, 59).unwrap()).try_into(),
            Ok(Expr::TypedString {
                data_type: DataType::Time,
                value: "20:11:59".to_owned()
            }),
        );
        assert_eq!(
            Value::Interval(Interval::Month(1)).try_into(),
            Ok(Expr::Interval {
                expr: Box::new(Expr::Literal(AstLiteral::Number(
                    BigDecimal::from_i64(1).unwrap()
                ))),
                leading_field: Some(DateTimeField::Month),
                last_field: None
            })
        );
        assert_eq!(
            Value::Uuid(195965723427462096757863453463987888808).try_into(),
            Ok(Expr::Literal(AstLiteral::QuotedString(
                "936da01f-9abd-4d9d-80c7-02af85c822a8".to_owned()
            )))
        );
        assert_eq!(
            Value::Map(HashMap::from([("a".to_owned(), Value::Bool(true))])).try_into(),
            Ok(Expr::Literal(AstLiteral::QuotedString(
                "{\"a\":true}".to_owned()
            )))
        );
        assert_eq!(
            Value::List(vec![
                Value::I64(1),
                Value::Bool(true),
                Value::Str("a".to_owned())
            ])
            .try_into(),
            Ok(Expr::Literal(AstLiteral::QuotedString(
                "[1,true,\"a\"]".to_owned()
            )))
        );
        assert_eq!(Value::Null.try_into(), Ok(Expr::Literal(AstLiteral::Null)));
        assert_eq!(
            Value::Point(Point::new(0.31413, 0.3415)).try_into(),
            Ok(Expr::Literal(AstLiteral::QuotedString(
                "POINT(0.31413 0.3415)".to_owned()
            )))
        );
    }
}
