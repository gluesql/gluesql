use {
    crate::{ast::ToSql, prelude::Value, result::Error, result::Result},
    bigdecimal::BigDecimal,
    bigdecimal::FromPrimitive,
    serde::{Deserialize, Serialize},
    strum_macros::Display,
    uuid::Uuid,
};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum AstLiteral {
    Boolean(bool),
    Number(BigDecimal),
    QuotedString(String),
    HexString(String),
    Null,
}

impl ToSql for AstLiteral {
    fn to_sql(&self) -> String {
        match self {
            AstLiteral::Boolean(b) => b.to_string().to_uppercase(),
            AstLiteral::Number(n) => n.to_string(),
            AstLiteral::QuotedString(qs) => format!(r#""{qs}""#),
            AstLiteral::HexString(hs) => format!(r#""{hs}""#),
            AstLiteral::Null => "NULL".to_owned(),
        }
    }
}

impl<'a> TryFrom<Value> for AstLiteral {
    type Error = Error;

    fn try_from(value: Value) -> Result<Self> {
        let ast_literal = match value {
            Value::Bool(v) => AstLiteral::Boolean(v),
            Value::I8(v) => AstLiteral::Number(BigDecimal::from_i8(v).unwrap()),
            Value::I16(v) => AstLiteral::Number(BigDecimal::from_i16(v).unwrap()),
            Value::I32(v) => AstLiteral::Number(BigDecimal::from_i32(v).unwrap()),
            Value::I64(v) => AstLiteral::Number(BigDecimal::from_i64(v).unwrap()),
            Value::I128(v) => AstLiteral::Number(BigDecimal::from_i128(v).unwrap()),
            Value::U8(v) => AstLiteral::Number(BigDecimal::from_u8(v).unwrap()),
            Value::F64(v) => AstLiteral::Number(BigDecimal::from_f64(v).unwrap()),
            Value::Decimal(v) => {
                AstLiteral::Number(BigDecimal::from_f64(v.try_into().unwrap()).unwrap())
            }
            Value::Str(v) => AstLiteral::QuotedString(v),
            Value::Bytea(v) => AstLiteral::HexString(hex::encode(v)),
            Value::Date(v) => AstLiteral::QuotedString(v.to_string()),
            Value::Timestamp(v) => AstLiteral::QuotedString(v.to_string()),
            // Value::Timestamp(v) => {
            //     AstLiteral::QuotedString(DateTime::<Utc>::from_utc(v, Utc).to_string().into())
            // }
            Value::Time(v) => AstLiteral::QuotedString(v.to_string()),
            Value::Interval(v) => AstLiteral::QuotedString(v.into()),
            Value::Uuid(v) => AstLiteral::QuotedString(Uuid::from_u128(v).hyphenated().to_string()),
            Value::Map(_) => todo!(),
            Value::List(_) => todo!(),
            Value::Null => AstLiteral::Null,
        };

        Ok(ast_literal)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Display)]
#[strum(serialize_all = "SCREAMING_SNAKE_CASE")]
pub enum DateTimeField {
    Year,
    Month,
    Day,
    Hour,
    Minute,
    Second,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, Display)]
#[strum(serialize_all = "SCREAMING_SNAKE_CASE")]
pub enum TrimWhereField {
    Both,
    Leading,
    Trailing,
}

#[cfg(test)]
mod tests {
    use {
        crate::{
            ast::{AstLiteral, ToSql},
            data::Interval,
            prelude::Value,
        },
        bigdecimal::BigDecimal,
        bigdecimal::FromPrimitive,
        chrono::{NaiveDate, NaiveTime},
        rust_decimal::Decimal,
    };

    #[test]
    fn to_sql() {
        assert_eq!("TRUE", AstLiteral::Boolean(true).to_sql());
        assert_eq!("123", AstLiteral::Number(BigDecimal::from(123)).to_sql());
        assert_eq!(
            r#""hello""#,
            AstLiteral::QuotedString("hello".to_owned()).to_sql()
        );
        assert_eq!("NULL", AstLiteral::Null.to_sql());
    }

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
            Value::Date(NaiveDate::from_ymd(2021, 8, 25)).try_into(),
            Ok(AstLiteral::QuotedString("2021-08-25".to_owned()))
        );
        assert_eq!(
            Value::Timestamp(NaiveDate::from_ymd(2021, 8, 25).and_hms_milli(8, 5, 30, 900))
                .try_into(),
            Ok(AstLiteral::QuotedString(
                "2021-08-25 08:05:30.900".to_owned()
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
        // assert_eq!(
        //     Value::Map(HashMap::from([("a".to_owned(), Value::Bool(true))])).try_into(),
        //     Ok(AstLiteral::QuotedString("todo".to_owned()))
        // );
        // assert_eq!(
        //     Value::List(vec![Value::Bool(true)]).try_into(),
        //     Ok(AstLiteral::QuotedString("todo".to_owned()))
        // );
        assert_eq!(Value::Null.try_into(), Ok(AstLiteral::Null));
    }
}
