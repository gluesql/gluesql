use {
    crate::{
        data::{Interval, Value},
        result::{Error, Result},
    },
    chrono::{NaiveDate, NaiveDateTime, NaiveTime},
    rust_decimal::Decimal,
    serde::{Deserialize, Serialize},
    std::{cmp::Ordering, fmt::Debug},
    thiserror::Error as ThisError,
};

#[derive(ThisError, Debug, PartialEq, Serialize)]
pub enum KeyError {
    #[error("FLOAT data type cannot be used as Key")]
    FloatTypeKeyNotSupported,

    #[error("MAP data type cannot be used as Key")]
    MapTypeKeyNotSupported,

    #[error("LIST data type cannot be used as Key")]
    ListTypeKeyNotSupported,
}

#[derive(PartialEq, Eq, Hash, Clone, Debug, Serialize, Deserialize)]
pub enum Key {
    I8(i8),
    I64(i64),
    Bool(bool),
    Str(String),
    Date(NaiveDate),
    Timestamp(NaiveDateTime),
    Time(NaiveTime),
    Interval(Interval),
    Uuid(u128),
    Decimal(Decimal),
    None,
}

impl PartialOrd for Key {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match (self, other) {
            (Key::I8(l), Key::I8(r)) => Some(l.cmp(r)),
            (Key::I64(l), Key::I64(r)) => Some(l.cmp(r)),
            (Key::Bool(l), Key::Bool(r)) => Some(l.cmp(r)),
            (Key::Date(l), Key::Date(r)) => Some(l.cmp(r)),
            (Key::Timestamp(l), Key::Timestamp(r)) => Some(l.cmp(r)),
            (Key::Time(l), Key::Time(r)) => Some(l.cmp(r)),
            (Key::Interval(l), Key::Interval(r)) => l.partial_cmp(r),
            (Key::Uuid(l), Key::Uuid(r)) => Some(l.cmp(r)),
            (Key::Decimal(l), Key::Decimal(r)) => Some(l.cmp(r)),
            _ => None,
        }
    }
}

impl TryFrom<Value> for Key {
    type Error = Error;

    fn try_from(value: Value) -> Result<Self> {
        use Value::*;

        match value {
            Bool(v) => Ok(Key::Bool(v)),
            I8(v) => Ok(Key::I8(v)),
            I64(v) => Ok(Key::I64(v)),
            Str(v) => Ok(Key::Str(v)),
            Date(v) => Ok(Key::Date(v)),
            Timestamp(v) => Ok(Key::Timestamp(v)),
            Time(v) => Ok(Key::Time(v)),
            Interval(v) => Ok(Key::Interval(v)),
            Uuid(v) => Ok(Key::Uuid(v)),
            Decimal(v) => Ok(Key::Decimal(v)),
            Null => Ok(Key::None),
            F64(_) => Err(KeyError::FloatTypeKeyNotSupported.into()),
            Map(_) => Err(KeyError::MapTypeKeyNotSupported.into()),
            List(_) => Err(KeyError::ListTypeKeyNotSupported.into()),
        }
    }
}

impl TryFrom<&Value> for Key {
    type Error = Error;

    fn try_from(value: &Value) -> Result<Self> {
        value.clone().try_into()
    }
}

#[cfg(test)]
mod tests {
    use {
        crate::{
            data::{Key, KeyError, Value},
            executor::evaluate_stateless,
            parse_sql::parse_expr,
            result::Result,
            translate::translate_expr,
        },
        std::collections::HashMap,
    };

    fn convert(sql: &str) -> Result<Key> {
        let parsed = parse_expr(sql).expect(sql);
        let expr = translate_expr(&parsed).expect(sql);

        evaluate_stateless(None, &expr).expect(sql).try_into()
    }

    #[test]
    fn evaluated_to_key() {
        // Some
        assert_eq!(convert("True"), Ok(Key::Bool(true)));
        assert_eq!(convert("CAST(11 AS INT(8))"), Ok(Key::I8(11)));
        assert_eq!(convert("2048"), Ok(Key::I64(2048)));
        assert_eq!(
            convert(r#""Hello World""#),
            Ok(Key::Str("Hello World".to_owned()))
        );
        assert!(matches!(convert(r#"DATE "2022-03-03""#), Ok(Key::Date(_))));
        assert!(matches!(convert(r#"TIME "12:30:00""#), Ok(Key::Time(_))));
        assert!(matches!(
            convert(r#"TIMESTAMP "2022-03-03 12:30:00Z""#),
            Ok(Key::Timestamp(_))
        ));
        assert!(matches!(
            convert(r#"INTERVAL "1" DAY"#),
            Ok(Key::Interval(_))
        ));
        assert!(matches!(convert("GENERATE_UUID()"), Ok(Key::Uuid(_))));

        // None
        assert_eq!(convert("NULL"), Ok(Key::None));

        // Error
        assert_eq!(
            convert("12.03"),
            Err(KeyError::FloatTypeKeyNotSupported.into())
        );
        assert_eq!(
            Key::try_from(Value::Map(HashMap::default())),
            Err(KeyError::MapTypeKeyNotSupported.into())
        );
        assert_eq!(
            Key::try_from(Value::List(Vec::default())),
            Err(KeyError::ListTypeKeyNotSupported.into())
        );
    }
}
