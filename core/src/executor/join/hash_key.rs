use {
    crate::{
        data::{Interval, Value},
        executor::evaluate::Evaluated,
        result::{Error, Result},
    },
    chrono::{NaiveDate, NaiveDateTime, NaiveTime},
    rust_decimal::Decimal,
    std::fmt::Debug,
};

#[derive(PartialEq, Eq, Hash, Clone, Debug)]
pub enum HashKey {
    I8(i8),
    I32(i32),
    I64(i64),
    I128(i128),
    U8(u8),
    U32(u32),
    U64(u64),
    U128(u128),
    Bool(bool),
    Str(String),
    Date(NaiveDate),
    Timestamp(NaiveDateTime),
    Time(NaiveTime),
    Interval(Interval),
    Uuid(u128),
    Decimal(Decimal),
}

impl From<Value> for Option<HashKey> {
    fn from(value: Value) -> Self {
        use Value::*;

        match value {
            Bool(v) => Some(HashKey::Bool(v)),
            I8(v) => Some(HashKey::I8(v)),
            I32(v) => Some(HashKey::I32(v)),
            I64(v) => Some(HashKey::I64(v)),
            I128(v) => Some(HashKey::I128(v)),
            U8(v) => Some(HashKey::U8(v)),
            U32(v) => Some(HashKey::U32(v)),
            U64(v) => Some(HashKey::U64(v)),
            U128(v) => Some(HashKey::U128(v)),
            Str(v) => Some(HashKey::Str(v)),
            Date(v) => Some(HashKey::Date(v)),
            Timestamp(v) => Some(HashKey::Timestamp(v)),
            Time(v) => Some(HashKey::Time(v)),
            Interval(v) => Some(HashKey::Interval(v)),
            Uuid(v) => Some(HashKey::Uuid(v)),
            Decimal(v) => Some(HashKey::Decimal(v)),
            Null | F64(_) | Map(_) | List(_) => None,
        }
    }
}

impl TryFrom<Evaluated<'_>> for Option<HashKey> {
    type Error = Error;

    fn try_from(evaluated: Evaluated<'_>) -> Result<Self> {
        let value: Value = evaluated.try_into()?;

        Ok(Self::from(value))
    }
}

#[cfg(test)]
mod tests {
    use {
        super::HashKey,
        crate::{
            executor::evaluate::evaluate_stateless, parse_sql::parse_expr,
            translate::translate_expr,
        },
    };

    fn convert(sql: &str) -> Option<HashKey> {
        let parsed = parse_expr(sql).expect(sql);
        let expr = translate_expr(&parsed).expect(sql);

        evaluate_stateless(None, &expr)
            .expect(sql)
            .try_into()
            .expect(sql)
    }

    #[test]
    fn value_to_hash_key() {
        assert!(convert("True").is_some());
        assert!(convert("CAST(11 AS INT(8))").is_some());
        assert!(convert("2048").is_some());
        assert!(convert(r#""Hello World""#).is_some());
        assert!(convert(r#"DATE "2022-03-03""#).is_some());
        assert!(convert(r#"TIME "12:30:00""#).is_some());
        assert!(convert(r#"TIMESTAMP "2022-03-03 12:30:00Z""#).is_some());
        assert!(convert(r#"INTERVAL "1" DAY"#).is_some());
        assert!(convert("GENERATE_UUID()").is_some());
        assert!(convert("NULL").is_none());
        assert!(convert("12.03").is_none());
    }
}
