use {
    crate::{
        data::{Interval, Value},
        result::{Error, Result},
    },
    chrono::{Datelike, NaiveDate, NaiveDateTime, NaiveTime, Timelike},
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
    I16(i16),
    I32(i32),
    I64(i64),
    I128(i128),
    U8(u8),
    Bool(bool),
    Str(String),
    Bytea(Vec<u8>),
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
            (Key::I16(l), Key::I16(r)) => Some(l.cmp(r)),
            (Key::I32(l), Key::I32(r)) => Some(l.cmp(r)),
            (Key::I64(l), Key::I64(r)) => Some(l.cmp(r)),
            (Key::Bool(l), Key::Bool(r)) => Some(l.cmp(r)),
            (Key::Str(l), Key::Str(r)) => Some(l.cmp(r)),
            (Key::Bytea(l), Key::Bytea(r)) => Some(l.cmp(r)),
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
            I16(v) => Ok(Key::I16(v)),
            I32(v) => Ok(Key::I32(v)),
            I64(v) => Ok(Key::I64(v)),
            I128(v) => Ok(Key::I128(v)),
            U8(v) => Ok(Key::U8(v)),
            Str(v) => Ok(Key::Str(v)),
            Bytea(v) => Ok(Key::Bytea(v)),
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

const VALUE: u8 = 0;
const NONE: u8 = 1;

impl Key {
    /// Key to Big-Endian for comparison purpose
    pub fn to_cmp_be_bytes(&self) -> Vec<u8> {
        match self {
            Key::Bool(v) => {
                if *v {
                    vec![VALUE, 1]
                } else {
                    vec![VALUE, 0]
                }
            }
            Key::I8(v) => {
                let sign = if *v >= 0 { 1 } else { 0 };

                [VALUE, sign]
                    .iter()
                    .chain(v.to_be_bytes().iter())
                    .copied()
                    .collect::<Vec<_>>()
            }
            Key::I16(v) => {
                let sign = if *v >= 0 { 1 } else { 0 };

                [VALUE, sign]
                    .iter()
                    .chain(v.to_be_bytes().iter())
                    .copied()
                    .collect::<Vec<_>>()
            }
            Key::I32(v) => {
                let sign = if *v >= 0 { 1 } else { 0 };

                [VALUE, sign]
                    .iter()
                    .chain(v.to_be_bytes().iter())
                    .copied()
                    .collect::<Vec<_>>()
            }
            Key::I64(v) => {
                let sign = if *v >= 0 { 1 } else { 0 };

                [VALUE, sign]
                    .iter()
                    .chain(v.to_be_bytes().iter())
                    .copied()
                    .collect::<Vec<_>>()
            }
            Key::I128(v) => {
                let sign = if *v >= 0 { 1 } else { 0 };

                [VALUE, sign]
                    .iter()
                    .chain(v.to_be_bytes().iter())
                    .copied()
                    .collect::<Vec<_>>()
            }
            Key::U8(v) => [VALUE, 1]
                .iter()
                .chain(v.to_be_bytes().iter())
                .copied()
                .collect::<Vec<_>>(),
            Key::Str(v) => [VALUE]
                .iter()
                .chain(v.as_bytes().iter())
                .copied()
                .collect::<Vec<_>>(),
            Key::Bytea(v) => v.to_vec(),
            Key::Date(date) => [VALUE]
                .iter()
                .chain(date.num_days_from_ce().to_be_bytes().iter())
                .copied()
                .collect::<Vec<_>>(),
            Key::Time(time) => {
                let secs = time.num_seconds_from_midnight();
                let frac = time.nanosecond();

                [VALUE]
                    .iter()
                    .chain(secs.to_be_bytes().iter())
                    .chain(frac.to_be_bytes().iter())
                    .copied()
                    .collect::<Vec<_>>()
            }
            Key::Timestamp(datetime) => {
                let date = datetime.num_days_from_ce();
                let secs = datetime.num_seconds_from_midnight();
                let frac = datetime.nanosecond();

                [VALUE]
                    .iter()
                    .chain(date.to_be_bytes().iter())
                    .chain(secs.to_be_bytes().iter())
                    .chain(frac.to_be_bytes().iter())
                    .copied()
                    .collect::<Vec<_>>()
            }
            Key::Interval(interval) => {
                let (month, microsec) = match interval {
                    Interval::Month(month) => (*month, 0),
                    Interval::Microsecond(microsec) => (0, *microsec),
                };

                [VALUE]
                    .iter()
                    .chain(month.to_be_bytes().iter())
                    .chain(microsec.to_be_bytes().iter())
                    .copied()
                    .collect::<Vec<_>>()
            }
            Key::Uuid(v) => [VALUE]
                .iter()
                .chain(v.to_be_bytes().iter())
                .copied()
                .collect::<Vec<_>>(),
            Key::Decimal(_) => {
                todo!();
            }
            Key::None => vec![NONE],
        }
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
        std::{cmp::Ordering, collections::HashMap},
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
        assert_eq!(convert("CAST(11 AS INT(16))"), Ok(Key::I16(11)));
        assert_eq!(convert("CAST(11 AS INT(32))"), Ok(Key::I32(11)));
        assert_eq!(convert("2048"), Ok(Key::I64(2048)));
        assert_eq!(convert("CAST(UNSIGNED INT(32)"), Ok(Key::U8(255)));
        assert_eq!(
            convert(r#""Hello World""#),
            Ok(Key::Str("Hello World".to_owned()))
        );
        assert_eq!(
            convert("X'1234'"),
            Ok(Key::Bytea(hex::decode("1234").unwrap())),
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

    fn cmp(ls: &[u8], rs: &[u8]) -> Ordering {
        for (l, r) in ls.iter().zip(rs.iter()) {
            match l.cmp(r) {
                Ordering::Equal => continue,
                ordering => return ordering,
            }
        }

        let size_l = ls.len();
        let size_r = rs.len();

        size_l.cmp(&size_r)
    }

    #[test]
    fn cmp_big_endian() {
        use {
            crate::data::{Interval as I, Key::*},
            chrono::{NaiveDate, NaiveTime},
        };

        let null = None.to_cmp_be_bytes();

        let n1 = Bool(true).to_cmp_be_bytes();
        let n2 = Bool(false).to_cmp_be_bytes();

        assert_eq!(cmp(&n2, &n2), Ordering::Equal);
        assert_eq!(cmp(&n1, &n2), Ordering::Greater);
        assert_eq!(cmp(&n2, &n1), Ordering::Less);
        assert_eq!(cmp(&n1, &null), Ordering::Less);

        let n1 = I8(-100).to_cmp_be_bytes();
        let n2 = I8(-10).to_cmp_be_bytes();
        let n3 = I8(0).to_cmp_be_bytes();
        let n4 = I8(3).to_cmp_be_bytes();
        let n5 = I8(20).to_cmp_be_bytes();
        let n6 = I8(100).to_cmp_be_bytes();

        assert_eq!(cmp(&n1, &n2), Ordering::Less);
        assert_eq!(cmp(&n3, &n2), Ordering::Greater);
        assert_eq!(cmp(&n1, &n6), Ordering::Less);
        assert_eq!(cmp(&n5, &n5), Ordering::Equal);
        assert_eq!(cmp(&n4, &n5), Ordering::Less);
        assert_eq!(cmp(&n6, &n4), Ordering::Greater);
        assert_eq!(cmp(&n4, &null), Ordering::Less);

        let n1 = I16(-100).to_cmp_be_bytes();
        let n2 = I16(-10).to_cmp_be_bytes();
        let n3 = I16(0).to_cmp_be_bytes();
        let n4 = I16(3).to_cmp_be_bytes();
        let n5 = I16(20).to_cmp_be_bytes();
        let n6 = I16(100).to_cmp_be_bytes();

        assert_eq!(cmp(&n1, &n2), Ordering::Less);
        assert_eq!(cmp(&n3, &n2), Ordering::Greater);
        assert_eq!(cmp(&n1, &n6), Ordering::Less);
        assert_eq!(cmp(&n5, &n5), Ordering::Equal);
        assert_eq!(cmp(&n4, &n5), Ordering::Less);
        assert_eq!(cmp(&n6, &n4), Ordering::Greater);
        assert_eq!(cmp(&n4, &null), Ordering::Less);

        let n1 = I32(-100).to_cmp_be_bytes();
        let n2 = I32(-10).to_cmp_be_bytes();
        let n3 = I32(0).to_cmp_be_bytes();
        let n4 = I32(3).to_cmp_be_bytes();
        let n5 = I32(20).to_cmp_be_bytes();
        let n6 = I32(100).to_cmp_be_bytes();

        assert_eq!(cmp(&n1, &n2), Ordering::Less);
        assert_eq!(cmp(&n3, &n2), Ordering::Greater);
        assert_eq!(cmp(&n1, &n6), Ordering::Less);
        assert_eq!(cmp(&n5, &n5), Ordering::Equal);
        assert_eq!(cmp(&n4, &n5), Ordering::Less);
        assert_eq!(cmp(&n6, &n4), Ordering::Greater);
        assert_eq!(cmp(&n4, &null), Ordering::Less);

        let n1 = I64(-123).to_cmp_be_bytes();
        let n2 = I64(-11).to_cmp_be_bytes();
        let n3 = I64(0).to_cmp_be_bytes();
        let n4 = I64(3).to_cmp_be_bytes();
        let n5 = I64(20).to_cmp_be_bytes();
        let n6 = I64(100).to_cmp_be_bytes();

        assert_eq!(cmp(&n1, &n2), Ordering::Less);
        assert_eq!(cmp(&n3, &n2), Ordering::Greater);
        assert_eq!(cmp(&n1, &n6), Ordering::Less);
        assert_eq!(cmp(&n5, &n5), Ordering::Equal);
        assert_eq!(cmp(&n4, &n5), Ordering::Less);
        assert_eq!(cmp(&n6, &n4), Ordering::Greater);
        assert_eq!(cmp(&n4, &null), Ordering::Less);

        let n1 = I128(-123).to_cmp_be_bytes();
        let n2 = I128(-11).to_cmp_be_bytes();
        let n3 = I128(0).to_cmp_be_bytes();
        let n4 = I128(3).to_cmp_be_bytes();
        let n5 = I128(20).to_cmp_be_bytes();
        let n6 = I128(100).to_cmp_be_bytes();

        assert_eq!(cmp(&n1, &n2), Ordering::Less);
        assert_eq!(cmp(&n3, &n2), Ordering::Greater);
        assert_eq!(cmp(&n1, &n6), Ordering::Less);
        assert_eq!(cmp(&n5, &n5), Ordering::Equal);
        assert_eq!(cmp(&n4, &n5), Ordering::Less);
        assert_eq!(cmp(&n6, &n4), Ordering::Greater);
        assert_eq!(cmp(&n4, &null), Ordering::Less);

        let n1 = Str("a".to_owned()).to_cmp_be_bytes();
        let n2 = Str("ab".to_owned()).to_cmp_be_bytes();
        let n3 = Str("aaa".to_owned()).to_cmp_be_bytes();
        let n4 = Str("aaz".to_owned()).to_cmp_be_bytes();
        let n5 = Str("c".to_owned()).to_cmp_be_bytes();

        assert_eq!(cmp(&n2, &n2), Ordering::Equal);
        assert_eq!(cmp(&n1, &n2), Ordering::Less);
        assert_eq!(cmp(&n3, &n1), Ordering::Greater);
        assert_eq!(cmp(&n2, &n3), Ordering::Greater);
        assert_eq!(cmp(&n3, &n4), Ordering::Less);
        assert_eq!(cmp(&n5, &n4), Ordering::Greater);
        assert_eq!(cmp(&n1, &null), Ordering::Less);

        let n1 = Bytea(n1).to_cmp_be_bytes();
        let n2 = Bytea(n2).to_cmp_be_bytes();
        let n3 = Bytea(n3).to_cmp_be_bytes();
        let n4 = Bytea(n4).to_cmp_be_bytes();
        let n5 = Bytea(n5).to_cmp_be_bytes();

        assert_eq!(cmp(&n2, &n2), Ordering::Equal);
        assert_eq!(cmp(&n1, &n2), Ordering::Less);
        assert_eq!(cmp(&n3, &n1), Ordering::Greater);
        assert_eq!(cmp(&n2, &n3), Ordering::Greater);
        assert_eq!(cmp(&n3, &n4), Ordering::Less);
        assert_eq!(cmp(&n5, &n4), Ordering::Greater);
        assert_eq!(cmp(&n1, &null), Ordering::Less);

        let n1 = Date(NaiveDate::from_ymd(2021, 1, 1)).to_cmp_be_bytes();
        let n2 = Date(NaiveDate::from_ymd(1989, 3, 20)).to_cmp_be_bytes();

        assert_eq!(cmp(&n2, &n2), Ordering::Equal);
        assert_eq!(cmp(&n1, &n2), Ordering::Greater);
        assert_eq!(cmp(&n1, &null), Ordering::Less);

        let n1 = Time(NaiveTime::from_hms_milli(20, 1, 9, 100)).to_cmp_be_bytes();
        let n2 = Time(NaiveTime::from_hms_milli(3, 10, 30, 0)).to_cmp_be_bytes();

        assert_eq!(cmp(&n2, &n2), Ordering::Equal);
        assert_eq!(cmp(&n1, &n2), Ordering::Greater);
        assert_eq!(cmp(&n1, &null), Ordering::Less);

        let n1 =
            Timestamp(NaiveDate::from_ymd(2021, 1, 1).and_hms_milli(1, 2, 3, 0)).to_cmp_be_bytes();
        let n2 = Timestamp(NaiveDate::from_ymd(1989, 3, 20).and_hms_milli(10, 0, 0, 1000))
            .to_cmp_be_bytes();

        assert_eq!(cmp(&n2, &n2), Ordering::Equal);
        assert_eq!(cmp(&n1, &n2), Ordering::Greater);
        assert_eq!(cmp(&n1, &null), Ordering::Less);

        let n1 = Interval(I::Month(30)).to_cmp_be_bytes();
        let n2 = Interval(I::Month(2)).to_cmp_be_bytes();
        let n3 = Interval(I::Microsecond(1000)).to_cmp_be_bytes();
        let n4 = Interval(I::Microsecond(30)).to_cmp_be_bytes();

        assert_eq!(cmp(&n1, &n1), Ordering::Equal);
        assert_eq!(cmp(&n2, &n1), Ordering::Less);
        assert_eq!(cmp(&n2, &n3), Ordering::Greater);
        assert_eq!(cmp(&n3, &n4), Ordering::Greater);
        assert_eq!(cmp(&n1, &null), Ordering::Less);

        let n1 = Uuid(100).to_cmp_be_bytes();
        let n2 = Uuid(101).to_cmp_be_bytes();

        assert_eq!(cmp(&n1, &n1), Ordering::Equal);
        assert_eq!(cmp(&n1, &n2), Ordering::Less);
        assert_eq!(cmp(&n2, &n1), Ordering::Greater);
        assert_eq!(cmp(&n1, &null), Ordering::Less);
    }
}
