use {
    crate::{
        data::{Interval, Value},
        result::{Error, Result},
    },
    chrono::{Datelike, NaiveDate, NaiveDateTime, NaiveTime, Timelike},
    ordered_float::OrderedFloat,
    rust_decimal::Decimal,
    serde::{Deserialize, Serialize},
    std::{cmp::Ordering, fmt::Debug, net::IpAddr},
    thiserror::Error as ThisError,
};

#[derive(ThisError, Debug, PartialEq, Eq, Serialize)]
pub enum KeyError {
    #[error("FLOAT data type cannot be converted to Big-Endian bytes for comparison")]
    FloatToCmpBigEndianNotSupported,

    #[error("MAP data type cannot be used as Key")]
    MapTypeKeyNotSupported,

    #[error("LIST data type cannot be used as Key")]
    ListTypeKeyNotSupported,

    #[error("POINT data type cannot be used as Key")]
    PointTypeKeyNotSupported,
}

#[derive(PartialEq, Eq, Hash, Clone, Debug, Serialize, Deserialize)]
pub enum Key {
    I8(i8),
    I16(i16),
    I32(i32),
    I64(i64),
    I128(i128),
    U8(u8),
    U16(u16),
    U32(u32),
    U64(u64),
    U128(u128),
    F32(OrderedFloat<f32>),
    F64(OrderedFloat<f64>),
    Decimal(Decimal),
    Bool(bool),
    Str(String),
    Bytea(Vec<u8>),
    Date(NaiveDate),
    Timestamp(NaiveDateTime),
    Time(NaiveTime),
    Interval(Interval),
    Uuid(u128),
    Inet(IpAddr),
    None,
}

impl Ord for Key {
    fn cmp(&self, other: &Self) -> Ordering {
        match (self, other) {
            (Key::I8(l), Key::I8(r)) => l.cmp(r),
            (Key::I16(l), Key::I16(r)) => l.cmp(r),
            (Key::I32(l), Key::I32(r)) => l.cmp(r),
            (Key::I64(l), Key::I64(r)) => l.cmp(r),
            (Key::I128(l), Key::I128(r)) => l.cmp(r),
            (Key::U8(l), Key::U8(r)) => l.cmp(r),
            (Key::U16(l), Key::U16(r)) => l.cmp(r),
            (Key::U32(l), Key::U32(r)) => l.cmp(r),
            (Key::U64(l), Key::U64(r)) => l.cmp(r),
            (Key::U128(l), Key::U128(r)) => l.cmp(r),
            (Key::F32(l), Key::F32(r)) => l.total_cmp(&r.0),
            (Key::F64(l), Key::F64(r)) => l.total_cmp(&r.0),
            (Key::Decimal(l), Key::Decimal(r)) => l.cmp(r),
            (Key::Bool(l), Key::Bool(r)) => l.cmp(r),
            (Key::Str(l), Key::Str(r)) => l.cmp(r),
            (Key::Bytea(l), Key::Bytea(r)) => l.cmp(r),
            (Key::Date(l), Key::Date(r)) => l.cmp(r),
            (Key::Timestamp(l), Key::Timestamp(r)) => l.cmp(r),
            (Key::Time(l), Key::Time(r)) => l.cmp(r),
            (Key::Interval(l), Key::Interval(r)) => l.cmp(r),
            (Key::Uuid(l), Key::Uuid(r)) => l.cmp(r),
            (Key::Inet(l), Key::Inet(r)) => l.cmp(r),
            (Key::None, Key::None) => Ordering::Equal,
            (Key::None, _) => Ordering::Greater,
            (_, Key::None) => Ordering::Less,

            (left, right) => {
                if left.to_order() <= right.to_order() {
                    Ordering::Greater
                } else {
                    Ordering::Less
                }
            }
        }
    }
}

impl PartialOrd for Key {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
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
            U16(v) => Ok(Key::U16(v)),
            U32(v) => Ok(Key::U32(v)),
            U64(v) => Ok(Key::U64(v)),
            U128(v) => Ok(Key::U128(v)),
            F32(v) => Ok(Key::F32(OrderedFloat(v))),
            F64(v) => Ok(Key::F64(OrderedFloat(v))),
            Decimal(v) => Ok(Key::Decimal(v)),
            Str(v) => Ok(Key::Str(v)),
            Bytea(v) => Ok(Key::Bytea(v)),
            Inet(v) => Ok(Key::Inet(v)),
            Date(v) => Ok(Key::Date(v)),
            Timestamp(v) => Ok(Key::Timestamp(v)),
            Time(v) => Ok(Key::Time(v)),
            Interval(v) => Ok(Key::Interval(v)),
            Uuid(v) => Ok(Key::Uuid(v)),
            Null => Ok(Key::None),
            Map(_) => Err(KeyError::MapTypeKeyNotSupported.into()),
            List(_) => Err(KeyError::ListTypeKeyNotSupported.into()),
            Point(_) => Err(KeyError::PointTypeKeyNotSupported.into()),
        }
    }
}

impl TryFrom<&Value> for Key {
    type Error = Error;

    fn try_from(value: &Value) -> Result<Self> {
        value.clone().try_into()
    }
}

impl From<Key> for Value {
    fn from(key: Key) -> Self {
        match key {
            Key::Bool(v) => Value::Bool(v),
            Key::I8(v) => Value::I8(v),
            Key::I16(v) => Value::I16(v),
            Key::I32(v) => Value::I32(v),
            Key::I64(v) => Value::I64(v),
            Key::I128(v) => Value::I128(v),
            Key::U8(v) => Value::U8(v),
            Key::U16(v) => Value::U16(v),
            Key::U32(v) => Value::U32(v),
            Key::U64(v) => Value::U64(v),
            Key::U128(v) => Value::U128(v),
            Key::F32(v) => Value::F32(v.0),
            Key::F64(v) => Value::F64(v.0),
            Key::Decimal(v) => Value::Decimal(v),
            Key::Str(v) => Value::Str(v),
            Key::Bytea(v) => Value::Bytea(v),
            Key::Inet(v) => Value::Inet(v),
            Key::Date(v) => Value::Date(v),
            Key::Timestamp(v) => Value::Timestamp(v),
            Key::Time(v) => Value::Time(v),
            Key::Interval(v) => Value::Interval(v),
            Key::Uuid(v) => Value::Uuid(v),
            Key::None => Value::Null,
        }
    }
}

const VALUE: u8 = 0;
const NONE: u8 = 1;

impl Key {
    /// Key to Big-Endian for comparison purpose
    pub fn to_cmp_be_bytes(&self) -> Result<Vec<u8>> {
        Ok(match self {
            Key::Bool(v) => {
                if *v {
                    vec![VALUE, 1]
                } else {
                    vec![VALUE, 0]
                }
            }
            Key::I8(v) => {
                let sign = u8::from(*v >= 0);

                [VALUE, sign]
                    .iter()
                    .chain(v.to_be_bytes().iter())
                    .copied()
                    .collect::<Vec<_>>()
            }
            Key::I16(v) => {
                let sign = u8::from(*v >= 0);

                [VALUE, sign]
                    .iter()
                    .chain(v.to_be_bytes().iter())
                    .copied()
                    .collect::<Vec<_>>()
            }
            Key::I32(v) => {
                let sign = u8::from(*v >= 0);

                [VALUE, sign]
                    .iter()
                    .chain(v.to_be_bytes().iter())
                    .copied()
                    .collect::<Vec<_>>()
            }
            Key::I64(v) => {
                let sign = u8::from(*v >= 0);

                [VALUE, sign]
                    .iter()
                    .chain(v.to_be_bytes().iter())
                    .copied()
                    .collect::<Vec<_>>()
            }
            Key::I128(v) => {
                let sign = u8::from(*v >= 0);

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
            Key::U16(v) => [VALUE, 1]
                .iter()
                .chain(v.to_be_bytes().iter())
                .copied()
                .collect::<Vec<_>>(),
            Key::U32(v) => [VALUE, 1]
                .iter()
                .chain(v.to_be_bytes().iter())
                .copied()
                .collect::<Vec<_>>(),
            Key::U64(v) => [VALUE, 1]
                .iter()
                .chain(v.to_be_bytes().iter())
                .copied()
                .collect::<Vec<_>>(),
            Key::U128(v) => [VALUE, 1]
                .iter()
                .chain(v.to_be_bytes().iter())
                .copied()
                .collect::<Vec<_>>(),
            Key::F32(_) | Key::F64(_) => {
                return Err(KeyError::FloatToCmpBigEndianNotSupported.into());
            }
            Key::Decimal(v) => {
                let sign = u8::from(v.is_sign_positive());
                let convert = |v: Decimal| {
                    let v = v.unpack();
                    let v = v.lo as i128 + ((v.mid as i128) << 32) + ((v.hi as i128) << 64);

                    if sign == 0 {
                        -v
                    } else {
                        v
                    }
                };

                [VALUE, sign]
                    .into_iter()
                    .chain(convert(v.trunc()).to_be_bytes())
                    .chain(convert(v.fract()).to_be_bytes())
                    .collect::<Vec<_>>()
            }
            Key::Str(v) => [VALUE]
                .iter()
                .chain(v.as_bytes().iter())
                .copied()
                .collect::<Vec<_>>(),
            Key::Bytea(v) => v.to_vec(),
            Key::Inet(v) => match v {
                IpAddr::V4(v) => v.octets().to_vec(),
                IpAddr::V6(v) => v.octets().to_vec(),
            },
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
            Key::None => vec![NONE],
        })
    }

    fn to_order(&self) -> u8 {
        match self {
            Key::I8(_) => 1,
            Key::I16(_) => 2,
            Key::I32(_) => 3,
            Key::I64(_) => 4,
            Key::I128(_) => 5,
            Key::U8(_) => 6,
            Key::U16(_) => 7,
            Key::U32(_) => 8,
            Key::U64(_) => 9,
            Key::U128(_) => 10,
            Key::F32(_) => 11,
            Key::F64(_) => 12,
            Key::Decimal(_) => 13,
            Key::Bool(_) => 14,
            Key::Str(_) => 15,
            Key::Bytea(_) => 16,
            Key::Date(_) => 17,
            Key::Timestamp(_) => 18,
            Key::Time(_) => 19,
            Key::Interval(_) => 20,
            Key::Uuid(_) => 21,
            Key::Inet(_) => 22,
            Key::None => 23,
        }
    }
}

#[cfg(test)]
mod tests {
    use {
        crate::{
            data::{Interval, Key, KeyError, Point, Value},
            executor::evaluate_stateless,
            parse_sql::parse_expr,
            result::Result,
            translate::translate_expr,
        },
        chrono::{DateTime, NaiveDate, NaiveTime},
        futures::executor::block_on,
        rust_decimal::Decimal,
        std::{cmp::Ordering, collections::HashMap, net::IpAddr, str::FromStr},
    };

    fn convert(sql: &str) -> Result<Key> {
        let parsed = parse_expr(sql).expect(sql);
        let expr = translate_expr(&parsed).expect(sql);

        block_on(evaluate_stateless(None, &expr))
            .expect(sql)
            .try_into()
    }

    #[test]
    fn evaluated_to_key() {
        // Some
        assert_eq!(convert("True"), Ok(Key::Bool(true)));
        assert_eq!(convert("CAST(11 AS INT8)"), Ok(Key::I8(11)));
        assert_eq!(convert("CAST(11 AS INT16)"), Ok(Key::I16(11)));
        assert_eq!(convert("CAST(11 AS INT32)"), Ok(Key::I32(11)));
        assert_eq!(convert("2048"), Ok(Key::I64(2048)));
        assert_eq!(convert("CAST(1024 AS INT128)"), Ok(Key::I128(1024)));
        assert_eq!(convert("CAST(11 AS UINT8)"), Ok(Key::U8(11)));
        assert_eq!(convert("CAST(11 AS UINT16)"), Ok(Key::U16(11)));
        assert_eq!(convert("CAST(11 AS UINT32)"), Ok(Key::U32(11)));
        assert_eq!(convert("CAST(11 AS UINT64)"), Ok(Key::U64(11)));
        assert_eq!(convert("CAST(11 AS UINT128)"), Ok(Key::U128(11)));
        assert!(matches!(convert("CAST(12.03 AS FLOAT32)"), Ok(Key::F32(_))));
        assert!(matches!(convert("12.03"), Ok(Key::F64(_))));

        assert_eq!(
            convert("CAST(123.45 AS DECIMAL)"),
            Ok(Key::Decimal(Decimal::from_str("123.45").unwrap()))
        );
        assert_eq!(
            convert("CAST(0 AS INET)"),
            Ok(Key::Inet(IpAddr::from_str("0.0.0.0").unwrap()))
        );

        assert_eq!(
            convert("'Hello World'"),
            Ok(Key::Str("Hello World".to_owned()))
        );
        assert_eq!(
            convert("X'1234'"),
            Ok(Key::Bytea(hex::decode("1234").unwrap())),
        );
        assert!(matches!(convert("DATE '2022-03-03'"), Ok(Key::Date(_))));
        assert!(matches!(convert("TIME '12:30:00'"), Ok(Key::Time(_))));
        assert!(matches!(
            convert("TIMESTAMP '2022-03-03 12:30:00Z'"),
            Ok(Key::Timestamp(_))
        ));
        assert!(matches!(convert("INTERVAL '1' DAY"), Ok(Key::Interval(_))));
        assert!(matches!(convert("GENERATE_UUID()"), Ok(Key::Uuid(_))));

        // None
        assert_eq!(convert("NULL"), Ok(Key::None));

        // Error
        assert_eq!(
            Key::try_from(Value::Map(HashMap::default())),
            Err(KeyError::MapTypeKeyNotSupported.into())
        );
        assert_eq!(
            Key::try_from(Value::List(Vec::default())),
            Err(KeyError::ListTypeKeyNotSupported.into())
        );
        assert_eq!(
            convert("SUBSTR('BEEF', 2, 3)"),
            Ok(Key::Str("EEF".to_owned()))
        );
        assert_eq!(convert("POSITION('PORK' IN 'MEAT')"), Ok(Key::I64(0)));
        assert_eq!(convert("FIND_IDX('Calzone', 'zone')"), Ok(Key::I64(4)));
        assert_eq!(
            convert("EXTRACT(SECOND FROM INTERVAL '8' SECOND)"),
            Ok(Key::I64(8))
        );
        assert_eq!(
            Key::try_from(Value::Point(Point::new(1.0, 2.0))),
            Err(KeyError::PointTypeKeyNotSupported.into())
        );
    }

    #[test]
    fn cmp() {
        use {
            std::{net::IpAddr, str::FromStr},
            uuid::Uuid,
        };

        let dec = |v| Decimal::from_str(v).unwrap();
        let date = |y, m, d| NaiveDate::from_ymd_opt(y, m, d).unwrap();
        let timestamp = |v| DateTime::from_timestamp_millis(v).unwrap().naive_utc();
        let time = |h, m, s| NaiveTime::from_hms_milli_opt(h, m, s, 0).unwrap();
        let uuid = |v| Uuid::parse_str(v).unwrap().as_u128();
        let inet = |v| IpAddr::from_str(v).unwrap();

        assert!(Key::I8(10) > Key::I8(3));
        assert!(Key::I8(1) > Key::I16(1));

        assert!(Key::I16(10) > Key::I16(3));
        assert!(Key::I16(1) > Key::I32(1));

        assert!(Key::I32(10) > Key::I32(3));
        assert!(Key::I32(1) > Key::I64(1));

        assert!(Key::I64(10) > Key::I64(3));
        assert!(Key::I64(1) > Key::I128(1));

        assert!(Key::I128(10) > Key::I128(3));
        assert!(Key::I128(1) > Key::U8(1));

        assert!(Key::U8(10) > Key::U8(3));
        assert!(Key::U8(1) > Key::U16(1));

        assert!(Key::U16(10) > Key::U16(3));
        assert!(Key::U16(1) > Key::Decimal(dec("1")));

        assert!(Key::U32(10) > Key::U32(3));
        assert!(Key::U32(1) > Key::Decimal(dec("1")));

        assert!(Key::U64(10) > Key::U64(3));
        assert!(Key::U64(1) > Key::Decimal(dec("1")));

        assert!(Key::U128(10) > Key::U128(3));
        assert!(Key::U128(1) > Key::Decimal(dec("1")));

        assert!(Key::F32(10.0_f32.into()) > Key::F32(3.0_f32.into()));
        assert!(Key::F32(1.0_f32.into()) > Key::F64(1.0.into()));

        assert!(Key::F64(10.0.into()) > Key::F64(3.0.into()));
        assert!(Key::F64(1.0.into()) > Key::Decimal(dec("1")));

        assert!(Key::Decimal(dec("123.45")) > Key::Decimal(dec("0.11")));
        assert!(Key::Decimal(dec("1")) > Key::Bool(true));

        assert!(Key::Bool(true) > Key::Bool(false));
        assert!(Key::Bool(true) > Key::Str("zzz".to_owned()));

        assert!(Key::Str("def".to_owned()) > Key::Str("abcd".to_owned()));
        assert!(Key::Str("hi".to_owned()) > Key::Bytea(vec![101]));

        assert!(Key::Bytea(vec![100]) > Key::Bytea(vec![3]));
        assert!(Key::Bytea(vec![0]) > Key::Date(date(2023, 1, 1)));

        assert!(Key::Date(date(2023, 3, 1)) > Key::Date(date(1999, 6, 11)));
        assert!(Key::Date(date(2022, 6, 1)) > Key::Timestamp(timestamp(1669000003)));

        assert!(Key::Timestamp(timestamp(1662921288)) > Key::Timestamp(timestamp(1661000000)));
        assert!(Key::Timestamp(timestamp(1668919293)) > Key::Time(time(23, 1, 59)));

        assert!(Key::Time(time(20, 1, 9)) > Key::Time(time(10, 0, 3)));
        assert!(Key::Time(time(1, 2, 3)) > Key::Interval(Interval::Month(12)));

        assert!(Key::Interval(Interval::Month(3)) > Key::Interval(Interval::Month(1)));
        assert!(
            Key::Interval(Interval::microseconds(1))
                > Key::Uuid(uuid("dc98e386-a4d0-45c7-babe-b4238de4b139"))
        );

        assert!(
            Key::Uuid(uuid("dc98e386-a4d0-45c7-babe-b4238de4b139"))
                > Key::Uuid(uuid("550e8400-e29b-41d4-a716-446655440000"))
        );
        assert!(
            Key::Uuid(uuid("dc98e386-a4d0-45c7-babe-b4238de4b139")) > Key::Inet(inet("127.0.0.1"))
        );

        assert!(Key::Inet(inet("127.0.0.1")) > Key::Inet(inet("0.0.0.1")));
        assert!(Key::Inet(inet("192.168.1.19")) < Key::None);

        assert_eq!(Key::None.partial_cmp(&Key::None), Some(Ordering::Equal));
        assert!(Key::None > Key::I8(100));
    }

    #[test]
    fn cmp_big_endian() {
        use crate::data::{Interval as I, Key::*};

        fn cmp(ls: &Result<Vec<u8>>, rs: &Result<Vec<u8>>) -> Ordering {
            let ls = ls.as_ref().unwrap();
            let rs = rs.as_ref().unwrap();

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

        let n1 = U8(0).to_cmp_be_bytes();
        let n2 = U8(3).to_cmp_be_bytes();
        let n3 = U8(20).to_cmp_be_bytes();
        let n4 = U8(20).to_cmp_be_bytes();
        assert_eq!(cmp(&n1, &n2), Ordering::Less);
        assert_eq!(cmp(&n3, &n2), Ordering::Greater);
        assert_eq!(cmp(&n1, &n4), Ordering::Less);
        assert_eq!(cmp(&n3, &n4), Ordering::Equal);

        let n1 = U16(0).to_cmp_be_bytes();
        let n2 = U16(3).to_cmp_be_bytes();
        let n3 = U16(20).to_cmp_be_bytes();
        let n4 = U16(20).to_cmp_be_bytes();
        assert_eq!(cmp(&n1, &n2), Ordering::Less);
        assert_eq!(cmp(&n3, &n2), Ordering::Greater);
        assert_eq!(cmp(&n1, &n4), Ordering::Less);
        assert_eq!(cmp(&n3, &n4), Ordering::Equal);

        let n1 = U32(0).to_cmp_be_bytes();
        let n2 = U32(3).to_cmp_be_bytes();
        let n3 = U32(20).to_cmp_be_bytes();
        let n4 = U32(20).to_cmp_be_bytes();
        assert_eq!(cmp(&n1, &n2), Ordering::Less);
        assert_eq!(cmp(&n3, &n2), Ordering::Greater);
        assert_eq!(cmp(&n1, &n4), Ordering::Less);
        assert_eq!(cmp(&n3, &n4), Ordering::Equal);

        let n1 = U64(0).to_cmp_be_bytes();
        let n2 = U64(3).to_cmp_be_bytes();
        let n3 = U64(20).to_cmp_be_bytes();
        let n4 = U64(20).to_cmp_be_bytes();
        assert_eq!(cmp(&n1, &n2), Ordering::Less);
        assert_eq!(cmp(&n3, &n2), Ordering::Greater);
        assert_eq!(cmp(&n1, &n4), Ordering::Less);
        assert_eq!(cmp(&n3, &n4), Ordering::Equal);

        let n1 = U128(0).to_cmp_be_bytes();
        let n2 = U128(3).to_cmp_be_bytes();
        let n3 = U128(20).to_cmp_be_bytes();
        let n4 = U128(20).to_cmp_be_bytes();
        assert_eq!(cmp(&n1, &n2), Ordering::Less);
        assert_eq!(cmp(&n3, &n2), Ordering::Greater);
        assert_eq!(cmp(&n1, &n4), Ordering::Less);
        assert_eq!(cmp(&n3, &n4), Ordering::Equal);

        let dec = |n| Decimal(rust_decimal::Decimal::from_str(n).unwrap());
        let n1 = dec("-1200.345678").to_cmp_be_bytes();
        let n2 = dec("-1.01").to_cmp_be_bytes();
        let n3 = dec("0").to_cmp_be_bytes();
        let n4 = dec("3.9").to_cmp_be_bytes();
        let n5 = dec("300.0").to_cmp_be_bytes();
        let n6 = dec("3000").to_cmp_be_bytes();
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

        let n1 = Bytea(n1.unwrap()).to_cmp_be_bytes();
        let n2 = Bytea(n2.unwrap()).to_cmp_be_bytes();
        let n3 = Bytea(n3.unwrap()).to_cmp_be_bytes();
        let n4 = Bytea(n4.unwrap()).to_cmp_be_bytes();
        let n5 = Bytea(n5.unwrap()).to_cmp_be_bytes();

        assert_eq!(cmp(&n2, &n2), Ordering::Equal);
        assert_eq!(cmp(&n1, &n2), Ordering::Less);
        assert_eq!(cmp(&n3, &n1), Ordering::Greater);
        assert_eq!(cmp(&n2, &n3), Ordering::Greater);
        assert_eq!(cmp(&n3, &n4), Ordering::Less);
        assert_eq!(cmp(&n5, &n4), Ordering::Greater);
        assert_eq!(cmp(&n1, &null), Ordering::Less);

        let n1 = Inet(IpAddr::from_str("192.168.0.1").unwrap()).to_cmp_be_bytes();
        let n2 = Inet(IpAddr::from_str("127.0.0.1").unwrap()).to_cmp_be_bytes();
        let n3 = Inet(IpAddr::from_str("10.0.0.1").unwrap()).to_cmp_be_bytes();
        let n4 = Inet(IpAddr::from_str("0.0.0.0").unwrap()).to_cmp_be_bytes();
        let n5 = Inet(IpAddr::from_str("0:0:0:0:0:0:0:1").unwrap()).to_cmp_be_bytes();
        let n6 = Inet(IpAddr::from_str("::1").unwrap()).to_cmp_be_bytes();

        assert_eq!(cmp(&n1, &n1), Ordering::Equal);
        assert_eq!(cmp(&n2, &n1), Ordering::Less);
        assert_eq!(cmp(&n2, &n3), Ordering::Greater);
        assert_eq!(cmp(&n3, &n4), Ordering::Greater);
        assert_eq!(cmp(&n1, &null), Ordering::Greater);
        assert_eq!(cmp(&n5, &n6), Ordering::Equal);

        let n1 = Date(NaiveDate::from_ymd_opt(2021, 1, 1).unwrap()).to_cmp_be_bytes();
        let n2 = Date(NaiveDate::from_ymd_opt(1989, 3, 20).unwrap()).to_cmp_be_bytes();

        assert_eq!(cmp(&n2, &n2), Ordering::Equal);
        assert_eq!(cmp(&n1, &n2), Ordering::Greater);
        assert_eq!(cmp(&n1, &null), Ordering::Less);

        let n1 = Time(NaiveTime::from_hms_milli_opt(20, 1, 9, 100).unwrap()).to_cmp_be_bytes();
        let n2 = Time(NaiveTime::from_hms_milli_opt(3, 10, 30, 0).unwrap()).to_cmp_be_bytes();

        assert_eq!(cmp(&n2, &n2), Ordering::Equal);
        assert_eq!(cmp(&n1, &n2), Ordering::Greater);
        assert_eq!(cmp(&n1, &null), Ordering::Less);

        let n1 = Timestamp(
            NaiveDate::from_ymd_opt(2021, 1, 1)
                .unwrap()
                .and_hms_milli_opt(1, 2, 3, 0)
                .unwrap(),
        )
        .to_cmp_be_bytes();
        let n2 = Timestamp(
            NaiveDate::from_ymd_opt(1989, 3, 20)
                .unwrap()
                .and_hms_milli_opt(10, 0, 0, 999)
                .unwrap(),
        )
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

        assert_eq!(
            F64(12.34.into()).to_cmp_be_bytes(),
            Err(KeyError::FloatToCmpBigEndianNotSupported.into())
        );
    }

    #[test]
    fn from_key_to_value() {
        use {crate::data::Interval as I, uuid::Uuid};

        assert_eq!(Value::from(Key::I8(2)), Value::I8(2));
        assert_eq!(Value::from(Key::I16(4)), Value::I16(4));
        assert_eq!(Value::from(Key::I32(8)), Value::I32(8));
        assert_eq!(Value::from(Key::I64(16)), Value::I64(16));
        assert_eq!(Value::from(Key::I128(32)), Value::I128(32));
        assert_eq!(Value::from(Key::U8(64)), Value::U8(64));
        assert_eq!(Value::from(Key::U16(128)), Value::U16(128));
        assert_eq!(Value::from(Key::U32(128)), Value::U32(128));
        assert_eq!(Value::from(Key::U64(128)), Value::U64(128));
        assert_eq!(Value::from(Key::U128(128)), Value::U128(128));
        assert_eq!(Value::from(Key::F32(1.0.into())), Value::F32(1.0_f32));
        assert_eq!(Value::from(Key::F64(1.0.into())), Value::F64(1.0));
        assert_eq!(
            Value::from(Key::Decimal(Decimal::from_str("123.45").unwrap())),
            Value::Decimal(Decimal::from_str("123.45").unwrap())
        );
        assert_eq!(Value::from(Key::Bool(true)), Value::Bool(true));
        assert_eq!(
            Value::from(Key::Str("abc".to_owned())),
            Value::Str("abc".to_owned())
        );
        assert_eq!(Value::from(Key::Bytea(vec![])), Value::Bytea(vec![]));
        assert_eq!(
            Value::from(Key::Inet(IpAddr::from_str("::1").unwrap())),
            Value::Inet(IpAddr::from_str("::1").unwrap())
        );
        assert_eq!(
            Value::from(Key::Date(NaiveDate::from_ymd_opt(2023, 1, 23).unwrap())),
            Value::Date(NaiveDate::from_ymd_opt(2023, 1, 23).unwrap())
        );
        assert_eq!(
            Value::from(Key::Timestamp(
                DateTime::from_timestamp_millis(1662921288)
                    .unwrap()
                    .naive_utc()
            )),
            Value::Timestamp(
                DateTime::from_timestamp_millis(1662921288)
                    .unwrap()
                    .naive_utc()
            )
        );
        assert_eq!(
            Value::from(Key::Time(
                NaiveTime::from_hms_milli_opt(20, 20, 1, 452).unwrap()
            )),
            Value::Time(NaiveTime::from_hms_milli_opt(20, 20, 1, 452).unwrap())
        );
        assert_eq!(
            Value::from(Key::Interval(I::Month(11))),
            Value::Interval(I::Month(11))
        );
        assert_eq!(
            Value::from(Key::Uuid(
                Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000")
                    .unwrap()
                    .as_u128()
            )),
            Value::Uuid(
                Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000")
                    .unwrap()
                    .as_u128()
            )
        );
        matches!(Value::from(Key::None), Value::Null);
    }
}
