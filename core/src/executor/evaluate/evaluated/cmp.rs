use {
    super::Evaluated,
    crate::data::{
        BigDecimalExt, Value,
        value::{parse_date, parse_time, parse_timestamp, parse_uuid},
    },
    bigdecimal::BigDecimal,
    std::{cmp::Ordering, net::IpAddr, str::FromStr},
};

impl<'a> Evaluated<'a> {
    pub fn evaluate_cmp(&self, other: &Evaluated<'a>) -> Option<Ordering> {
        match (self, other) {
            (Evaluated::Number(l), Evaluated::Number(r)) => Some(l.cmp(r)),
            (Evaluated::Text(l), Evaluated::Text(r)) => Some(l.cmp(r)),
            (Evaluated::Number(l), Evaluated::Value(r)) => {
                value_cmp_with_number(r, l).map(Ordering::reverse)
            }
            (Evaluated::Text(l), Evaluated::Value(r)) => {
                value_cmp_with_text(r, l).map(Ordering::reverse)
            }
            (Evaluated::Value(l), Evaluated::Number(r)) => value_cmp_with_number(l, r),
            (Evaluated::Value(l), Evaluated::Text(r)) => value_cmp_with_text(l, r),
            (Evaluated::Value(left), Evaluated::Value(right)) => left.evaluate_cmp(right),
            (Evaluated::Text(l), Evaluated::StrSlice { source, range }) => {
                Some(l.as_ref().cmp(&source[range.clone()]))
            }
            (Evaluated::StrSlice { source, range }, Evaluated::Text(r)) => {
                Some(source[range.clone()].cmp(r.as_ref()))
            }
            (Evaluated::Number(_), Evaluated::Text(_) | Evaluated::StrSlice { .. })
            | (Evaluated::Text(_) | Evaluated::StrSlice { .. }, Evaluated::Number(_)) => None,
            (Evaluated::Value(l), Evaluated::StrSlice { source, range }) => {
                value_cmp_with_text(l, &source[range.clone()])
            }
            (Evaluated::StrSlice { source, range }, Evaluated::Value(r)) => {
                value_cmp_with_text(r, &source[range.clone()]).map(Ordering::reverse)
            }
            (
                Evaluated::StrSlice {
                    source: a,
                    range: ar,
                },
                Evaluated::StrSlice {
                    source: b,
                    range: br,
                },
            ) => a[ar.clone()].partial_cmp(&b[br.clone()]),
        }
    }
}

fn value_cmp_with_number(value: &Value, number: &BigDecimal) -> Option<Ordering> {
    match value {
        Value::I8(l) => l.partial_cmp(&number.to_i8()?),
        Value::I16(l) => l.partial_cmp(&number.to_i16()?),
        Value::I32(l) => l.partial_cmp(&number.to_i32()?),
        Value::I64(l) => l.partial_cmp(&number.to_i64()?),
        Value::I128(l) => l.partial_cmp(&number.to_i128()?),
        Value::U8(l) => l.partial_cmp(&number.to_u8()?),
        Value::U16(l) => l.partial_cmp(&number.to_u16()?),
        Value::U32(l) => l.partial_cmp(&number.to_u32()?),
        Value::U64(l) => l.partial_cmp(&number.to_u64()?),
        Value::U128(l) => l.partial_cmp(&number.to_u128()?),
        Value::F32(l) => l.partial_cmp(&number.to_f32()?),
        Value::F64(l) => l.partial_cmp(&number.to_f64()?),
        Value::Decimal(l) => {
            BigDecimal::new(l.mantissa().into(), i64::from(l.scale())).partial_cmp(number)
        }
        Value::Inet(l) => {
            if let Some(x) = number.to_u32() {
                l.partial_cmp(&IpAddr::V4(x.into()))
            } else if let Some(x) = number.to_u128() {
                l.partial_cmp(&IpAddr::V6(x.into()))
            } else {
                None
            }
        }
        _ => None,
    }
}

fn value_cmp_with_text(value: &Value, text: &str) -> Option<Ordering> {
    match value {
        Value::Str(l) => Some(l.as_str().cmp(text)),
        Value::Date(l) => l.partial_cmp(&parse_date(text)?),
        Value::Timestamp(l) => l.partial_cmp(&parse_timestamp(text)?),
        Value::Time(l) => l.partial_cmp(&parse_time(text)?),
        Value::Uuid(l) => l.partial_cmp(&parse_uuid(text).ok()?),
        Value::Inet(l) => l.partial_cmp(&IpAddr::from_str(text).ok()?),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use {
        super::{Evaluated, value_cmp_with_number, value_cmp_with_text},
        crate::data::{
            Value,
            value::{parse_date, parse_time, parse_timestamp, parse_uuid},
        },
        bigdecimal::BigDecimal,
        rust_decimal::Decimal,
        std::{borrow::Cow, cmp::Ordering, net::IpAddr, str::FromStr},
    };

    #[test]
    fn test_value_cmp_with_number() {
        let cmp = |v: &Value, n: i32| value_cmp_with_number(v, &BigDecimal::from(n));

        assert_eq!(cmp(&Value::I8(1), 1), Some(Ordering::Equal));
        assert_eq!(cmp(&Value::I16(1), 2), Some(Ordering::Less));
        assert_eq!(cmp(&Value::I32(10), 3), Some(Ordering::Greater));
        assert_eq!(cmp(&Value::I64(10), 10), Some(Ordering::Equal));
        assert_eq!(cmp(&Value::I128(10), 10), Some(Ordering::Equal));
        assert_eq!(cmp(&Value::U8(1), 1), Some(Ordering::Equal));
        assert_eq!(cmp(&Value::U16(1), 2), Some(Ordering::Less));
        assert_eq!(cmp(&Value::U32(10), 3), Some(Ordering::Greater));
        assert_eq!(cmp(&Value::U64(10), 10), Some(Ordering::Equal));
        assert_eq!(cmp(&Value::U128(10), 10), Some(Ordering::Equal));
        assert_eq!(cmp(&Value::F32(10.0), 10), Some(Ordering::Equal));
        assert_eq!(cmp(&Value::F64(10.0), 10), Some(Ordering::Equal));
        assert_eq!(
            cmp(&Value::Decimal(Decimal::new(215, 2)), 3),
            Some(Ordering::Less)
        );
        assert_eq!(cmp(&Value::Null, 1), None);
        assert_eq!(
            cmp(&Value::Inet(IpAddr::from_str("127.0.0.1").unwrap()), -1),
            None
        );

        // Inet with large numbers
        assert_eq!(
            value_cmp_with_number(
                &Value::Inet(IpAddr::from_str("255.255.255.255").unwrap()),
                &BigDecimal::new(4_294_967_295_u32.into(), 0),
            ),
            Some(Ordering::Equal),
        );
        assert_eq!(
            value_cmp_with_number(
                &Value::Inet(IpAddr::from_str("::2:4cb0:16ea").unwrap()),
                &BigDecimal::new(9_876_543_210_u128.into(), 0),
            ),
            Some(Ordering::Equal),
        );
    }

    #[test]
    fn test_value_cmp_with_text() {
        assert_eq!(
            value_cmp_with_text(&Value::Str("Hello".to_owned()), "Hello"),
            Some(Ordering::Equal),
        );
        assert_eq!(
            value_cmp_with_text(
                &Value::Date(parse_date("2021-11-21").unwrap()),
                "2021-11-21"
            ),
            Some(Ordering::Equal),
        );
        assert_eq!(
            value_cmp_with_text(
                &Value::Timestamp(parse_timestamp("2021-11-21T10:00:00Z").unwrap()),
                "2021-11-21T10:00:00Z",
            ),
            Some(Ordering::Equal),
        );
        assert_eq!(
            value_cmp_with_text(&Value::Time(parse_time("10:00:00").unwrap()), "10:00:00"),
            Some(Ordering::Equal),
        );
        assert_eq!(
            value_cmp_with_text(
                &Value::Uuid(parse_uuid("936DA01F9ABD4d9d80C702AF85C822A8").unwrap()),
                "936DA01F9ABD4d9d80C702AF85C822A8",
            ),
            Some(Ordering::Equal),
        );
        assert_eq!(
            value_cmp_with_text(
                &Value::Inet(IpAddr::from_str("215.87.1.1").unwrap()),
                "215.87.1.1"
            ),
            Some(Ordering::Equal),
        );
        assert_eq!(value_cmp_with_text(&Value::Null, "foo"), None);
    }

    #[test]
    fn literal_comparison_regression() {
        macro_rules! literal {
            (num: $value:expr) => {
                Evaluated::Number(Cow::Owned(BigDecimal::from_str($value).unwrap()))
            };
            (text: $value:expr) => {
                Evaluated::Text(Cow::Owned($value.to_owned()))
            };
        }

        assert_eq!(
            Some(Ordering::Less),
            literal!(num: "1").evaluate_cmp(&literal!(num: "2"))
        );
        assert_eq!(
            Some(Ordering::Less),
            literal!(text: "a").evaluate_cmp(&literal!(text: "b"))
        );

        let slice = Evaluated::StrSlice {
            source: Cow::Owned("hello".to_owned()),
            range: 0..5,
        };
        assert_eq!(None, literal!(num: "42").evaluate_cmp(&slice));
    }
}
