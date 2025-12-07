use {
    super::Evaluated,
    crate::data::{
        BigDecimalExt, Value,
        value::{parse_date, parse_time, parse_timestamp, parse_uuid},
    },
    bigdecimal::BigDecimal,
    std::{borrow::Cow, cmp::Ordering, net::IpAddr, str::FromStr},
};

impl<'a> Evaluated<'a> {
    pub fn evaluate_cmp(&self, other: &Evaluated<'a>) -> Option<Ordering> {
        match (self, other) {
            (Evaluated::Number(l), Evaluated::Number(r)) => Some(l.cmp(r)),
            (Evaluated::Text(l), Evaluated::Text(r)) => Some(l.cmp(r)),
            (left @ (Evaluated::Number(_) | Evaluated::Text(_)), Evaluated::Value(right)) => {
                value_cmp_with_literal(right, left).map(Ordering::reverse)
            }
            (Evaluated::Value(left), right @ (Evaluated::Number(_) | Evaluated::Text(_))) => {
                value_cmp_with_literal(left, right)
            }
            (Evaluated::Value(left), Evaluated::Value(right)) => left.evaluate_cmp(right),
            (Evaluated::Text(l), Evaluated::StrSlice { source, range }) => {
                Some(l.as_ref().cmp(&source[range.clone()]))
            }
            (Evaluated::StrSlice { source, range }, Evaluated::Text(r)) => {
                Some(source[range.clone()].cmp(r.as_ref()))
            }
            (Evaluated::Number(_), Evaluated::Text(_) | Evaluated::StrSlice { .. })
            | (Evaluated::Text(_) | Evaluated::StrSlice { .. }, Evaluated::Number(_)) => None,
            (Evaluated::Value(left), Evaluated::StrSlice { source, range }) => {
                let slice = Evaluated::Text(Cow::Borrowed(&source[range.clone()]));

                value_cmp_with_literal(left, &slice)
            }
            (Evaluated::StrSlice { source, range }, Evaluated::Value(right)) => {
                let slice = Evaluated::Text(Cow::Borrowed(&source[range.clone()]));

                value_cmp_with_literal(right, &slice).map(Ordering::reverse)
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

fn value_cmp_with_literal(value: &Value, literal: &Evaluated<'_>) -> Option<Ordering> {
    match (value, literal) {
        (Value::I8(l), Evaluated::Number(r)) => l.partial_cmp(&r.to_i8()?),
        (Value::I16(l), Evaluated::Number(r)) => l.partial_cmp(&r.to_i16()?),
        (Value::I32(l), Evaluated::Number(r)) => l.partial_cmp(&r.to_i32()?),
        (Value::I64(l), Evaluated::Number(r)) => l.partial_cmp(&r.to_i64()?),
        (Value::I128(l), Evaluated::Number(r)) => l.partial_cmp(&r.to_i128()?),
        (Value::U8(l), Evaluated::Number(r)) => l.partial_cmp(&r.to_u8()?),
        (Value::U16(l), Evaluated::Number(r)) => l.partial_cmp(&r.to_u16()?),
        (Value::U32(l), Evaluated::Number(r)) => l.partial_cmp(&r.to_u32()?),
        (Value::U64(l), Evaluated::Number(r)) => l.partial_cmp(&r.to_u64()?),
        (Value::U128(l), Evaluated::Number(r)) => l.partial_cmp(&r.to_u128()?),
        (Value::F32(l), Evaluated::Number(r)) => l.partial_cmp(&r.to_f32()?),
        (Value::F64(l), Evaluated::Number(r)) => l.partial_cmp(&r.to_f64()?),
        (Value::Decimal(l), Evaluated::Number(r)) => {
            BigDecimal::new(l.mantissa().into(), i64::from(l.scale())).partial_cmp(r)
        }
        (Value::Str(l), Evaluated::Text(r)) => Some(l.as_str().cmp(r)),
        (Value::Date(l), Evaluated::Text(r)) => l.partial_cmp(&parse_date(r)?),
        (Value::Timestamp(l), Evaluated::Text(r)) => l.partial_cmp(&parse_timestamp(r)?),
        (Value::Time(l), Evaluated::Text(r)) => l.partial_cmp(&parse_time(r)?),
        (Value::Uuid(l), Evaluated::Text(r)) => l.partial_cmp(&parse_uuid(r).ok()?),
        (Value::Inet(l), Evaluated::Text(r)) => l.partial_cmp(&IpAddr::from_str(r).ok()?),
        (Value::Inet(l), Evaluated::Number(r)) => {
            if let Some(x) = r.to_u32() {
                l.partial_cmp(&IpAddr::V4(x.into()))
            } else if let Some(x) = r.to_u128() {
                l.partial_cmp(&IpAddr::V6(x.into()))
            } else {
                None
            }
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use {
        super::{Evaluated, value_cmp_with_literal},
        crate::data::{
            Value,
            value::{parse_date, parse_time, parse_timestamp, parse_uuid},
        },
        bigdecimal::BigDecimal,
        rust_decimal::Decimal,
        std::{borrow::Cow, cmp::Ordering, net::IpAddr, str::FromStr},
    };

    #[test]
    fn value_cmp_with_literal_regression() {
        let num = |n| Evaluated::Number(Cow::Owned(BigDecimal::from(n)));
        let text = |v: &str| Evaluated::Text(Cow::Owned(v.to_owned()));

        #[allow(clippy::similar_names)]
        let assert_cmp = |value: Value, literal: Evaluated<'_>, expected| {
            assert_eq!(value_cmp_with_literal(&value, &literal), expected);
        };

        assert_cmp(Value::I8(1), num(1), Some(Ordering::Equal));
        assert_cmp(Value::I16(1), num(2), Some(Ordering::Less));
        assert_cmp(Value::I32(10), num(3), Some(Ordering::Greater));
        assert_cmp(Value::I64(10), num(10), Some(Ordering::Equal));
        assert_cmp(Value::I128(10), num(10), Some(Ordering::Equal));
        assert_cmp(Value::U8(1), num(1), Some(Ordering::Equal));
        assert_cmp(Value::U16(1), num(2), Some(Ordering::Less));
        assert_cmp(Value::U32(10), num(3), Some(Ordering::Greater));
        assert_cmp(Value::U64(10), num(10), Some(Ordering::Equal));
        assert_cmp(Value::U128(10), num(10), Some(Ordering::Equal));
        assert_cmp(Value::F32(10.0), num(10), Some(Ordering::Equal));
        assert_cmp(Value::F64(10.0), num(10), Some(Ordering::Equal));
        assert_cmp(
            Value::Decimal(Decimal::new(215, 2)),
            num(3),
            Some(Ordering::Less),
        );
        assert_cmp(
            Value::Str("Hello".to_owned()),
            text("Hello"),
            Some(Ordering::Equal),
        );
        assert_cmp(
            Value::Date(parse_date("2021-11-21").unwrap()),
            text("2021-11-21"),
            Some(Ordering::Equal),
        );
        assert_cmp(
            Value::Timestamp(parse_timestamp("2021-11-21T10:00:00Z").unwrap()),
            text("2021-11-21T10:00:00Z"),
            Some(Ordering::Equal),
        );
        assert_cmp(
            Value::Time(parse_time("10:00:00").unwrap()),
            text("10:00:00"),
            Some(Ordering::Equal),
        );
        assert_cmp(
            Value::Uuid(parse_uuid("936DA01F9ABD4d9d80C702AF85C822A8").unwrap()),
            text("936DA01F9ABD4d9d80C702AF85C822A8"),
            Some(Ordering::Equal),
        );
        assert_cmp(
            Value::Inet(IpAddr::from_str("215.87.1.1").unwrap()),
            text("215.87.1.1"),
            Some(Ordering::Equal),
        );
        assert_cmp(
            Value::Inet(IpAddr::from_str("255.255.255.255").unwrap()),
            Evaluated::Number(Cow::Owned(BigDecimal::new(4_294_967_295_u32.into(), 0))),
            Some(Ordering::Equal),
        );
        assert_cmp(
            Value::Inet(IpAddr::from_str("::2:4cb0:16ea").unwrap()),
            Evaluated::Number(Cow::Owned(BigDecimal::new(9_876_543_210_u128.into(), 0))),
            Some(Ordering::Equal),
        );
        assert_cmp(
            Value::Inet(IpAddr::from_str("127.0.0.1").unwrap()),
            num(-1),
            None,
        );
        assert_cmp(Value::Null, num(1), None);
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
