use {
    super::Evaluated,
    crate::{
        data::{
            BigDecimalExt, Value,
            value::{parse_date, parse_time, parse_timestamp, parse_uuid},
        },
        executor::Literal,
    },
    bigdecimal::BigDecimal,
    std::{borrow::Cow, cmp::Ordering, net::IpAddr, str::FromStr},
};

impl<'a> Evaluated<'a> {
    pub fn evaluate_cmp(&self, other: &Evaluated<'a>) -> Option<Ordering> {
        match (self, other) {
            (Evaluated::Literal(l), Evaluated::Literal(r)) => l.evaluate_cmp(r),
            (Evaluated::Literal(l), Evaluated::Value(r)) => {
                value_cmp_with_literal(r, l).map(Ordering::reverse)
            }
            (Evaluated::Value(l), Evaluated::Literal(r)) => value_cmp_with_literal(l, r),
            (Evaluated::Value(l), Evaluated::Value(r)) => l.evaluate_cmp(r),
            (Evaluated::Literal(l), Evaluated::StrSlice { source, range }) => {
                let r = Literal::Text(Cow::Borrowed(&source[range.clone()]));

                l.evaluate_cmp(&r)
            }
            (Evaluated::Value(l), Evaluated::StrSlice { source, range }) => {
                let r = Literal::Text(Cow::Borrowed(&source[range.clone()]));

                value_cmp_with_literal(l, &r)
            }
            (Evaluated::StrSlice { source, range }, Evaluated::Literal(l)) => {
                let r = Literal::Text(Cow::Borrowed(&source[range.clone()]));

                l.evaluate_cmp(&r).map(Ordering::reverse)
            }
            (Evaluated::StrSlice { source, range }, Evaluated::Value(r)) => {
                let l = Literal::Text(Cow::Borrowed(&source[range.clone()]));

                value_cmp_with_literal(r, &l).map(Ordering::reverse)
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

fn value_cmp_with_literal(value: &Value, literal: &Literal<'_>) -> Option<Ordering> {
    match (value, literal) {
        (Value::I8(l), Literal::Number(r)) => l.partial_cmp(&r.to_i8()?),
        (Value::I16(l), Literal::Number(r)) => l.partial_cmp(&r.to_i16()?),
        (Value::I32(l), Literal::Number(r)) => l.partial_cmp(&r.to_i32()?),
        (Value::I64(l), Literal::Number(r)) => l.partial_cmp(&r.to_i64()?),
        (Value::I128(l), Literal::Number(r)) => l.partial_cmp(&r.to_i128()?),
        (Value::U8(l), Literal::Number(r)) => l.partial_cmp(&r.to_u8()?),
        (Value::U16(l), Literal::Number(r)) => l.partial_cmp(&r.to_u16()?),
        (Value::U32(l), Literal::Number(r)) => l.partial_cmp(&r.to_u32()?),
        (Value::U64(l), Literal::Number(r)) => l.partial_cmp(&r.to_u64()?),
        (Value::U128(l), Literal::Number(r)) => l.partial_cmp(&r.to_u128()?),
        (Value::F32(l), Literal::Number(r)) => l.partial_cmp(&r.to_f32()?),
        (Value::F64(l), Literal::Number(r)) => l.partial_cmp(&r.to_f64()?),
        (Value::Decimal(l), Literal::Number(r)) => {
            BigDecimal::new(l.mantissa().into(), i64::from(l.scale())).partial_cmp(r)
        }
        (Value::Str(l), Literal::Text(r)) => Some(l.as_str().cmp(r)),
        (Value::Date(l), Literal::Text(r)) => l.partial_cmp(&parse_date(r)?),
        (Value::Timestamp(l), Literal::Text(r)) => l.partial_cmp(&parse_timestamp(r)?),
        (Value::Time(l), Literal::Text(r)) => l.partial_cmp(&parse_time(r)?),
        (Value::Uuid(l), Literal::Text(r)) => l.partial_cmp(&parse_uuid(r).ok()?),
        (Value::Inet(l), Literal::Text(r)) => l.partial_cmp(&IpAddr::from_str(r).ok()?),
        (Value::Inet(l), Literal::Number(r)) => {
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
        super::value_cmp_with_literal,
        crate::{
            data::{
                Value,
                value::{parse_date, parse_time, parse_timestamp, parse_uuid},
            },
            executor::Literal,
        },
        bigdecimal::BigDecimal,
        rust_decimal::Decimal,
        std::{borrow::Cow, cmp::Ordering, net::IpAddr, str::FromStr},
    };

    #[test]
    fn value_cmp_with_literal_regression() {
        let num = |n| Literal::Number(Cow::Owned(BigDecimal::from(n)));
        let text = |v: &str| Literal::Text(Cow::Owned(v.to_owned()));

        #[allow(clippy::similar_names)]
        let assert_cmp = |value: Value, literal, expected| {
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
            Some(Ordering::Less),
        );
        assert_cmp(
            Value::Inet(IpAddr::from_str("215.87.1.1").unwrap()),
            text("215.87.1.1"),
            Some(Ordering::Less),
        );
        assert_cmp(
            Value::Inet(IpAddr::from_str("255.255.255.255").unwrap()),
            Literal::Number(Cow::Owned(BigDecimal::new(4_294_967_295_u32.into(), 0))),
            Some(Ordering::Equal),
        );
        assert_cmp(
            Value::Inet(IpAddr::from_str("::2:4cb0:16ea").unwrap()),
            Literal::Number(Cow::Owned(BigDecimal::new(9_876_543_210_u128.into(), 0))),
            Some(Ordering::Equal),
        );
        assert_cmp(Value::Null, num(1), None);
    }
}
