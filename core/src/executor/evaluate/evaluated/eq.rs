use {
    super::Evaluated,
    crate::data::{
        BigDecimalExt, Value,
        value::{parse_time, parse_timestamp, parse_uuid},
    },
    chrono::NaiveDate,
    std::{
        borrow::Cow,
        net::{IpAddr, Ipv4Addr, Ipv6Addr},
        str::FromStr,
    },
    utils::Tribool,
};

impl<'a> Evaluated<'a> {
    pub fn evaluate_eq(&self, other: &Evaluated<'a>) -> Tribool {
        match (self, other) {
            (Evaluated::Number(a), Evaluated::Number(b)) => Tribool::from(a == b),
            (Evaluated::Text(a), Evaluated::Text(b)) => Tribool::from(a == b),
            (literal @ (Evaluated::Number(_) | Evaluated::Text(_)), Evaluated::Value(value))
            | (Evaluated::Value(value), literal @ (Evaluated::Number(_) | Evaluated::Text(_))) => {
                value_eq_with_literal(value, literal)
            }
            (Evaluated::Value(a), Evaluated::Value(b)) => a.evaluate_eq(b),
            (Evaluated::Number(_), Evaluated::Text(_))
            | (Evaluated::Text(_), Evaluated::Number(_)) => Tribool::from(false),
            (
                literal @ (Evaluated::Number(_) | Evaluated::Text(_)),
                Evaluated::StrSlice { source, range },
            )
            | (
                Evaluated::StrSlice { source, range },
                literal @ (Evaluated::Number(_) | Evaluated::Text(_)),
            ) => {
                let slice = Evaluated::Text(Cow::Borrowed(&source[range.clone()]));

                Tribool::from(literal == &slice)
            }
            (Evaluated::Value(a), Evaluated::StrSlice { source, range })
            | (Evaluated::StrSlice { source, range }, Evaluated::Value(a)) => {
                let slice = Evaluated::Text(Cow::Borrowed(&source[range.clone()]));

                value_eq_with_literal(a, &slice)
            }
            (
                Evaluated::StrSlice { source, range },
                Evaluated::StrSlice {
                    source: source2,
                    range: range2,
                },
            ) => Tribool::from(source[range.clone()] == source2[range2.clone()]),
        }
    }
}

fn value_eq_with_literal(value: &Value, literal: &Evaluated<'_>) -> Tribool {
    match (value, literal) {
        (Value::I8(l), Evaluated::Number(r)) => Tribool::from(r.to_i8().is_some_and(|r| *l == r)),
        (Value::I16(l), Evaluated::Number(r)) => Tribool::from(r.to_i16().is_some_and(|r| *l == r)),
        (Value::I32(l), Evaluated::Number(r)) => Tribool::from(r.to_i32().is_some_and(|r| *l == r)),
        (Value::I64(l), Evaluated::Number(r)) => Tribool::from(r.to_i64().is_some_and(|r| *l == r)),
        (Value::I128(l), Evaluated::Number(r)) => {
            Tribool::from(r.to_i128().is_some_and(|r| *l == r))
        }
        (Value::U8(l), Evaluated::Number(r)) => Tribool::from(r.to_u8().is_some_and(|r| *l == r)),
        (Value::U16(l), Evaluated::Number(r)) => Tribool::from(r.to_u16().is_some_and(|r| *l == r)),
        (Value::U32(l), Evaluated::Number(r)) => Tribool::from(r.to_u32().is_some_and(|r| *l == r)),
        (Value::U64(l), Evaluated::Number(r)) => Tribool::from(r.to_u64().is_some_and(|r| *l == r)),
        (Value::U128(l), Evaluated::Number(r)) => {
            Tribool::from(r.to_u128().is_some_and(|r| *l == r))
        }
        (Value::F32(l), Evaluated::Number(r)) => Tribool::from(r.to_f32().is_some_and(|r| *l == r)),
        (Value::F64(l), Evaluated::Number(r)) => Tribool::from(r.to_f64().is_some_and(|r| *l == r)),
        (Value::Str(l), Evaluated::Text(r)) => Tribool::from(l == r.as_ref()),
        (Value::Date(l), Evaluated::Text(r)) => match r.parse::<NaiveDate>() {
            Ok(r) => Tribool::from(l == &r),
            Err(_) => Tribool::from(false),
        },
        (Value::Timestamp(l), Evaluated::Text(r)) => match parse_timestamp(r) {
            Some(r) => Tribool::from(l == &r),
            None => Tribool::from(false),
        },
        (Value::Time(l), Evaluated::Text(r)) => match parse_time(r) {
            Some(r) => Tribool::from(l == &r),
            None => Tribool::from(false),
        },
        (Value::Uuid(l), Evaluated::Text(r)) => {
            Tribool::from(parse_uuid(r).map(|r| l == &r).unwrap_or(false))
        }
        (Value::Inet(l), Evaluated::Text(r)) => match IpAddr::from_str(r) {
            Ok(x) => Tribool::from(l == &x),
            Err(_) => Tribool::from(false),
        },
        (Value::Inet(l), Evaluated::Number(r)) => {
            if let Some(x) = r.to_u32() {
                Tribool::from(l == &Ipv4Addr::from(x))
            } else if let Some(x) = r.to_u128() {
                Tribool::from(l == &Ipv6Addr::from(x))
            } else {
                Tribool::from(false)
            }
        }
        (Value::Null, _) => Tribool::Null,
        _ => Tribool::from(false),
    }
}

#[cfg(test)]
mod tests {
    use {
        super::{Evaluated, value_eq_with_literal},
        crate::data::{Value, value::parse_uuid},
        bigdecimal::BigDecimal,
        chrono::{NaiveDate, NaiveDateTime, NaiveTime},
        std::{borrow::Cow, net::IpAddr, str::FromStr},
        utils::Tribool::{False, Null, True},
    };

    fn date(year: i32, month: u32, day: u32) -> NaiveDate {
        chrono::NaiveDate::from_ymd_opt(year, month, day).unwrap()
    }

    fn date_time(y: i32, m: u32, d: u32, hh: u32, mm: u32, ss: u32, ms: u32) -> NaiveDateTime {
        chrono::NaiveDate::from_ymd_opt(y, m, d)
            .unwrap()
            .and_hms_milli_opt(hh, mm, ss, ms)
            .unwrap()
    }

    fn time(hour: u32, min: u32, sec: u32, milli: u32) -> NaiveTime {
        chrono::NaiveTime::from_hms_milli_opt(hour, min, sec, milli).unwrap()
    }

    #[test]
    fn literal_equality_regression() {
        macro_rules! literal {
            (num: $value:expr) => {
                Evaluated::Number(Cow::Owned(BigDecimal::from_str($value).unwrap()))
            };
            (text: $value:expr) => {
                Evaluated::Text(Cow::Owned($value.to_owned()))
            };
        }

        assert_eq!(True, literal!(num: "1").evaluate_eq(&literal!(num: "1")));
        assert_eq!(
            True,
            literal!(text: "foo").evaluate_eq(&literal!(text: "foo"))
        );
        assert_eq!(
            False,
            literal!(num: "1").evaluate_eq(&literal!(text: "foo"))
        );
    }

    #[test]
    fn value_eq_with_literal_matches_previous_behavior() {
        macro_rules! num {
            ($num: expr) => {
                Evaluated::Number(Cow::Owned(BigDecimal::from_str($num).unwrap()))
            };
        }

        macro_rules! text {
            ($text: expr) => {
                Evaluated::Text(Cow::Owned($text.to_owned()))
            };
        }

        let uuid_text = "936DA01F9ABD4d9d80C702AF85C822A8";
        let uuid = parse_uuid(uuid_text).unwrap();

        let inet = |v: &str| Value::Inet(IpAddr::from_str(v).unwrap());

        assert_eq!(True, value_eq_with_literal(&Value::I8(8), &num!("8")));
        assert_eq!(True, value_eq_with_literal(&Value::I32(32), &num!("32")));
        assert_eq!(True, value_eq_with_literal(&Value::I16(16), &num!("16")));
        assert_eq!(True, value_eq_with_literal(&Value::I32(32), &num!("32")));
        assert_eq!(True, value_eq_with_literal(&Value::I64(64), &num!("64")));
        assert_eq!(True, value_eq_with_literal(&Value::I128(128), &num!("128")));
        assert_eq!(True, value_eq_with_literal(&Value::U8(7), &num!("7")));
        assert_eq!(True, value_eq_with_literal(&Value::U16(64), &num!("64")));
        assert_eq!(True, value_eq_with_literal(&Value::U32(64), &num!("64")));
        assert_eq!(True, value_eq_with_literal(&Value::U64(64), &num!("64")));
        assert_eq!(True, value_eq_with_literal(&Value::U128(64), &num!("64")));
        assert_eq!(
            True,
            value_eq_with_literal(&Value::F32(7.123), &num!("7.123"))
        );
        assert_eq!(
            True,
            value_eq_with_literal(&Value::F64(7.123), &num!("7.123"))
        );
        assert_eq!(
            True,
            value_eq_with_literal(&Value::Str("Hello".to_owned()), &text!("Hello"))
        );
        assert_eq!(
            True,
            value_eq_with_literal(&inet("127.0.0.1"), &text!("127.0.0.1"))
        );
        assert_eq!(True, value_eq_with_literal(&inet("::1"), &text!("::1")));
        assert_eq!(True, value_eq_with_literal(&inet("0.0.0.0"), &num!("0")));
        assert_eq!(False, value_eq_with_literal(&inet("::1"), &num!("0")));
        assert_eq!(
            True,
            value_eq_with_literal(&inet("::2:4cb0:16ea"), &num!("9876543210"))
        );
        assert_eq!(False, value_eq_with_literal(&inet("::1"), &text!("-1")));
        assert_eq!(False, value_eq_with_literal(&inet("::1"), &num!("-1")));
        assert_eq!(
            True,
            value_eq_with_literal(&Value::Date(date(2021, 11, 20)), &text!("2021-11-20"))
        );
        assert_eq!(
            False,
            value_eq_with_literal(&Value::Date(date(2021, 11, 20)), &text!("202=abcdef"))
        );
        assert_eq!(
            True,
            value_eq_with_literal(
                &Value::Timestamp(date_time(2021, 11, 20, 10, 0, 0, 0)),
                &text!("2021-11-20T10:00:00Z")
            )
        );
        assert_eq!(
            False,
            value_eq_with_literal(
                &Value::Timestamp(date_time(2021, 11, 20, 10, 0, 0, 0)),
                &text!("2021-11-Hello")
            )
        );
        assert_eq!(
            True,
            value_eq_with_literal(&Value::Time(time(10, 0, 0, 0)), &text!("10:00:00"))
        );
        assert_eq!(
            False,
            value_eq_with_literal(&Value::Time(time(10, 0, 0, 0)), &text!("FALSE"))
        );
        assert_eq!(
            True,
            value_eq_with_literal(&Value::Uuid(uuid), &text!(uuid_text))
        );
        assert_eq!(Null, value_eq_with_literal(&Value::Null, &text!("STRING")));
        assert_eq!(Null, value_eq_with_literal(&Value::Null, &num!("123.456")));
    }
}
