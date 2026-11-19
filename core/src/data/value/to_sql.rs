use crate::{ast::ToSql, data::Value};

impl ToSql for Value {
    fn to_sql(&self) -> String {
        match self {
            Value::Bool(b) => b.to_string().to_uppercase(),
            Value::I8(n) => n.to_string(),
            Value::I16(n) => n.to_string(),
            Value::I32(n) => n.to_string(),
            Value::I64(n) => n.to_string(),
            Value::I128(n) => n.to_string(),
            Value::U8(n) => n.to_string(),
            Value::U16(n) => n.to_string(),
            Value::U32(n) => n.to_string(),
            Value::U64(n) => n.to_string(),
            Value::U128(n) => n.to_string(),
            Value::F32(n) => n.to_string(),
            Value::F64(n) => n.to_string(),
            Value::Decimal(n) => n.to_string(),
            Value::Str(s) => {
                let escaped = s.replace('\'', "''");
                format!("'{escaped}'")
            }
            Value::Bytea(bytes) => format!("X'{}'", hex::encode(bytes)),
            Value::Inet(addr) => format!("'{addr}'"),
            Value::Date(d) => format!("DATE '{d}'"),
            Value::Timestamp(ts) => format!("TIMESTAMP '{ts}'"),
            Value::Time(t) => format!("TIME '{t}'"),
            Value::Interval(i) => format!("INTERVAL {}", i.to_sql_str()),
            Value::Uuid(u) => format!("'{}'", uuid::Uuid::from_u128(*u).hyphenated()),
            Value::Map(_) => {
                let json: serde_json::Value =
                    self.clone().try_into().unwrap_or(serde_json::Value::Null);
                let escaped = json.to_string().replace('\'', "''");
                format!("'{escaped}'")
            }
            Value::List(values) => {
                let elements = values
                    .iter()
                    .map(list_element_to_sql)
                    .collect::<Vec<_>>()
                    .join(", ");
                format!("[{elements}]")
            }
            Value::Point(p) => format!("POINT({}, {})", p.x, p.y),
            Value::Null => "NULL".to_owned(),
        }
    }
}

fn list_element_to_sql(value: &Value) -> String {
    use crate::ast::DataType::*;

    let sql = match value {
        Value::Point(p) => format!("POINT(CAST('{}' AS FLOAT), CAST('{}' AS FLOAT))", p.x, p.y),
        Value::Bool(_)
        | Value::I8(_)
        | Value::I16(_)
        | Value::I32(_)
        | Value::I64(_)
        | Value::I128(_)
        | Value::U8(_)
        | Value::U16(_)
        | Value::U32(_)
        | Value::U64(_)
        | Value::U128(_)
        | Value::F32(_)
        | Value::F64(_)
        | Value::Decimal(_)
        | Value::Str(_)
        | Value::Bytea(_)
        | Value::Inet(_)
        | Value::Date(_)
        | Value::Timestamp(_)
        | Value::Time(_)
        | Value::Interval(_)
        | Value::Uuid(_)
        | Value::Map(_)
        | Value::List(_)
        | Value::Null => value.to_sql(),
    };

    match value.get_type() {
        Some(ty @ (Float32 | Float | Decimal)) => format!("CAST('{sql}' AS {ty})"),
        Some(
            ty @ (Int8 | Int16 | Int32 | Int128 | Uint8 | Uint16 | Uint32 | Uint64 | Uint128 | Inet
            | Uuid | Map),
        ) => format!("CAST({sql} AS {ty})"),
        Some(Boolean | Int | Text | Bytea | Date | Timestamp | Time | Interval | Point | List)
        | None => sql,
    }
}

#[cfg(test)]
mod tests {
    use {
        crate::{
            ast::ToSql,
            data::{Interval, Point, Value},
        },
        chrono::{NaiveDate, NaiveTime},
        rust_decimal::Decimal,
        std::{collections::BTreeMap, net::IpAddr, str::FromStr},
    };

    #[test]
    fn to_sql() {
        // Bool - true, false
        assert_eq!(Value::Bool(true).to_sql(), "TRUE");
        assert_eq!(Value::Bool(false).to_sql(), "FALSE");

        // I8, I16, I32, I64, I128
        assert_eq!(Value::I8(127).to_sql(), "127");
        assert_eq!(Value::I16(32767).to_sql(), "32767");
        assert_eq!(Value::I32(2_147_483_647).to_sql(), "2147483647");
        assert_eq!(Value::I64(64).to_sql(), "64");
        assert_eq!(Value::I128(128).to_sql(), "128");

        // U8, U16, U32, U64, U128
        assert_eq!(Value::U8(255).to_sql(), "255");
        assert_eq!(Value::U16(65535).to_sql(), "65535");
        assert_eq!(Value::U32(32).to_sql(), "32");
        assert_eq!(Value::U64(64).to_sql(), "64");
        assert_eq!(Value::U128(128).to_sql(), "128");

        // F32, F64
        assert_eq!(Value::F32(1.5).to_sql(), "1.5");
        assert_eq!(Value::F64(2.5).to_sql(), "2.5");

        // Decimal
        assert_eq!(Value::Decimal(Decimal::new(314, 2)).to_sql(), "3.14");

        // Str - plain, with quotes
        assert_eq!(Value::Str("hello".to_owned()).to_sql(), "'hello'");
        assert_eq!(Value::Str("it's".to_owned()).to_sql(), "'it''s'");

        // Bytea
        assert_eq!(
            Value::Bytea(vec![0x48, 0x65, 0x6c, 0x6c, 0x6f]).to_sql(),
            "X'48656c6c6f'"
        );

        // Inet - IPv4, IPv6
        assert_eq!(
            Value::Inet(IpAddr::from_str("192.168.1.1").unwrap()).to_sql(),
            "'192.168.1.1'"
        );
        assert_eq!(
            Value::Inet(IpAddr::from_str("::1").unwrap()).to_sql(),
            "'::1'"
        );

        // Date
        assert_eq!(
            Value::Date(NaiveDate::from_ymd_opt(2024, 1, 15).unwrap()).to_sql(),
            "DATE '2024-01-15'"
        );

        // Timestamp
        assert_eq!(
            Value::Timestamp(
                NaiveDate::from_ymd_opt(2024, 1, 15)
                    .unwrap()
                    .and_hms_opt(13, 30, 45)
                    .unwrap()
            )
            .to_sql(),
            "TIMESTAMP '2024-01-15 13:30:45'"
        );

        // Time
        assert_eq!(
            Value::Time(NaiveTime::from_hms_opt(13, 30, 45).unwrap()).to_sql(),
            "TIME '13:30:45'"
        );

        // Interval
        assert_eq!(
            Value::Interval(Interval::Month(14)).to_sql(),
            "INTERVAL '1-2' YEAR TO MONTH"
        );

        // Uuid
        assert_eq!(
            Value::Uuid(0x936d_a01f_9abd_4d9d_80c7_02af_85c8_22a8_u128).to_sql(),
            "'936da01f-9abd-4d9d-80c7-02af85c822a8'"
        );

        // Map
        let map = BTreeMap::from([
            ("a".to_owned(), Value::I64(1)),
            ("b".to_owned(), Value::Bool(true)),
        ]);
        assert_eq!(Value::Map(map).to_sql(), "'{\"a\":1,\"b\":true}'");

        // List
        let list = vec![Value::I64(1), Value::I64(2), Value::I64(3)];
        assert_eq!(Value::List(list).to_sql(), "[1, 2, 3]");

        // Point
        assert_eq!(
            Value::Point(Point::new(1.5, 2.5)).to_sql(),
            "POINT(1.5, 2.5)"
        );

        // Null
        assert_eq!(Value::Null.to_sql(), "NULL");
    }

    #[test]
    fn list_element_to_sql() {
        macro_rules! test {
            ($value:expr, $expected:literal) => {
                assert_eq!(super::list_element_to_sql(&$value), $expected);
            };
        }

        test!(
            Value::Point(Point::new(f64::INFINITY, f64::NAN)),
            "POINT(CAST('inf' AS FLOAT), CAST('NaN' AS FLOAT))"
        );

        test!(Value::F32(-0.0), "CAST('-0' AS FLOAT32)");
        test!(Value::F64(1.0), "CAST('1' AS FLOAT)");
        test!(
            Value::Decimal(Decimal::new(25, 1)),
            "CAST('2.5' AS DECIMAL)"
        );

        test!(Value::I8(-1), "CAST(-1 AS INT8)");
        test!(Value::I16(2), "CAST(2 AS INT16)");
        test!(Value::I32(3), "CAST(3 AS INT32)");
        test!(Value::I128(4), "CAST(4 AS INT128)");
        test!(Value::U8(5), "CAST(5 AS UINT8)");
        test!(Value::U16(6), "CAST(6 AS UINT16)");
        test!(Value::U32(7), "CAST(7 AS UINT32)");
        test!(Value::U64(8), "CAST(8 AS UINT64)");
        test!(Value::U128(9), "CAST(9 AS UINT128)");
        test!(Value::Inet("::1".parse().unwrap()), "CAST('::1' AS INET)");
        test!(
            Value::Uuid(0),
            "CAST('00000000-0000-0000-0000-000000000000' AS UUID)"
        );
        let map = BTreeMap::from([("it's".to_owned(), Value::I64(1))]);
        test!(Value::Map(map), r#"CAST('{"it''s":1}' AS MAP)"#);

        test!(Value::Bool(true), "TRUE");
        test!(Value::I64(1), "1");
        test!(Value::Str("2026-01-01".to_owned()), "'2026-01-01'");
        test!(Value::Bytea(vec![0xff]), "X'ff'");
        let date = NaiveDate::from_ymd_opt(2026, 1, 1).unwrap();
        test!(Value::Date(date), "DATE '2026-01-01'");
        test!(
            Value::Timestamp(date.and_hms_opt(12, 30, 0).unwrap()),
            "TIMESTAMP '2026-01-01 12:30:00'"
        );
        test!(
            Value::Time(NaiveTime::from_hms_opt(12, 30, 0).unwrap()),
            "TIME '12:30:00'"
        );
        test!(
            Value::Interval(Interval::Month(14)),
            "INTERVAL '1-2' YEAR TO MONTH"
        );
        test!(Value::List(vec![]), "[]");
        test!(Value::List(vec![Value::I8(1)]), "[CAST(1 AS INT8)]");
        test!(Value::Null, "NULL");
    }
}
