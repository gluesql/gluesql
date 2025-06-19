use gluesql_macros::FromRow;
use gluesql_core::{data::{Row, Value}, FromRow as FromRowTrait};
use std::collections::HashMap;
use chrono::{NaiveDate, NaiveDateTime};
use rust_decimal::Decimal;
use std::net::{IpAddr, Ipv4Addr};

#[derive(FromRow, Debug, PartialEq)]
struct Sample {
    id: i64,
    name: String,
}

#[test]
fn derive_from_row_basic() {
    let row = Row::Map(HashMap::from([
        ("id".to_string(), Value::I64(1)),
        ("name".to_string(), Value::Str("abc".to_string())),
    ]));

    let sample = Sample::from_row(&row).unwrap();

    assert_eq!(sample, Sample { id: 1, name: "abc".to_string() });
}

#[derive(FromRow, Debug, PartialEq)]
struct Extended {
    id: i64,
    name: String,
    active: bool,
    rate: f64,
    balance: Decimal,
    created: NaiveDateTime,
    ip: IpAddr,
}

#[test]
fn derive_from_row_multiple_types() {
    let ts = NaiveDate::from_ymd_opt(2024, 1, 2)
        .unwrap()
        .and_hms_opt(3, 4, 5)
        .unwrap();

    let row = Row::Map(HashMap::from([
        ("id".to_string(), Value::I64(10)),
        ("name".to_string(), Value::Str("xyz".to_string())),
        ("active".to_string(), Value::Bool(true)),
        ("rate".to_string(), Value::F64(1.23)),
        ("balance".to_string(), Value::Decimal(Decimal::new(4567, 2))),
        ("created".to_string(), Value::Timestamp(ts)),
        (
            "ip".to_string(),
            Value::Inet(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1))),
        ),
    ]));

    let value = Extended::from_row(&row).unwrap();

    assert_eq!(
        value,
        Extended {
            id: 10,
            name: "xyz".to_string(),
            active: true,
            rate: 1.23,
            balance: Decimal::new(4567, 2),
            created: ts,
            ip: IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)),
        }
    );
}
