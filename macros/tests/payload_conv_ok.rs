use gluesql::FromGlueRow;
use gluesql::core::data::Value;
use gluesql::core::executor::Payload;
use gluesql::core::row_conversion::{SelectExt, SelectResultExt};

use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
use rust_decimal::Decimal;
use std::collections::BTreeMap;
use std::net::{IpAddr, Ipv4Addr};

#[derive(Debug, PartialEq, FromGlueRow)]
struct User {
    id: i64,
    name: String,
    email: Option<String>,
}

#[test]
fn rows_as_ok() {
    let payload = Payload::Select {
        labels: vec!["id".into(), "name".into(), "email".into()],
        rows: vec![
            vec![Value::I64(1), Value::Str("A".into()), Value::Null],
            vec![
                Value::I64(2),
                Value::Str("B".into()),
                Value::Str("b@x.com".into()),
            ],
        ],
    };
    let v: Vec<User> = payload.rows_as::<User>().unwrap();
    assert_eq!(v.len(), 2);
    assert_eq!(
        v[0],
        User {
            id: 1,
            name: "A".into(),
            email: None
        }
    );
    assert_eq!(v[1].email.as_deref(), Some("b@x.com"));
}

#[test]
fn one_as_ok() {
    let payload = Payload::Select {
        labels: vec!["id".into(), "name".into(), "email".into()],
        rows: vec![vec![Value::I64(7), Value::Str("Z".into()), Value::Null]],
    };
    let u: User = payload.one_as::<User>().unwrap();
    assert_eq!(u.id, 7);
}

#[derive(Debug, PartialEq, FromGlueRow)]
struct Order {
    #[glue(rename = "order_id")]
    id: i64,
    total: f64,
}

#[test]
fn rename_ok() {
    let payload = Payload::Select {
        labels: vec!["order_id".into(), "total".into()],
        rows: vec![vec![Value::I64(10), Value::F64(12.5)]],
    };
    let o: Order = payload.one_as::<Order>().unwrap();
    assert_eq!(o.id, 10);
}

#[derive(Debug, PartialEq, FromGlueRow)]
struct AllTypes {
    i8_: i8,
    i16_: i16,
    i32_: i32,
    i64_: i64,
    i128_: i128,
    u8_: u8,
    u16_: u16,
    u32_: u32,
    u64_: u64,
    u128_: u128,
    f32_: f32,
    f64_: f64,
    b_: bool,
    s_: String,
    bytes_: Vec<u8>,
    ip_: IpAddr,
    date_: NaiveDate,
    ts_: NaiveDateTime,
    time_: NaiveTime,
    dec_: Decimal,
    interval_: gluesql::core::data::Interval,
    map_: BTreeMap<String, Value>,
    list_: Vec<Value>,
    point_: gluesql::core::data::Point,
    opt_s_none: Option<String>,
    opt_i64_some: Option<i64>,
}

#[test]
fn all_types_ok() {
    use gluesql::core::data::{Interval, Point};

    let date = NaiveDate::from_ymd_opt(2020, 1, 2).unwrap();
    let time = NaiveTime::from_hms_opt(3, 4, 5).unwrap();
    let ts = NaiveDateTime::new(date, time);

    let mut map = BTreeMap::new();
    map.insert("a".to_string(), Value::I64(1));
    map.insert("b".to_string(), Value::Bool(true));

    let list = vec![Value::Str("x".into()), Value::F64(1.5)];

    let payload = Payload::Select {
        labels: vec![
            "i8_".into(),
            "i16_".into(),
            "i32_".into(),
            "i64_".into(),
            "i128_".into(),
            "u8_".into(),
            "u16_".into(),
            "u32_".into(),
            "u64_".into(),
            "u128_".into(),
            "f32_".into(),
            "f64_".into(),
            "b_".into(),
            "s_".into(),
            "bytes_".into(),
            "ip_".into(),
            "date_".into(),
            "ts_".into(),
            "time_".into(),
            "dec_".into(),
            "interval_".into(),
            "map_".into(),
            "list_".into(),
            "point_".into(),
            "opt_s_none".into(),
            "opt_i64_some".into(),
        ],
        rows: vec![vec![
            Value::I8(-1),
            Value::I16(-2),
            Value::I32(-3),
            Value::I64(-4),
            Value::I128(-5),
            Value::U8(1),
            Value::U16(2),
            Value::U32(3),
            Value::U64(4),
            Value::Uuid(5),
            Value::F32(1.25),
            Value::F64(2.5),
            Value::Bool(true),
            Value::Str("hello".into()),
            Value::Bytea(vec![1, 2, 3]),
            Value::Inet(Ipv4Addr::new(127, 0, 0, 1).into()),
            Value::Date(date),
            Value::Timestamp(ts),
            Value::Time(time),
            Value::Decimal(Decimal::new(12345, 3)), // 12.345
            Value::Interval(Interval::months(3)),
            Value::Map(map.clone()),
            Value::List(list.clone()),
            Value::Point(Point::new(1.0, 2.0)),
            Value::Null,
            Value::I64(42),
        ]],
    };

    let row: AllTypes = payload.one_as::<AllTypes>().unwrap();
    assert_eq!(row.i8_, -1);
    assert_eq!(row.i16_, -2);
    assert_eq!(row.i32_, -3);
    assert_eq!(row.i64_, -4);
    assert_eq!(row.i128_, -5);
    assert_eq!(row.u8_, 1);
    assert_eq!(row.u16_, 2);
    assert_eq!(row.u32_, 3);
    assert_eq!(row.u64_, 4);
    assert_eq!(row.u128_, 5);
    assert_eq!(row.f32_, 1.25);
    assert_eq!(row.f64_, 2.5);
    assert_eq!(row.b_, true);
    assert_eq!(row.s_, "hello".to_string());
    assert_eq!(row.bytes_, vec![1, 2, 3]);
    assert_eq!(row.ip_, IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)));
    assert_eq!(row.date_, date);
    assert_eq!(row.ts_, ts);
    assert_eq!(row.time_, time);
    assert_eq!(row.dec_, Decimal::new(12345, 3));
    assert_eq!(row.interval_, Interval::months(3));
    assert_eq!(row.map_, map);
    assert_eq!(row.list_, list);
    assert_eq!(row.point_, Point::new(1.0, 2.0));
    assert_eq!(row.opt_s_none, None);
    assert_eq!(row.opt_i64_some, Some(42));
}
