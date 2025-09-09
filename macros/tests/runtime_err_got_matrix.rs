use gluesql::{
    FromGlueRow,
    core::{
        data::Value,
        executor::Payload,
        row_conversion::{RowConversionError, SelectExt},
    },
};

#[derive(Debug, FromGlueRow)]
struct BoolField { v: bool }

#[test]
fn got_i64_expected_bool() {
    let payload = Payload::Select { labels: vec!["v".into()], rows: vec![vec![Value::I64(0)]] };
    let err = payload.rows_as::<BoolField>().unwrap_err();
    if let RowConversionError::TypeMismatch { expected, got, .. } = err {
        assert_eq!(expected, "bool");
        assert_eq!(got, "I64");
    } else { panic!("expected TypeMismatch") }
}

#[derive(Debug, FromGlueRow)]
struct StringField { v: String }

#[test]
fn got_i64_expected_string() {
    let payload = Payload::Select { labels: vec!["v".into()], rows: vec![vec![Value::I64(1)]] };
    let err = payload.rows_as::<StringField>().unwrap_err();
    if let RowConversionError::TypeMismatch { expected, got, .. } = err {
        assert_eq!(expected, "String");
        assert_eq!(got, "I64");
    } else { panic!("expected TypeMismatch") }
}

#[derive(Debug, FromGlueRow)]
struct F64Field { v: f64 }

#[test]
fn got_str_expected_f64() {
    let payload = Payload::Select { labels: vec!["v".into()], rows: vec![vec![Value::Str("x".into())]] };
    let err = payload.rows_as::<F64Field>().unwrap_err();
    if let RowConversionError::TypeMismatch { expected, got, .. } = err {
        assert_eq!(expected, "f64");
        assert_eq!(got, "Str");
    } else { panic!("expected TypeMismatch") }
}

#[derive(Debug, FromGlueRow)]
struct DecimalField { v: rust_decimal::Decimal }

#[test]
fn got_str_expected_decimal() {
    let payload = Payload::Select { labels: vec!["v".into()], rows: vec![vec![Value::Str("x".into())]] };
    let err = payload.rows_as::<DecimalField>().unwrap_err();
    if let RowConversionError::TypeMismatch { expected, got, .. } = err {
        assert_eq!(expected, "Decimal");
        assert_eq!(got, "Str");
    } else { panic!("expected TypeMismatch") }
}

#[derive(Debug, FromGlueRow)]
struct IpField { v: std::net::IpAddr }

#[test]
fn got_bool_expected_ip() {
    let payload = Payload::Select { labels: vec!["v".into()], rows: vec![vec![Value::Bool(true)]] };
    let err = payload.rows_as::<IpField>().unwrap_err();
    if let RowConversionError::TypeMismatch { expected, got, .. } = err {
        assert_eq!(expected, "IpAddr");
        assert_eq!(got, "Bool");
    } else { panic!("expected TypeMismatch") }
}

#[derive(Debug, FromGlueRow)]
struct DateField { v: chrono::NaiveDate }

#[test]
fn got_time_expected_date() {
    let payload = Payload::Select { labels: vec!["v".into()], rows: vec![vec![Value::Time(chrono::NaiveTime::from_hms_opt(0,0,0).unwrap())]] };
    let err = payload.rows_as::<DateField>().unwrap_err();
    if let RowConversionError::TypeMismatch { expected, got, .. } = err {
        assert_eq!(expected, "NaiveDate");
        assert_eq!(got, "Time");
    } else { panic!("expected TypeMismatch") }
}

#[derive(Debug, FromGlueRow)]
struct TimeField { v: chrono::NaiveTime }

#[test]
fn got_list_expected_time() {
    let payload = Payload::Select { labels: vec!["v".into()], rows: vec![vec![Value::List(vec![])]] };
    let err = payload.rows_as::<TimeField>().unwrap_err();
    if let RowConversionError::TypeMismatch { expected, got, .. } = err {
        assert_eq!(expected, "NaiveTime");
        assert_eq!(got, "List");
    } else { panic!("expected TypeMismatch") }
}

#[derive(Debug, FromGlueRow)]
struct IntervalField { v: gluesql::core::data::Interval }

#[test]
fn got_u64_expected_interval() {
    let payload = Payload::Select { labels: vec!["v".into()], rows: vec![vec![Value::U64(1)]] };
    let err = payload.rows_as::<IntervalField>().unwrap_err();
    if let RowConversionError::TypeMismatch { expected, got, .. } = err {
        assert_eq!(expected, "Interval");
        assert_eq!(got, "U64");
    } else { panic!("expected TypeMismatch") }
}

#[derive(Debug, FromGlueRow)]
struct MapField { v: std::collections::BTreeMap<String, Value> }

#[test]
fn got_bool_expected_map() {
    let payload = Payload::Select { labels: vec!["v".into()], rows: vec![vec![Value::Bool(true)]] };
    let err = payload.rows_as::<MapField>().unwrap_err();
    if let RowConversionError::TypeMismatch { expected, got, .. } = err {
        assert_eq!(expected, "BTreeMap<String, Value>");
        assert_eq!(got, "Bool");
    } else { panic!("expected TypeMismatch") }
}

#[derive(Debug, FromGlueRow)]
struct ListField { v: Vec<Value> }

#[test]
fn got_bool_expected_list() {
    let payload = Payload::Select { labels: vec!["v".into()], rows: vec![vec![Value::Bool(false)]] };
    let err = payload.rows_as::<ListField>().unwrap_err();
    if let RowConversionError::TypeMismatch { expected, got, .. } = err {
        assert_eq!(expected, "Vec<Value>");
        assert_eq!(got, "Bool");
    } else { panic!("expected TypeMismatch") }
}

#[derive(Debug, FromGlueRow)]
struct PointOptField { v: Option<gluesql::core::data::Point> }

#[test]
fn got_null_expected_point_maps_to_none() {
    let payload = Payload::Select { labels: vec!["v".into()], rows: vec![vec![Value::Null]] };
    let rows: Vec<PointOptField> = payload.rows_as::<PointOptField>().unwrap();
    assert_eq!(rows[0].v, None);
}
