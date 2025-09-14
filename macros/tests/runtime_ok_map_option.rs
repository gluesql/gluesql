use gluesql::{
    FromGlueRow,
    core::{data::Value, row_conversion::FromGlueRow as _},
};

#[derive(Debug, PartialEq, FromGlueRow)]
struct BMOptI64 {
    v: std::collections::BTreeMap<String, Option<i64>>,
}

#[test]
fn btreemap_option_i64_rows_as_and_direct() {
    use gluesql::core::{executor::Payload, row_conversion::SelectExt};
    use std::collections::BTreeMap;

    let mut m = BTreeMap::new();
    m.insert("a".to_string(), Value::I64(1));
    m.insert("b".to_string(), Value::Null);

    // rows_as (by_idx path)
    let payload = Payload::Select {
        labels: vec!["v".into()],
        rows: vec![vec![Value::Map(m.clone())]],
    };
    let rows: Vec<BMOptI64> = payload.rows_as::<BMOptI64>().unwrap();
    let mut expected = BTreeMap::new();
    expected.insert("a".to_string(), Some(1));
    expected.insert("b".to_string(), None);
    assert_eq!(rows[0].v, expected);

    // from_glue_row (by_ref path)
    let labels = vec!["v".to_string()];
    let row = vec![Value::Map(m)];
    let got = BMOptI64::from_glue_row(&labels, &row).unwrap();
    assert_eq!(got.v, expected);
}

#[derive(Debug, PartialEq, FromGlueRow)]
struct HMOptStr {
    v: std::collections::HashMap<String, Option<String>>,
}

#[test]
fn hashmap_option_string_rows_as_and_direct() {
    use gluesql::core::{executor::Payload, row_conversion::SelectExt};
    use std::collections::BTreeMap;

    let mut m = BTreeMap::new();
    m.insert("x".to_string(), Value::Str("s".into()));
    m.insert("y".to_string(), Value::Null);

    let payload = Payload::Select {
        labels: vec!["v".into()],
        rows: vec![vec![Value::Map(m.clone())]],
    };
    let rows: Vec<HMOptStr> = payload.rows_as::<HMOptStr>().unwrap();
    let mut expected = std::collections::HashMap::new();
    expected.insert("x".to_string(), Some("s".to_string()));
    expected.insert("y".to_string(), None);
    assert_eq!(rows[0].v, expected);

    let labels = vec!["v".to_string()];
    let row = vec![Value::Map(m)];
    let got = HMOptStr::from_glue_row(&labels, &row).unwrap();
    assert_eq!(got.v, expected);
}

#[derive(Debug, PartialEq, FromGlueRow)]
struct HMValue {
    v: std::collections::HashMap<String, Value>,
}

#[test]
fn hashmap_value_rows_as_and_direct() {
    use gluesql::core::{executor::Payload, row_conversion::SelectExt};
    use std::collections::BTreeMap;

    let mut m = BTreeMap::new();
    m.insert("x".to_string(), Value::I64(10));
    m.insert("y".to_string(), Value::Str("a".into()));

    let payload = Payload::Select {
        labels: vec!["v".into()],
        rows: vec![vec![Value::Map(m.clone())]],
    };
    let rows: Vec<HMValue> = payload.rows_as::<HMValue>().unwrap();
    let mut expected = std::collections::HashMap::new();
    expected.insert("x".to_string(), Value::I64(10));
    expected.insert("y".to_string(), Value::Str("a".into()));
    assert_eq!(rows[0].v, expected);

    let labels = vec!["v".to_string()];
    let row = vec![Value::Map(m)];
    let got = HMValue::from_glue_row(&labels, &row).unwrap();
    assert_eq!(got.v, expected);
}
