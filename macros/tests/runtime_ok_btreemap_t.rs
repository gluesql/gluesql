use gluesql::{
    FromGlueRow,
    core::{data::Value, executor::Payload, row_conversion::SelectExt},
};

#[derive(Debug, PartialEq, FromGlueRow)]
struct MapStr {
    v: std::collections::BTreeMap<String, String>,
}

#[test]
fn map_of_str_to_btreemap_string_string() {
    let mut m = std::collections::BTreeMap::new();
    m.insert("a".to_string(), Value::Str("1".into()));
    m.insert("b".to_string(), Value::Str("2".into()));

    let payload = Payload::Select {
        labels: vec!["v".into()],
        rows: vec![vec![Value::Map(m)]],
    };

    let rows: Vec<MapStr> = payload.rows_as::<MapStr>().unwrap();
    let mut expected = std::collections::BTreeMap::new();
    expected.insert("a".to_string(), "1".to_string());
    expected.insert("b".to_string(), "2".to_string());
    assert_eq!(rows[0].v, expected);
}

#[derive(Debug, PartialEq, FromGlueRow)]
struct MapI64 {
    v: std::collections::BTreeMap<String, i64>,
}

#[test]
fn map_of_i64_to_btreemap_string_i64() {
    let mut m = std::collections::BTreeMap::new();
    m.insert("x".to_string(), Value::I64(10));
    m.insert("y".to_string(), Value::I64(20));

    let payload = Payload::Select {
        labels: vec!["v".into()],
        rows: vec![vec![Value::Map(m)]],
    };

    let rows: Vec<MapI64> = payload.rows_as::<MapI64>().unwrap();
    let mut expected = std::collections::BTreeMap::new();
    expected.insert("x".to_string(), 10);
    expected.insert("y".to_string(), 20);
    assert_eq!(rows[0].v, expected);
}
