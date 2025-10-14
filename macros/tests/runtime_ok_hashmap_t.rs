use {
    gluesql_core::{data::Value, executor::Payload, row_conversion::SelectExt},
    gluesql_macros::FromGlueRow,
};

#[derive(Debug, PartialEq, FromGlueRow)]
struct HMapStr {
    v: std::collections::HashMap<String, String>,
}

#[test]
fn map_of_str_to_hashmap_string_string() {
    use std::collections::BTreeMap;
    let mut m = BTreeMap::new();
    m.insert("a".to_string(), Value::Str("1".into()));
    m.insert("b".to_string(), Value::Str("2".into()));

    let payload = Payload::Select {
        labels: vec!["v".into()],
        rows: vec![vec![Value::Map(m)]],
    };

    let rows: Vec<HMapStr> = payload.rows_as::<HMapStr>().unwrap();
    let mut expected = std::collections::HashMap::new();
    expected.insert("a".to_string(), "1".to_string());
    expected.insert("b".to_string(), "2".to_string());
    assert_eq!(rows[0].v, expected);
}

#[derive(Debug, PartialEq, FromGlueRow)]
struct HMapI64 {
    v: std::collections::HashMap<String, i64>,
}

#[test]
fn map_of_i64_to_hashmap_string_i64() {
    use std::collections::BTreeMap;
    let mut m = BTreeMap::new();
    m.insert("x".to_string(), Value::I64(10));
    m.insert("y".to_string(), Value::I64(20));

    let payload = Payload::Select {
        labels: vec!["v".into()],
        rows: vec![vec![Value::Map(m)]],
    };

    let rows: Vec<HMapI64> = payload.rows_as::<HMapI64>().unwrap();
    let mut expected = std::collections::HashMap::new();
    expected.insert("x".to_string(), 10);
    expected.insert("y".to_string(), 20);
    assert_eq!(rows[0].v, expected);
}
