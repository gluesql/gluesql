use gluesql::{
    FromGlueRow,
    core::{data::Value, executor::Payload, row_conversion::SelectExt},
};

#[derive(Debug, PartialEq, FromGlueRow)]
struct VecVecI64 {
    v: Vec<Vec<i64>>,
}

#[test]
fn list_of_list_to_vec_vec_i64() {
    let payload = Payload::Select {
        labels: vec!["v".into()],
        rows: vec![vec![Value::List(vec![
            Value::List(vec![Value::I64(1), Value::I64(2)]),
            Value::List(vec![Value::I64(3)]),
        ])]],
    };

    let rows: Vec<VecVecI64> = payload.rows_as::<VecVecI64>().unwrap();
    assert_eq!(rows[0].v, vec![vec![1, 2], vec![3]]);
}

#[derive(Debug, PartialEq, FromGlueRow)]
struct VecMapI64 {
    v: Vec<std::collections::BTreeMap<String, i64>>,
}

#[test]
fn list_of_map_to_vec_btreemap() {
    use std::collections::BTreeMap;
    let mut m1 = BTreeMap::new();
    m1.insert("a".to_string(), Value::I64(10));
    let mut m2 = BTreeMap::new();
    m2.insert("b".to_string(), Value::I64(20));

    let payload = Payload::Select {
        labels: vec!["v".into()],
        rows: vec![vec![Value::List(vec![Value::Map(m1), Value::Map(m2)])]],
    };

    let rows: Vec<VecMapI64> = payload.rows_as::<VecMapI64>().unwrap();
    let mut expected1 = std::collections::BTreeMap::new();
    expected1.insert("a".to_string(), 10);
    let mut expected2 = std::collections::BTreeMap::new();
    expected2.insert("b".to_string(), 20);
    assert_eq!(rows[0].v, vec![expected1, expected2]);
}

#[derive(Debug, PartialEq, FromGlueRow)]
struct MapVecStr {
    v: std::collections::HashMap<String, Vec<String>>,
}

#[test]
fn map_to_hashmap_vec_string() {
    use std::collections::BTreeMap;
    let mut m = BTreeMap::new();
    m.insert(
        "x".to_string(),
        Value::List(vec![Value::Str("a".into()), Value::Str("b".into())]),
    );
    m.insert("y".to_string(), Value::List(vec![]));

    let payload = Payload::Select {
        labels: vec!["v".into()],
        rows: vec![vec![Value::Map(m)]],
    };

    let rows: Vec<MapVecStr> = payload.rows_as::<MapVecStr>().unwrap();
    let mut expected = std::collections::HashMap::new();
    expected.insert("x".to_string(), vec!["a".to_string(), "b".to_string()]);
    expected.insert("y".to_string(), vec![]);
    assert_eq!(rows[0].v, expected);
}

#[derive(Debug, PartialEq, FromGlueRow)]
struct VecOptI64 {
    v: Vec<Option<i64>>,
}

#[test]
fn list_of_optional_i64() {
    let payload = Payload::Select {
        labels: vec!["v".into()],
        rows: vec![vec![Value::List(vec![
            Value::I64(1),
            Value::Null,
            Value::I64(3),
        ])]],
    };

    let rows: Vec<VecOptI64> = payload.rows_as::<VecOptI64>().unwrap();
    assert_eq!(rows[0].v, vec![Some(1), None, Some(3)]);
}
