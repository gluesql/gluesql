use gluesql::{
    FromGlueRow,
    core::{data::Value, executor::Payload, row_conversion::SelectExt},
};

#[derive(Debug, PartialEq, FromGlueRow)]
struct VecStr {
    v: Vec<String>,
}

#[test]
fn list_of_str_to_vec_string() {
    let payload = Payload::Select {
        labels: vec!["v".into()],
        rows: vec![vec![Value::List(vec![
            Value::Str("a".into()),
            Value::Str("b".into()),
            Value::Str("c".into()),
        ])]],
    };

    let rows: Vec<VecStr> = payload.rows_as::<VecStr>().unwrap();
    assert_eq!(
        rows[0].v,
        vec!["a".to_string(), "b".to_string(), "c".to_string()]
    );
}

#[derive(Debug, PartialEq, FromGlueRow)]
struct VecI64 {
    v: Vec<i64>,
}

#[test]
fn list_of_i64_to_vec_i64() {
    let payload = Payload::Select {
        labels: vec!["v".into()],
        rows: vec![vec![Value::List(vec![
            Value::I64(1),
            Value::I64(2),
            Value::I64(3),
        ])]],
    };

    let rows: Vec<VecI64> = payload.rows_as::<VecI64>().unwrap();
    assert_eq!(rows[0].v, vec![1, 2, 3]);
}
