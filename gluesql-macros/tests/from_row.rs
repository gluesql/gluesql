use gluesql_macros::FromRow;
use gluesql_core::{data::{Row, Value}, FromRow as FromRowTrait};
use std::collections::HashMap;

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
