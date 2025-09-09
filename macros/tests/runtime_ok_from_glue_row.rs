use gluesql::{
    FromGlueRow,
    core::{data::Value, row_conversion::FromGlueRow as _},
};

#[derive(Debug, PartialEq, FromGlueRow)]
struct SingleField {
    id: i64,
}

#[test]
fn direct_from_glue_row_ok() {
    let labels = vec!["id".to_string()];
    let row = vec![Value::I64(10)];
    let v = SingleField::from_glue_row(&labels, &row).unwrap();
    assert_eq!(v, SingleField { id: 10 });
}

#[derive(Debug, PartialEq, FromGlueRow)]
struct DirectRename {
    #[glue(rename = "order_id")]
    id: i64,
}

#[test]
fn direct_from_glue_row_rename_ok() {
    let labels = vec!["order_id".to_string()];
    let row = vec![Value::I64(77)];
    let v = DirectRename::from_glue_row(&labels, &row).unwrap();
    assert_eq!(v, DirectRename { id: 77 });
}

#[allow(dead_code)]
#[derive(Debug, FromGlueRow)]
struct DirectNonNull {
    v: String,
}

#[test]
fn direct_from_glue_row_null_not_allowed() {
    use gluesql::core::row_conversion::RowConversionError;
    let labels = vec!["v".to_string()];
    let row = vec![Value::Null];
    let err = DirectNonNull::from_glue_row(&labels, &row).unwrap_err();
    assert!(matches!(err, RowConversionError::NullNotAllowed { .. }));
}

#[allow(dead_code)]
#[derive(Debug, FromGlueRow)]
struct DirectMismatch {
    v: bool,
}

#[test]
fn direct_from_glue_row_type_mismatch() {
    use gluesql::core::row_conversion::RowConversionError;
    let labels = vec!["v".to_string()];
    let row = vec![Value::I64(1)];
    let err = DirectMismatch::from_glue_row(&labels, &row).unwrap_err();
    if let RowConversionError::TypeMismatch { expected, got, .. } = err {
        assert_eq!(expected, "bool");
        assert_eq!(got, "I64");
    } else {
        panic!("expected TypeMismatch");
    }
}
