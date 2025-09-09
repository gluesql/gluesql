use gluesql::{
    FromGlueRow,
    core::{
        data::Value,
        executor::Payload,
        row_conversion::{RowConversionError, SelectExt},
    },
};

#[allow(dead_code)]
#[derive(Debug, FromGlueRow)]
struct NeedsName {
    id: i64,
    name: String,
}

#[test]
fn missing_column() {
    let payload = Payload::Select {
        labels: vec!["id".into()], // name missing
        rows: vec![vec![Value::I64(1)]],
    };
    let err = payload.rows_as::<NeedsName>().unwrap_err();
    assert!(matches!(err, RowConversionError::MissingColumn { .. }));
}

#[allow(dead_code)]
#[derive(Debug, FromGlueRow)]
struct NonNullable {
    id: i64,
    name: String, // name cannot be Null
}

#[test]
fn null_not_allowed() {
    let payload = Payload::Select {
        labels: vec!["id".into(), "name".into()],
        rows: vec![vec![Value::I64(1), Value::Null]],
    };
    let err = payload.rows_as::<NonNullable>().unwrap_err();
    assert!(matches!(err, RowConversionError::NullNotAllowed { .. }));
}

#[allow(dead_code)]
#[derive(Debug, FromGlueRow)]
struct Price {
    price: f64,
}

#[test]
fn type_mismatch() {
    let payload = Payload::Select {
        labels: vec!["price".into()],
        rows: vec![vec![Value::Str("9.99".into())]], // string -> f64 forbidden
    };
    let err = payload.rows_as::<Price>().unwrap_err();
    assert!(matches!(err, RowConversionError::TypeMismatch { .. }));
}

#[derive(Debug, PartialEq, FromGlueRow)]
struct User {
    id: i64,
    name: String,
    email: Option<String>,
}

#[test]
fn not_select_payload() {
    let payload = Payload::Insert(1);
    let err = payload.rows_as::<User>().unwrap_err();
    assert!(matches!(err, RowConversionError::NotSelectPayload));
}

#[test]
fn one_as_not_found() {
    let payload = Payload::Select {
        labels: vec!["id".into()],
        rows: vec![],
    };
    let err = payload.one_as::<User>().unwrap_err();
    assert!(matches!(err, RowConversionError::NotFound));
}

#[test]
fn one_as_more_than_one() {
    let payload = Payload::Select {
        labels: vec!["id".into(), "name".into(), "email".into()],
        rows: vec![
            vec![Value::I64(1), Value::Str("A".into()), Value::Null],
            vec![Value::I64(2), Value::Str("B".into()), Value::Null],
        ],
    };
    let err = payload.one_as::<User>().unwrap_err();
    assert!(matches!(err, RowConversionError::MoreThanOneRow { .. }));
}

#[test]
fn one_as_not_select_payload_on_payload() {
    let payload = Payload::Insert(1);
    let err = payload.one_as::<User>().unwrap_err();
    assert!(matches!(err, RowConversionError::NotSelectPayload));
}
