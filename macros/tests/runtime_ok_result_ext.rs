use gluesql::{
    FromGlueRow,
    core::{
        data::Value,
        error::Error,
        executor::Payload,
        row_conversion::SelectResultExt,
    },
};

#[derive(Debug, PartialEq, FromGlueRow)]
struct User {
    id: i64,
}

#[test]
fn result_rows_as_ok() {
    let res: Result<Vec<Payload>, Error> = Ok(vec![Payload::Select {
        labels: vec!["id".into()],
        rows: vec![vec![Value::I64(1)]],
    }]);

    let rows: Vec<User> = res.rows_as::<User>().unwrap();
    assert_eq!(rows, vec![User { id: 1 }]);
}

#[test]
fn result_rows_as_not_select_payload_maps_error() {
    use gluesql::core::error::Error as CoreError;
    use gluesql::core::row_conversion::RowConversionError;

    let res: Result<Vec<Payload>, Error> = Ok(vec![Payload::Insert(1)]);
    let err = res.rows_as::<User>().unwrap_err();
    match err {
        CoreError::RowConversion(RowConversionError::NotSelectPayload) => {}
        other => panic!("unexpected error: {other:?}"),
    }
}

