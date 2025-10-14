use {
    gluesql_core::{data::Value, error::Error, executor::Payload, row_conversion::SelectResultExt},
    gluesql_macros::FromGlueRow,
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
    use gluesql_core::{error::Error as CoreError, row_conversion::RowConversionError};

    let res: Result<Vec<Payload>, Error> = Ok(vec![Payload::Insert(1)]);
    let err = res.rows_as::<User>().unwrap_err();
    assert!(matches!(
        err,
        CoreError::RowConversion(RowConversionError::NotSelectPayload)
    ));
}

#[test]
fn result_rows_as_err_passthrough_vec() {
    let res: Result<Vec<Payload>, Error> = Err(Error::StorageMsg("x".into()));
    let got = res.rows_as::<User>().unwrap_err();
    assert!(matches!(got, Error::StorageMsg(_)));
}

#[test]
fn result_one_as_err_passthrough_vec() {
    let res: Result<Vec<Payload>, Error> = Err(Error::StorageMsg("y".into()));
    let got = res.one_as::<User>().unwrap_err();
    assert!(matches!(got, Error::StorageMsg(_)));
}

#[test]
fn result_rows_as_err_passthrough_single() {
    let res: Result<Payload, Error> = Err(Error::StorageMsg("z".into()));
    let got = res.rows_as::<User>().unwrap_err();
    assert!(matches!(got, Error::StorageMsg(_)));
}

#[test]
fn result_one_as_err_passthrough_single() {
    let res: Result<Payload, Error> = Err(Error::StorageMsg("w".into()));
    let got = res.one_as::<User>().unwrap_err();
    assert!(matches!(got, Error::StorageMsg(_)));
}
