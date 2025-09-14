use gluesql::{
    FromGlueRow,
    core::{
        data::Value,
        executor::Payload,
        row_conversion::{RowConversionError, SelectExt},
    },
};

#[derive(Debug, PartialEq, FromGlueRow)]
struct User {
    id: i64,
}

#[test]
fn vec_payload_picks_last_select_or_errors() {
    // no select at all
    let err = vec![Payload::Insert(1)].rows_as::<User>().unwrap_err();
    assert!(matches!(err, RowConversionError::NotSelectPayload));

    // first is Select, last is another Select: choose the last
    let rows = vec![
        Payload::Select {
            labels: vec!["id".into()],
            rows: vec![vec![Value::I64(1)]],
        },
        Payload::Insert(0),
        Payload::Select {
            labels: vec!["id".into()],
            rows: vec![vec![Value::I64(9)]],
        },
    ]
    .one_as::<User>()
    .unwrap();

    assert_eq!(rows, User { id: 9 });
}
