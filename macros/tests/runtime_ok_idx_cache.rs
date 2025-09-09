use gluesql::{
    FromGlueRow,
    core::{executor::Payload, row_conversion::SelectExt},
};

#[derive(Debug, PartialEq, FromGlueRow)]
struct OnlyId {
    id: i64,
}

#[test]
fn rows_as_zero_rows_returns_empty() {
    let payload = Payload::Select {
        labels: vec!["id".into()],
        rows: vec![],
    };

    let rows: Vec<OnlyId> = payload.rows_as::<OnlyId>().unwrap();
    assert!(rows.is_empty());
}
