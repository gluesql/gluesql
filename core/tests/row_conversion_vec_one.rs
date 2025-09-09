use gluesql_core::{
    data::Value,
    executor::Payload,
    row_conversion::{FromGlueRow, RowConversionError, SelectExt},
};

#[derive(Debug, PartialEq)]
struct Dummy {
    id: i64,
}

impl FromGlueRow for Dummy {
    fn from_glue_row(labels: &[String], row: &[Value]) -> Result<Self, RowConversionError> {
        let pos =
            labels
                .iter()
                .position(|l| l == "id")
                .ok_or(RowConversionError::MissingColumn {
                    field: "id",
                    column: "id",
                })?;
        match &row[pos] {
            Value::Null => Err(RowConversionError::NullNotAllowed {
                field: "id",
                column: labels[pos].clone(),
            }),
            Value::I64(n) => Ok(Dummy { id: *n }),
            _ => Err(RowConversionError::TypeMismatch {
                field: Some("id"),
                column: Some(labels[pos].clone()),
                expected: "i64",
                got: "<other>",
            }),
        }
    }

    fn __glue_fields() -> &'static [(&'static str, &'static str)] {
        static FIELDS: [(&str, &str); 1] = [("id", "id")];
        &FIELDS
    }

    fn from_glue_row_with_idx(
        idx: &[usize],
        labels: &[String],
        row: &[Value],
    ) -> Result<Self, RowConversionError> {
        let pos = idx[0];
        match &row[pos] {
            Value::Null => Err(RowConversionError::NullNotAllowed {
                field: "id",
                column: labels[pos].clone(),
            }),
            Value::I64(n) => Ok(Dummy { id: *n }),
            _ => Err(RowConversionError::TypeMismatch {
                field: Some("id"),
                column: Some(labels[pos].clone()),
                expected: "i64",
                got: "<other>",
            }),
        }
    }
}

#[test]
fn vec_one_as_not_select_payload() {
    let payloads = vec![Payload::Insert(1)];
    let err = payloads.one_as::<Dummy>().unwrap_err();
    assert!(matches!(err, RowConversionError::NotSelectPayload));
}

#[test]
fn vec_one_as_picks_last_select() {
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
    .one_as::<Dummy>()
    .unwrap();

    assert_eq!(rows, Dummy { id: 9 });
}
