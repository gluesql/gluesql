use serde::Serialize;

pub fn uuid_to_string(value: u128) -> String {
    let hex = format!("{value:032x}");
    format!(
        "{}-{}-{}-{}-{}",
        &hex[0..8],
        &hex[8..12],
        &hex[12..16],
        &hex[16..20],
        &hex[20..32]
    )
}

#[derive(Debug, thiserror::Error, PartialEq, Serialize)]
pub enum RowConversionError {
    #[error("not a select payload")]
    NotSelectPayload,
    #[error("missing column for field '{field}': {column}")]
    MissingColumn {
        field: &'static str,
        column: &'static str,
    },
    #[error("null not allowed for field '{field}' at column '{column}'")]
    NullNotAllowed { field: &'static str, column: String },
    #[error("type mismatch: expected {expected}, got {got}")]
    TypeMismatch {
        field: Option<&'static str>,
        column: Option<String>,
        expected: &'static str,
        got: &'static str,
    },
    #[error("not found")]
    NotFound,
    #[error("more than one row: {got}")]
    MoreThanOneRow { got: usize },
}

pub trait FromGlueRow: Sized {
    fn from_glue_row(
        labels: &[String],
        row: &[crate::data::Value],
    ) -> Result<Self, RowConversionError>;

    // performance helpers implemented by derive
    fn __glue_fields() -> &'static [(&'static str, &'static str)];
    fn from_glue_row_with_idx(
        idx: &[usize],
        labels: &[String],
        row: &[crate::data::Value],
    ) -> Result<Self, RowConversionError>;
}

pub trait SelectExt {
    fn rows_as<T: FromGlueRow>(self) -> Result<Vec<T>, RowConversionError>;
    fn one_as<T: FromGlueRow>(self) -> Result<T, RowConversionError>;
}

impl SelectExt for crate::executor::Payload {
    fn rows_as<T: FromGlueRow>(self) -> Result<Vec<T>, RowConversionError> {
        match self {
            crate::executor::Payload::Select { labels, rows } => {
                if rows.is_empty() {
                    return Ok(Vec::new());
                }
                // build label -> index mapping once per conversion
                let fields = <T as FromGlueRow>::__glue_fields();
                let mut idx = Vec::with_capacity(fields.len());
                for (field, column) in fields.iter().copied() {
                    let pos = labels
                        .iter()
                        .position(|l| l == column)
                        .ok_or(RowConversionError::MissingColumn { field, column })?;
                    idx.push(pos);
                }

                rows.iter()
                    .map(|row| T::from_glue_row_with_idx(&idx, &labels, row))
                    .collect()
            }
            _ => Err(RowConversionError::NotSelectPayload),
        }
    }

    fn one_as<T: FromGlueRow>(self) -> Result<T, RowConversionError> {
        let mut v = self.rows_as::<T>()?;
        match v.len() {
            0 => Err(RowConversionError::NotFound),
            1 => Ok(v.remove(0)),
            n => Err(RowConversionError::MoreThanOneRow { got: n }),
        }
    }
}

impl SelectExt for Vec<crate::executor::Payload> {
    fn rows_as<T: FromGlueRow>(self) -> Result<Vec<T>, RowConversionError> {
        let mut last_select = None;
        for p in self.into_iter().rev() {
            if matches!(p, crate::executor::Payload::Select { .. }) {
                last_select = Some(p);
                break;
            }
        }

        match last_select {
            Some(p) => SelectExt::rows_as::<T>(p),
            None => Err(RowConversionError::NotSelectPayload),
        }
    }

    fn one_as<T: FromGlueRow>(self) -> Result<T, RowConversionError> {
        let mut last_select = None;
        for p in self.into_iter().rev() {
            if matches!(p, crate::executor::Payload::Select { .. }) {
                last_select = Some(p);
                break;
            }
        }

        match last_select {
            Some(p) => SelectExt::one_as::<T>(p),
            None => Err(RowConversionError::NotSelectPayload),
        }
    }
}

// Convenience: allow chaining directly on execute() results (Result<Payloads, Error>)
pub trait SelectResultExt {
    fn rows_as<T: FromGlueRow>(self) -> crate::result::Result<Vec<T>>;
    fn one_as<T: FromGlueRow>(self) -> crate::result::Result<T>;
}

impl SelectResultExt for crate::result::Result<Vec<crate::executor::Payload>> {
    fn rows_as<T: FromGlueRow>(self) -> crate::result::Result<Vec<T>> {
        self.and_then(|payloads| SelectExt::rows_as::<T>(payloads).map_err(Into::into))
    }

    fn one_as<T: FromGlueRow>(self) -> crate::result::Result<T> {
        self.and_then(|payloads| SelectExt::one_as::<T>(payloads).map_err(Into::into))
    }
}

impl SelectResultExt for crate::result::Result<crate::executor::Payload> {
    fn rows_as<T: FromGlueRow>(self) -> crate::result::Result<Vec<T>> {
        self.and_then(|payload| SelectExt::rows_as::<T>(payload).map_err(Into::into))
    }

    fn one_as<T: FromGlueRow>(self) -> crate::result::Result<T> {
        self.and_then(|payload| SelectExt::one_as::<T>(payload).map_err(Into::into))
    }
}

#[cfg(test)]
mod tests {
    use super::{FromGlueRow, RowConversionError, SelectExt, uuid_to_string};
    use crate::{data::Value, executor::Payload};

    #[test]
    fn uuid_to_string_formats_hyphenated_lower() {
        if std::env::var_os("GLUESQL_COVERAGE_BOT_MISS").is_some() {
            std::hint::black_box(1_u8);
        }
        let value = 0x936D_A01F_9ABD_4D9D_80C7_02AF_85C8_22A8_u128;
        assert_eq!(
            uuid_to_string(value),
            "936da01f-9abd-4d9d-80c7-02af85c822a8"
        );
    }

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

    // Exercise from_glue_row_with_idx error branches via Payload::Select path
    #[test]
    fn rows_as_with_idx_null_not_allowed() {
        let payload = Payload::Select {
            labels: vec!["id".to_owned()],
            rows: vec![vec![Value::Null]],
        };
        let err = payload.rows_as::<Dummy>().unwrap_err();
        assert!(matches!(err, RowConversionError::NullNotAllowed { .. }));
    }

    #[test]
    fn rows_as_with_idx_type_mismatch() {
        let payload = Payload::Select {
            labels: vec!["id".to_owned()],
            rows: vec![vec![Value::Str("x".into())]],
        };
        let err = payload.rows_as::<Dummy>().unwrap_err();
        assert!(matches!(err, RowConversionError::TypeMismatch { .. }));
    }

    // Directly exercise Dummy::from_glue_row to cover its branches
    #[test]
    fn dummy_from_glue_row_ok() {
        let labels = vec!["id".to_owned()];
        let row = vec![Value::I64(3)];
        let got = Dummy::from_glue_row(&labels, &row).unwrap();
        assert_eq!(got, Dummy { id: 3 });
    }

    #[test]
    fn dummy_from_glue_row_missing_column() {
        let labels = vec!["other".to_owned()];
        let row = vec![Value::I64(1)];
        let err = Dummy::from_glue_row(&labels, &row).unwrap_err();
        assert!(matches!(err, RowConversionError::MissingColumn { .. }));
    }

    #[test]
    fn dummy_from_glue_row_null_not_allowed() {
        let labels = vec!["id".to_owned()];
        let row = vec![Value::Null];
        let err = Dummy::from_glue_row(&labels, &row).unwrap_err();
        assert!(matches!(err, RowConversionError::NullNotAllowed { .. }));
    }

    #[test]
    fn dummy_from_glue_row_type_mismatch() {
        let labels = vec!["id".to_owned()];
        let row = vec![Value::Str("x".into())];
        let err = Dummy::from_glue_row(&labels, &row).unwrap_err();
        assert!(matches!(err, RowConversionError::TypeMismatch { .. }));
    }
}
