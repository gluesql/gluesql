// Purposefully avoid importing Value/Payload to allow re-exports below without name clashes.
use serde::Serialize;

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
        let rows = self.rows_as::<T>();
        match rows {
            Err(RowConversionError::NotSelectPayload) => Err(RowConversionError::NotSelectPayload),
            Err(e) => Err(e),
            Ok(mut v) => match v.len() {
                0 => Err(RowConversionError::NotFound),
                1 => Ok(v.remove(0)),
                n => Err(RowConversionError::MoreThanOneRow { got: n }),
            },
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
        match self {
            Ok(payloads) => SelectExt::rows_as::<T>(payloads).map_err(Into::into),
            Err(e) => Err(e),
        }
    }

    fn one_as<T: FromGlueRow>(self) -> crate::result::Result<T> {
        match self {
            Ok(payloads) => SelectExt::one_as::<T>(payloads).map_err(Into::into),
            Err(e) => Err(e),
        }
    }
}

impl SelectResultExt for crate::result::Result<crate::executor::Payload> {
    fn rows_as<T: FromGlueRow>(self) -> crate::result::Result<Vec<T>> {
        match self {
            Ok(payload) => SelectExt::rows_as::<T>(payload).map_err(Into::into),
            Err(e) => Err(e),
        }
    }

    fn one_as<T: FromGlueRow>(self) -> crate::result::Result<T> {
        match self {
            Ok(payload) => SelectExt::one_as::<T>(payload).map_err(Into::into),
            Err(e) => Err(e),
        }
    }
}
