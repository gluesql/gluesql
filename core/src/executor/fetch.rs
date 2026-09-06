use {
    super::{context::RowContext, filter::check_expr},
    crate::{
        data::{Key, Row, SCHEMALESS_DOC_COLUMN},
        plan::ExprPlan,
        result::Result,
        store::GStore,
    },
    serde::Serialize,
    std::{borrow::Cow, fmt::Debug, rc::Rc},
    thiserror::Error as ThisError,
};

pub type KeyedRows<'a> = Box<dyn Iterator<Item = Result<(Key, Row)>> + 'a>;

#[derive(ThisError, Serialize, Debug, PartialEq, Eq)]
pub enum FetchError {
    #[error("table not found: {0}")]
    TableNotFound(String),
}

pub(crate) fn trace_access_path(access_path: &'static str) {
    #[cfg(feature = "tracing")]
    tracing::debug!(target: "gluesql", access_path, "selected query access path");
    #[cfg(not(feature = "tracing"))]
    let _ = access_path;
}

pub fn fetch<'a, T: GStore>(
    storage: &'a T,
    table_name: &'a str,
    columns: Rc<[String]>,
    where_clause: Option<&'a ExprPlan>,
) -> Result<KeyedRows<'a>> {
    trace_access_path("full_scan");
    let rows = storage.scan_data(table_name)?.filter_map(move |row| {
        let (key, values) = match row {
            Ok(row) => row,
            Err(error) => return Some(Err(error)),
        };
        let row = Row {
            columns: Rc::clone(&columns),
            values,
        };

        match where_clause {
            Some(expr) => {
                let context = RowContext::new(table_name, Cow::Borrowed(&row), None);
                let context = Rc::new(context);
                match check_expr(storage, Some(&context), None, expr) {
                    Ok(true) => Some(Ok((key, row))),
                    Ok(false) => None,
                    Err(error) => Some(Err(error)),
                }
            }
            None => Some(Ok((key, row))),
        }
    });

    Ok(Box::new(rows))
}

pub fn fetch_columns<T: GStore>(storage: &T, table_name: &str) -> Result<Vec<String>> {
    let columns = storage
        .fetch_schema(table_name)?
        .ok_or_else(|| FetchError::TableNotFound(table_name.to_owned()))?
        .column_defs
        .map_or_else(
            || vec![SCHEMALESS_DOC_COLUMN.to_owned()],
            |column_defs| {
                column_defs
                    .into_iter()
                    .map(|column_def| column_def.name)
                    .collect()
            },
        );

    Ok(columns)
}
