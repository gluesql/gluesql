use {
    super::{context::RowContext, filter::check_expr},
    crate::{
        ast::IndexOperator,
        data::{Key, Row, SCHEMALESS_DOC_COLUMN, Value},
        plan::ExprPlan,
        result::Result,
        store::{GStore, RowIter},
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

#[cfg(feature = "tracing")]
fn trace_access_path(access_path: &'static str) {
    tracing::debug!(target: "gluesql", access_path, "selected query access path");
}

#[cfg_attr(
    feature = "tracing",
    tracing::instrument(
        name = "gluesql.storage.scan_data",
        target = "gluesql",
        level = "trace",
        skip_all
    )
)]
pub(crate) fn trace_scan<'a, T: GStore>(storage: &'a T, table_name: &str) -> Result<RowIter<'a>> {
    #[cfg(feature = "tracing")]
    trace_access_path("full_scan");
    storage.scan_data(table_name)
}

#[cfg_attr(
    feature = "tracing",
    tracing::instrument(
        name = "gluesql.storage.scan_indexed_data",
        target = "gluesql",
        level = "trace",
        skip_all
    )
)]
pub(crate) fn trace_index_scan<'a, T: GStore>(
    storage: &'a T,
    table_name: &str,
    index_name: &str,
    asc: Option<bool>,
    cmp_value: Option<(&IndexOperator, Value)>,
) -> Result<RowIter<'a>> {
    #[cfg(feature = "tracing")]
    trace_access_path("secondary_index");
    storage.scan_indexed_data(table_name, index_name, asc, cmp_value)
}

#[cfg_attr(
    feature = "tracing",
    tracing::instrument(
        name = "gluesql.storage.fetch_data",
        target = "gluesql",
        level = "trace",
        skip_all
    )
)]
pub(crate) fn trace_fetch<T: GStore>(
    storage: &T,
    table_name: &str,
    key: &Key,
) -> Result<Option<Vec<Value>>> {
    #[cfg(feature = "tracing")]
    trace_access_path("primary_key");
    storage.fetch_data(table_name, key)
}

pub fn fetch<'a, T: GStore>(
    storage: &'a T,
    table_name: &'a str,
    columns: Rc<[String]>,
    where_clause: Option<&'a ExprPlan>,
) -> Result<KeyedRows<'a>> {
    let rows = trace_scan(storage, table_name)?.filter_map(move |row| {
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
