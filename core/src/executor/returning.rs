use {
    super::{context::RowContext, evaluate::evaluate},
    crate::{
        data::{Row, Value},
        executor::execute::{ExecuteError, Payload},
        plan::SelectItemPlan,
        result::Result,
        store::GStore,
    },
    std::{borrow::Cow, rc::Rc},
};

/// Computes the labels a `RETURNING` projection produces.
///
/// Callers run this before touching the storage so that a projection naming an
/// unknown table fails without leaving the statement half-applied.
///
/// # Errors
///
/// Returns an error when a qualified star does not name the target table.
pub fn labels(
    table_name: &str,
    column_names: &[String],
    items: &[SelectItemPlan],
) -> Result<Vec<String>> {
    let mut labels = Vec::with_capacity(items.len());

    for item in items {
        match item {
            SelectItemPlan::Expr { label, .. } => labels.push(label.clone()),
            SelectItemPlan::Wildcard => labels.extend(column_names.iter().cloned()),
            SelectItemPlan::QualifiedWildcard(alias) if alias == table_name => {
                labels.extend(column_names.iter().cloned());
            }
            SelectItemPlan::QualifiedWildcard(alias) => {
                return Err(ExecuteError::ReturningTableNotFound(alias.clone()).into());
            }
        }
    }

    Ok(labels)
}

/// Builds the [`Payload::Select`] produced by a `RETURNING` clause.
///
/// Each row in `rows` holds the values of every table column, ordered as in
/// `column_names`. For `INSERT` and `UPDATE` these are the values about to be
/// stored, for `DELETE` the values of the row about to be deleted. `labels`
/// comes from [`labels`].
///
/// Callers run this before handing the rows to the storage, so that a
/// projection that fails to evaluate leaves the table untouched even on a
/// storage that cannot roll a statement back.
///
/// # Errors
///
/// Returns an error when evaluating a projection expression fails.
pub fn build_payload<T: GStore>(
    storage: &T,
    table_name: &str,
    column_names: &Rc<[String]>,
    items: &[SelectItemPlan],
    labels: Vec<String>,
    rows: Vec<Vec<Value>>,
) -> Result<Payload> {
    let rows = rows.into_iter().map(|values| (values, None)).collect();

    build_payload_with(storage, table_name, column_names, items, labels, rows, None)
}

/// Like [`build_payload`], but every result row may carry an extra row
/// reachable in the projection under `extra_alias`. `UPDATE ... FROM` uses this
/// to expose the matched source row's columns. It runs before the storage
/// mutation for the same reason [`build_payload`] does.
///
/// # Errors
///
/// Returns an error when evaluating a projection expression fails.
pub fn build_payload_with<T: GStore>(
    storage: &T,
    table_name: &str,
    column_names: &Rc<[String]>,
    items: &[SelectItemPlan],
    labels: Vec<String>,
    rows: Vec<(Vec<Value>, Option<Row>)>,
    extra_alias: Option<&str>,
) -> Result<Payload> {
    let mut result_rows = Vec::with_capacity(rows.len());

    for (values, extra_row) in rows {
        let row = Row {
            columns: Rc::clone(column_names),
            values,
        };
        let next = match (extra_alias, extra_row.as_ref()) {
            (Some(alias), Some(extra_row)) => Some(Rc::new(RowContext::new(
                alias,
                Cow::Borrowed(extra_row),
                None,
            ))),
            _ => None,
        };
        let context = Rc::new(RowContext::new(table_name, Cow::Borrowed(&row), next));

        let mut result_row = Vec::with_capacity(labels.len());
        for item in items {
            match item {
                SelectItemPlan::Expr { expr, .. } => {
                    let value: Value = evaluate(storage, Some(&context), None, expr)?.try_into()?;

                    result_row.push(value);
                }
                SelectItemPlan::Wildcard | SelectItemPlan::QualifiedWildcard(_) => {
                    result_row.extend(row.values.iter().cloned());
                }
            }
        }

        result_rows.push(result_row);
    }

    Ok(Payload::Select {
        labels,
        rows: result_rows,
    })
}
