use {
    super::{
        Payload, Referencing,
        context::RowContext,
        execute::{ExecuteError, fetch_source_rows},
        fetch::{fetch, fetch_columns},
        filter::check_expr,
        returning,
    },
    crate::{
        ast::{BinaryOperator, ForeignKey, ReferentialAction, SourceTable},
        plan::{ExprPlan, SelectItemPlan},
        result::Result,
        store::{GStore, GStoreMut},
    },
    serde::Serialize,
    std::{borrow::Cow, rc::Rc},
    thiserror::Error as ThisError,
};

#[derive(ThisError, Serialize, Debug, PartialEq, Eq)]
pub enum DeleteError {
    #[error("referencing column exists: {0}")]
    ReferencingColumnExists(String),

    #[error("Value not found on column: {0}")]
    ValueNotFound(String),
}

pub fn delete<T: GStore + GStoreMut>(
    storage: &mut T,
    table_name: &str,
    using: Option<&SourceTable>,
    selection: Option<&ExprPlan>,
    returning: Option<&[SelectItemPlan]>,
) -> Result<Payload> {
    let has_schema = storage
        .fetch_schema(table_name)?
        .is_some_and(|schema| schema.column_defs.is_some());

    if returning.is_some() && !has_schema {
        return Err(ExecuteError::ReturningOnSchemalessTable(table_name.to_owned()).into());
    }

    let source = match using {
        Some(source_table) => {
            if !has_schema {
                return Err(ExecuteError::SourceRequiresSchema(table_name.to_owned()).into());
            }
            let source_rows = fetch_source_rows(storage, &source_table.name)?;
            let alias = source_table
                .alias
                .as_deref()
                .unwrap_or(source_table.name.as_str());

            Some((alias, source_rows))
        }
        None => None,
    };

    let columns: Rc<[String]> = Rc::from(fetch_columns(storage, table_name)?);
    let labels = returning
        .map(|items| returning::labels(table_name, &columns, items))
        .transpose()?;
    let referencings = storage
        .fetch_referencings(table_name)?
        .into_iter()
        .map(|referencing| {
            fetch_columns(storage, &referencing.table_name)
                .map(Rc::from)
                .map(|columns| (referencing, columns))
        })
        .collect::<Result<Vec<_>>>()?;

    let fetch_selection = source.is_none().then_some(selection).flatten();

    let mut keys = Vec::new();
    let mut deleted_rows = Vec::new();
    for item in fetch(storage, table_name, Rc::clone(&columns), fetch_selection)? {
        let (key, row) = item?;

        if let Some((alias, source_rows)) = &source {
            let mut matched = false;
            for source_row in source_rows {
                let next = Rc::new(RowContext::new(alias, Cow::Borrowed(source_row), None));
                let context = Rc::new(RowContext::new(table_name, Cow::Borrowed(&row), Some(next)));
                let passes = match selection {
                    Some(expr) => check_expr(storage, Some(&context), None, expr)?,
                    None => true,
                };

                if passes {
                    matched = true;
                    break;
                }
            }

            if !matched {
                continue;
            }
        }

        for (referencing, columns) in &referencings {
            let Referencing {
                table_name: referencing_table_name,
                foreign_key:
                    ForeignKey {
                        referencing_column_name,
                        referenced_column_name,
                        on_delete,
                        ..
                    },
            } = referencing;

            let value = row
                .get_value(referenced_column_name)
                .ok_or(DeleteError::ValueNotFound(referenced_column_name.clone()))?
                .clone();

            let expr = &ExprPlan::BinaryOp {
                left: Box::new(ExprPlan::Identifier(referencing_column_name.clone())),
                op: BinaryOperator::Eq,
                right: Box::new(ExprPlan::Value(value)),
            };

            let mut referencing_rows = fetch(
                storage,
                referencing_table_name,
                Rc::clone(columns),
                Some(expr),
            )?;

            let referencing_row_exists = referencing_rows.next().transpose()?.is_some();
            if referencing_row_exists && on_delete == &ReferentialAction::NoAction {
                return Err(DeleteError::ReferencingColumnExists(format!(
                    "{referencing_table_name}.{referencing_column_name}"
                ))
                .into());
            }
        }

        keys.push(key);
        if returning.is_some() {
            deleted_rows.push(row.values);
        }
    }
    let num_keys = keys.len();

    storage.delete_data(table_name, keys)?;

    match (returning, labels) {
        (Some(items), Some(labels)) => {
            returning::build_payload(storage, table_name, &columns, items, labels, deleted_rows)
        }
        _ => Ok(Payload::Delete(num_keys)),
    }
}
