use {
    super::{
        Payload, Referencing,
        fetch::{fetch, fetch_columns},
    },
    crate::{
        ast::{BinaryOperator, ForeignKey, ReferentialAction},
        plan::ExprPlan,
        result::Result,
        store::{GStore, GStoreMut},
    },
    serde::Serialize,
    std::sync::Arc,
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
    selection: Option<&ExprPlan>,
) -> Result<Payload> {
    let columns = Arc::from(fetch_columns(storage, table_name)?);
    let referencings = storage.fetch_referencings(table_name)?;
    let mut keys = Vec::new();
    for item in fetch(storage, table_name, columns, selection)? {
        let (key, row) = item?;

        for Referencing {
            table_name: referencing_table_name,
            foreign_key:
                ForeignKey {
                    referencing_column_name,
                    referenced_column_name,
                    on_delete,
                    ..
                },
        } in &referencings
        {
            let value = row
                .get_value(referenced_column_name)
                .ok_or(DeleteError::ValueNotFound(referenced_column_name.clone()))?
                .clone();

            let expr = &ExprPlan::BinaryOp {
                left: Box::new(ExprPlan::Identifier(referencing_column_name.clone())),
                op: BinaryOperator::Eq,
                right: Box::new(ExprPlan::Value(value)),
            };

            let columns = Arc::from(fetch_columns(storage, referencing_table_name)?);
            let mut referencing_rows = fetch(storage, referencing_table_name, columns, Some(expr))?;

            let referencing_row_exists = referencing_rows.next().transpose()?.is_some();
            if referencing_row_exists && on_delete == &ReferentialAction::NoAction {
                return Err(DeleteError::ReferencingColumnExists(format!(
                    "{referencing_table_name}.{referencing_column_name}"
                ))
                .into());
            }
        }

        keys.push(key);
    }
    let num_keys = keys.len();

    storage
        .delete_data(table_name, keys)
        .map(|()| Payload::Delete(num_keys))
}
