use {
    super::{
        Payload, Referencing,
        fetch::{fetch, fetch_columns},
    },
    crate::{
        ast::{BinaryOperator, ForeignKey, ReferentialAction},
        data::{Key, Value},
        plan::ExprPlan,
        result::Result,
        store::{GStore, GStoreMut},
    },
    serde::Serialize,
    std::{collections::HashSet, rc::Rc},
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
    let columns = Rc::from(fetch_columns(storage, table_name)?);
    let referencings = storage
        .fetch_referencings(table_name)?
        .into_iter()
        .map(|referencing| {
            fetch_columns(storage, &referencing.table_name)
                .map(Rc::from)
                .map(|columns| (referencing, columns))
        })
        .collect::<Result<Vec<_>>>()?;

    let mut keys = Vec::new();
    // Self-references are checked after every row to delete is known, so a referencing row
    // deleted by the same statement doesn't block it. Only the referenced values are kept.
    let mut self_references = Vec::new();
    for item in fetch(storage, table_name, columns, selection)? {
        let (key, row) = item?;

        for (referencing, columns) in &referencings {
            let referenced_column_name = &referencing.foreign_key.referenced_column_name;
            let value = row
                .get_value(referenced_column_name)
                .ok_or(DeleteError::ValueNotFound(referenced_column_name.clone()))?
                .clone();

            if referencing.table_name == table_name {
                self_references.push((referencing, columns, value));
            } else {
                check_referencing(storage, referencing, columns, value, None)?;
            }
        }

        keys.push(key);
    }

    if !self_references.is_empty() {
        let deleting = keys.iter().collect::<HashSet<_>>();
        for (referencing, columns, value) in self_references {
            check_referencing(storage, referencing, columns, value, Some(&deleting))?;
        }
    }
    let num_keys = keys.len();

    storage
        .delete_data(table_name, keys)
        .map(|()| Payload::Delete(num_keys))
}

fn check_referencing<T: GStore>(
    storage: &T,
    referencing: &Referencing,
    columns: &Rc<[String]>,
    value: Value,
    deleting: Option<&HashSet<&Key>>,
) -> Result<()> {
    let Referencing {
        table_name: referencing_table_name,
        foreign_key:
            ForeignKey {
                referencing_column_name,
                on_delete,
                ..
            },
    } = referencing;

    let expr = &ExprPlan::BinaryOp {
        left: Box::new(ExprPlan::Identifier(referencing_column_name.clone())),
        op: BinaryOperator::Eq,
        right: Box::new(ExprPlan::Value(value)),
    };

    let mut referencing_row_exists = false;
    for item in fetch(
        storage,
        referencing_table_name,
        Rc::clone(columns),
        Some(expr),
    )? {
        let (key, _) = item?;
        if !deleting.is_some_and(|deleting| deleting.contains(&key)) {
            referencing_row_exists = true;
            break;
        }
    }

    if referencing_row_exists && on_delete == &ReferentialAction::NoAction {
        return Err(DeleteError::ReferencingColumnExists(format!(
            "{referencing_table_name}.{referencing_column_name}"
        ))
        .into());
    }

    Ok(())
}
