use {
    super::{context::RowContext, evaluate::evaluate},
    crate::{
        ast::{ColumnDef, ColumnUniqueOption, ForeignKey},
        data::{Key, Row, Value},
        plan::AssignmentPlan,
        result::Result,
        store::GStore,
    },
    serde::Serialize,
    std::{borrow::Cow, fmt::Debug, rc::Rc},
    thiserror::Error,
};

#[derive(Error, Serialize, Debug, PartialEq, Eq)]
pub enum UpdateError {
    #[error("column not found {0}")]
    ColumnNotFound(String),

    #[error("update on primary key is not supported: {0}")]
    UpdateOnPrimaryKeyNotSupported(String),

    #[error("conflict on schema, row data does not fit to schema")]
    ConflictOnSchema,

    #[error("conflict on schemaless row, expected first value to be map")]
    ConflictOnNonMapSchemalessRow,

    #[error(
        "cannot find referenced value on {table_name}.{column_name} with value {referenced_value:?}"
    )]
    CannotFindReferencedValue {
        table_name: String,
        column_name: String,
        referenced_value: String,
    },
}

/// The alias `PostgreSQL` binds the proposed row to inside `ON CONFLICT DO UPDATE`.
/// `GlueSQL` matches identifiers exactly, so both spellings clients write are bound.
const EXCLUDED: &str = "excluded";
const EXCLUDED_UPPERCASE: &str = "EXCLUDED";

/// What an `ON CONFLICT DO UPDATE` expression sees: the row already in the table, by its
/// own column names, and the row the insert proposed, under the `excluded` alias. The
/// table comes first in the chain, so a bare column name is the stored row's, as
/// `PostgreSQL` resolves it.
pub fn conflict_context<'a>(
    table_name: &'a str,
    row: &'a Row,
    excluded: &'a Row,
) -> RowContext<'a> {
    let uppercase = Rc::new(RowContext::new(
        EXCLUDED_UPPERCASE,
        Cow::Borrowed(excluded),
        None,
    ));
    let excluded = Rc::new(RowContext::new(
        EXCLUDED,
        Cow::Borrowed(excluded),
        Some(uppercase),
    ));

    RowContext::new(table_name, Cow::Borrowed(row), Some(excluded))
}

pub struct Update<'a, T: GStore> {
    storage: &'a T,
    table_name: &'a str,
    fields: &'a [AssignmentPlan],
    column_defs: Option<&'a [ColumnDef]>,
}

impl<'a, T: GStore> Update<'a, T> {
    pub fn new(
        storage: &'a T,
        table_name: &'a str,
        fields: &'a [AssignmentPlan],
        column_defs: Option<&'a [ColumnDef]>,
    ) -> Result<Self> {
        if let Some(column_defs) = column_defs {
            for assignment in fields {
                let AssignmentPlan { id, .. } = assignment;

                if column_defs.iter().all(|col_def| &col_def.name != id) {
                    return Err(UpdateError::ColumnNotFound(id.to_owned()).into());
                } else if column_defs.iter().any(|ColumnDef { name, unique, .. }| {
                    name == id && matches!(unique, Some(ColumnUniqueOption { is_primary: true }))
                }) {
                    return Err(UpdateError::UpdateOnPrimaryKeyNotSupported(id.to_owned()).into());
                }
            }
        }

        Ok(Self {
            storage,
            table_name,
            fields,
            column_defs,
        })
    }

    pub fn apply(&self, row: Row, foreign_keys: &[ForeignKey]) -> Result<Row> {
        self.apply_with(row, foreign_keys, None)
    }

    /// `excluded` is the row an `INSERT ... ON CONFLICT DO UPDATE` proposed; the
    /// assignments see it under that alias.
    pub fn apply_with(
        &self,
        row: Row,
        foreign_keys: &[ForeignKey],
        excluded: Option<&Row>,
    ) -> Result<Row> {
        let context = match excluded {
            Some(excluded) => conflict_context(self.table_name, &row, excluded),
            None => RowContext::new(self.table_name, Cow::Borrowed(&row), None),
        };
        let context = Some(Rc::new(context));

        let mut assignments = Vec::with_capacity(self.fields.len());
        for assignment in self.fields {
            let AssignmentPlan {
                id,
                value: value_expr,
            } = assignment;
            let evaluated = evaluate(self.storage, context.as_ref(), None, value_expr)?;
            let value = match self.column_defs {
                Some(column_defs) => {
                    let ColumnDef {
                        data_type,
                        nullable,
                        ..
                    } = column_defs
                        .iter()
                        .find(|column_def| id == &column_def.name)
                        .ok_or(UpdateError::ConflictOnSchema)?;

                    evaluated.try_into_value(data_type, *nullable)?
                }
                None => evaluated.try_into()?,
            };

            if value != Value::Null {
                for foreign_key in foreign_keys {
                    let ForeignKey {
                        referencing_column_name,
                        referenced_table_name,
                        referenced_column_name,
                        ..
                    } = foreign_key;

                    if referencing_column_name != id {
                        continue;
                    }

                    let no_referenced = self
                        .storage
                        .fetch_data(referenced_table_name, &Key::try_from(&value)?)?
                        .is_none();

                    if no_referenced {
                        return Err(UpdateError::CannotFindReferencedValue {
                            table_name: referenced_table_name.to_owned(),
                            column_name: referenced_column_name.to_owned(),
                            referenced_value: String::from(value),
                        }
                        .into());
                    }
                }
            }

            assignments.push((id.as_str(), value));
        }

        let Row { columns, values } = row;

        let values = if self.column_defs.is_none() {
            // Schemaless table: update fields inside the _doc Map
            let mut values = values;
            let Some(Value::Map(map)) = values.first_mut() else {
                return Err(UpdateError::ConflictOnNonMapSchemalessRow.into());
            };

            for (id, value) in assignments {
                map.insert(id.to_owned(), value);
            }
            values
        } else {
            columns
                .iter()
                .zip(values)
                .map(|(column, value)| {
                    assignments
                        .iter()
                        .find_map(|(id, new_value)| (column == id).then_some(new_value.clone()))
                        .unwrap_or(value)
                })
                .collect()
        };

        Ok(Row { columns, values })
    }
}
