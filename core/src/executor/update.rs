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

    #[error("foreign key on {0} has no primary key among its referenced columns")]
    ForeignKeyWithoutPrimaryKey(String),

    #[error(
        "cannot find referenced value on {table_name}.{column_name} with value {referenced_value:?}"
    )]
    CannotFindReferencedValue {
        table_name: String,
        column_name: String,
        referenced_value: String,
    },
}

/// Built once per statement so the row loop never re-reads a schema
struct ForeignKeyPlan<'a> {
    foreign_key: &'a ForeignKey,
    primary_key_position: usize,
    referenced_indexes: Vec<usize>,
}

pub struct Update<'a, T: GStore> {
    storage: &'a T,
    table_name: &'a str,
    fields: &'a [AssignmentPlan],
    column_defs: Option<&'a [ColumnDef]>,
    foreign_key_plans: Vec<ForeignKeyPlan<'a>>,
}

impl<'a, T: GStore> Update<'a, T> {
    pub fn new(
        storage: &'a T,
        table_name: &'a str,
        fields: &'a [AssignmentPlan],
        column_defs: Option<&'a [ColumnDef]>,
        foreign_keys: &'a [ForeignKey],
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

        let foreign_key_plans = foreign_keys
            .iter()
            .filter(|foreign_key| {
                foreign_key
                    .referencing_column_names
                    .iter()
                    .any(|column_name| {
                        fields
                            .iter()
                            .any(|AssignmentPlan { id, .. }| id == column_name)
                    })
            })
            .map(|foreign_key| {
                let referenced_column_defs = storage
                    .fetch_schema(&foreign_key.referenced_table_name)?
                    .and_then(|schema| schema.column_defs)
                    .unwrap_or_default();

                let position_of = |name: &str| {
                    referenced_column_defs
                        .iter()
                        .position(|column_def| column_def.name == name)
                };

                let primary_key_position = foreign_key
                    .referenced_column_names
                    .iter()
                    .position(|name| {
                        referenced_column_defs.iter().any(|column_def| {
                            column_def.name == *name
                                && column_def.unique
                                    == Some(ColumnUniqueOption { is_primary: true })
                        })
                    })
                    .ok_or_else(|| {
                        UpdateError::ForeignKeyWithoutPrimaryKey(foreign_key.name.clone())
                    })?;

                let referenced_indexes = foreign_key
                    .referenced_column_names
                    .iter()
                    .map(|name| {
                        position_of(name)
                            .ok_or_else(|| UpdateError::ColumnNotFound(name.clone()).into())
                    })
                    .collect::<Result<Vec<_>>>()?;

                Ok(ForeignKeyPlan {
                    foreign_key,
                    primary_key_position,
                    referenced_indexes,
                })
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(Self {
            storage,
            table_name,
            fields,
            column_defs,
            foreign_key_plans,
        })
    }

    pub fn apply(&self, row: Row) -> Result<Row> {
        let context = RowContext::new(self.table_name, Cow::Borrowed(&row), None);
        let context = Some(Rc::new(context));

        let mut assignments = Vec::with_capacity(self.fields.len());
        let mut assigned = Vec::with_capacity(self.fields.len());
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

            assigned.push(id.clone());
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

        let row = Row { columns, values };

        self.validate_foreign_keys(&row, &assigned)?;

        Ok(row)
    }

    fn validate_foreign_keys(&self, row: &Row, assigned: &[String]) -> Result<()> {
        for plan in &self.foreign_key_plans {
            let ForeignKeyPlan {
                foreign_key,
                primary_key_position,
                referenced_indexes,
            } = plan;
            let ForeignKey {
                referencing_column_names,
                referenced_table_name,
                referenced_column_names,
                ..
            } = *foreign_key;

            if !referencing_column_names
                .iter()
                .any(|column_name| assigned.iter().any(|id| id == column_name))
            {
                continue;
            }

            let values = referencing_column_names
                .iter()
                .map(|column_name| row.get_value(column_name).cloned().unwrap_or(Value::Null))
                .collect::<Vec<_>>();

            // MATCH SIMPLE: a NULL anywhere in the referencing tuple satisfies the constraint
            if values.iter().any(|value| value == &Value::Null) {
                continue;
            }

            let referenced_row = self.storage.fetch_data(
                referenced_table_name,
                &Key::try_from(&values[*primary_key_position])?,
            )?;

            let matched = referenced_row.is_some_and(|referenced_row| {
                values.iter().enumerate().all(|(position, value)| {
                    position == *primary_key_position
                        || referenced_row.get(referenced_indexes[position]) == Some(value)
                })
            });

            if !matched {
                let referenced_value = values
                    .iter()
                    .map(|value| String::from(value.clone()))
                    .collect::<Vec<_>>()
                    .join(", ");

                return Err(UpdateError::CannotFindReferencedValue {
                    table_name: referenced_table_name.to_owned(),
                    column_name: referenced_column_names.join(", "),
                    referenced_value,
                }
                .into());
            }
        }

        Ok(())
    }
}
