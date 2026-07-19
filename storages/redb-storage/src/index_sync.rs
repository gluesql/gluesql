use {
    crate::error::StorageError,
    gluesql_core::{
        data::{Schema, SchemaIndex, Value},
        error::Result,
        executor::{RowContext, evaluate_stateless},
        plan::{ExprPlan, plan_scalar_expr},
    },
    redb::{MultimapTableDefinition, WriteTransaction},
};

const INDEX_TABLE_PREFIX: &str = "__GLUESQL_INDEX__";

pub(super) type UpdateRow = (Vec<u8>, Option<Vec<Value>>, Vec<Value>);

pub(super) fn index_table_name(table_name: &str, index_name: &str) -> String {
    format!("{INDEX_TABLE_PREFIX}/{table_name}/{index_name}")
}

fn index_table_def(table_name: &str) -> MultimapTableDefinition<'_, &'static [u8], &'static [u8]> {
    MultimapTableDefinition::new(table_name)
}

struct PlannedIndex {
    name: String,
    expr: ExprPlan,
}

impl PlannedIndex {
    fn new(index: &SchemaIndex) -> Self {
        Self {
            name: index.name.clone(),
            expr: plan_scalar_expr(index.expr.clone()),
        }
    }
}

#[derive(Clone, Copy)]
struct RowChange<'a> {
    row_key: &'a [u8],
    old_row: Option<&'a [Value]>,
    new_row: Option<&'a [Value]>,
}

struct PreparedChange {
    row_key: Vec<u8>,
    old_value: Option<Vec<u8>>,
    new_value: Option<Vec<u8>>,
}

struct PreparedIndex {
    name: String,
    changes: Vec<PreparedChange>,
}

pub(super) struct PreparedIndexChanges {
    table_name: String,
    indexes: Vec<PreparedIndex>,
}

impl PreparedIndexChanges {
    pub(super) fn apply(self, txn: &WriteTransaction) -> Result<()> {
        for index in self.indexes {
            let table_name = index_table_name(&self.table_name, &index.name);
            let mut table = txn
                .open_multimap_table(index_table_def(&table_name))
                .map_err(StorageError::from)?;

            for change in index.changes {
                if let Some(old_value) = change.old_value {
                    table
                        .remove(old_value.as_slice(), change.row_key.as_slice())
                        .map_err(StorageError::from)?;
                }

                if let Some(new_value) = change.new_value {
                    table
                        .insert(new_value.as_slice(), change.row_key.as_slice())
                        .map_err(StorageError::from)?;
                }
            }
        }

        Ok(())
    }
}

pub(super) fn prepare_insert(
    schema: &Schema,
    rows: &[(Vec<u8>, Vec<Value>)],
) -> Result<PreparedIndexChanges> {
    let changes = rows
        .iter()
        .map(|(row_key, row)| RowChange {
            row_key,
            old_row: None,
            new_row: Some(row),
        })
        .collect::<Vec<_>>();

    prepare(schema, schema.indexes.iter(), &changes)
}

pub(super) fn prepare_update(schema: &Schema, rows: &[UpdateRow]) -> Result<PreparedIndexChanges> {
    let changes = rows
        .iter()
        .map(|(row_key, old_row, new_row)| RowChange {
            row_key,
            old_row: old_row.as_deref(),
            new_row: Some(new_row),
        })
        .collect::<Vec<_>>();

    prepare(schema, schema.indexes.iter(), &changes)
}

pub(super) fn prepare_delete(
    schema: &Schema,
    rows: &[(Vec<u8>, Vec<Value>)],
) -> Result<PreparedIndexChanges> {
    let changes = rows
        .iter()
        .map(|(row_key, row)| RowChange {
            row_key,
            old_row: Some(row),
            new_row: None,
        })
        .collect::<Vec<_>>();

    prepare(schema, schema.indexes.iter(), &changes)
}

pub(super) fn prepare_backfill(
    schema: &Schema,
    index: &SchemaIndex,
    rows: &[(Vec<u8>, Vec<Value>)],
) -> Result<PreparedIndexChanges> {
    let changes = rows
        .iter()
        .map(|(row_key, row)| RowChange {
            row_key,
            old_row: None,
            new_row: Some(row),
        })
        .collect::<Vec<_>>();

    prepare(schema, std::iter::once(index), &changes)
}

pub(super) fn delete_index_table(
    txn: &WriteTransaction,
    table_name: &str,
    index_name: &str,
) -> Result<()> {
    let index_table_name = index_table_name(table_name, index_name);
    txn.delete_multimap_table(index_table_def(&index_table_name))
        .map_err(StorageError::from)?;

    Ok(())
}

fn prepare<'a>(
    schema: &Schema,
    indexes: impl Iterator<Item = &'a SchemaIndex>,
    changes: &[RowChange<'_>],
) -> Result<PreparedIndexChanges> {
    let columns = schema.column_defs.as_ref().map(|column_defs| {
        column_defs
            .iter()
            .map(|column_def| column_def.name.clone())
            .collect::<Vec<_>>()
    });

    let indexes = indexes
        .map(|index| {
            let index = PlannedIndex::new(index);
            let changes = changes
                .iter()
                .copied()
                .map(|change| prepare_change(&index, columns.as_deref(), change))
                .filter_map(Result::transpose)
                .collect::<Result<Vec<_>>>()?;

            Ok(PreparedIndex {
                name: index.name,
                changes,
            })
        })
        .collect::<Result<Vec<_>>>()?;

    Ok(PreparedIndexChanges {
        table_name: schema.table_name.clone(),
        indexes,
    })
}

fn prepare_change(
    index: &PlannedIndex,
    columns: Option<&[String]>,
    change: RowChange<'_>,
) -> Result<Option<PreparedChange>> {
    let old_value = change
        .old_row
        .map(|row| evaluate_index_value(&index.expr, columns, row))
        .transpose()?;
    let new_value = change
        .new_row
        .map(|row| evaluate_index_value(&index.expr, columns, row))
        .transpose()?;

    Ok((old_value != new_value).then(|| PreparedChange {
        row_key: change.row_key.to_vec(),
        old_value,
        new_value,
    }))
}

fn evaluate_index_value(
    index_expr: &ExprPlan,
    columns: Option<&[String]>,
    row: &[Value],
) -> Result<Vec<u8>> {
    let context = Some(RowContext::RefVecData {
        columns: columns.unwrap_or(&[]),
        values: row,
    });
    let value: Value = evaluate_stateless(context, index_expr)?.try_into()?;

    value.to_cmp_be_bytes()
}
