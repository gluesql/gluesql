mod sync;

use {
    super::{RedbRowIter, Result, StorageCore, TransactionState},
    bincode::deserialize,
    gluesql_core::{
        ast::{IndexOperator, OrderByExpr},
        chrono::Utc,
        data::{Key, Schema, SchemaIndex, SchemaIndexOrd, Value},
        executor::{RowContext, evaluate_stateless},
        plan::ExprPlan,
        store::IndexError,
    },
    redb::{MultimapRange, MultimapTableDefinition, ReadableMultimapTable, ReadableTable},
    std::{iter, ops::Bound},
};

type IndexBounds = (Bound<Vec<u8>>, Bound<Vec<u8>>);
pub(super) type IndexTableDefinition<'a> =
    MultimapTableDefinition<'a, &'static [u8], &'static [u8]>;

pub(super) const INDEX_TABLE_PREFIX: &str = "__GLUESQL_INDEX__";

pub(super) fn index_table_name(table_name: &str, index_name: &str) -> String {
    format!("{INDEX_TABLE_PREFIX}/{table_name}/{index_name}")
}

impl StorageCore {
    pub(crate) fn create_index(
        &mut self,
        table_name: &str,
        index_name: &str,
        column: &OrderByExpr,
    ) -> Result<()> {
        let data_def = Self::data_table_def(table_name)?;
        let txn = self.txn()?;
        let mut schema = self
            .fetch_schema(table_name)?
            .ok_or_else(|| IndexError::TableNotFound(table_name.to_owned()))?;
        if schema.indexes.iter().any(|index| index.name == index_name) {
            return Err(IndexError::IndexNameAlreadyExists(index_name.to_owned()).into());
        }

        let columns = index_columns(&schema);
        let expr = ExprPlan::from(column.expr.clone());
        let data = txn.open_table(data_def)?;
        let entries = data
            .iter()?
            .map(|entry| {
                let (key, payload) = entry?;
                let (_, row): (Key, Vec<Value>) = deserialize(&payload.value())?;
                let value = evaluate_index_key(&expr, &columns, &row)?;
                Ok((value, key.value().to_vec()))
            })
            .collect::<Result<Vec<_>>>()?;
        let name = index_table_name(table_name, index_name);
        let mut index = txn.open_multimap_table(IndexTableDefinition::new(&name))?;
        for (value, key) in entries {
            index.insert(value.as_slice(), key.as_slice())?;
        }
        drop(index);
        drop(data);

        schema.indexes.push(SchemaIndex {
            name: index_name.to_owned(),
            expr: column.expr.clone(),
            order: SchemaIndexOrd::Both,
            created: Utc::now().naive_utc(),
        });
        self.insert_schema(&schema)
    }

    pub(crate) fn drop_index(&mut self, table_name: &str, index_name: &str) -> Result<()> {
        Self::data_table_def(table_name)?;
        let txn = self.txn()?;
        let mut schema = self
            .fetch_schema(table_name)?
            .ok_or_else(|| IndexError::TableNotFound(table_name.to_owned()))?;
        let position = schema
            .indexes
            .iter()
            .position(|index| index.name == index_name)
            .ok_or_else(|| IndexError::IndexNameDoesNotExist(index_name.to_owned()))?;

        let name = index_table_name(table_name, index_name);
        txn.delete_multimap_table(IndexTableDefinition::new(&name))?;
        schema.indexes.remove(position);
        self.insert_schema(&schema)
    }

    pub(crate) fn scan_indexed_data<'a>(
        &'a self,
        table_name: &str,
        index_name: &str,
        asc: Option<bool>,
        cmp_value: Option<(&IndexOperator, Value)>,
    ) -> Result<RedbRowIter<'a>> {
        let data_def = Self::data_table_def(table_name)?;
        let name = index_table_name(table_name, index_name);
        let (lower, upper) = index_bounds(cmp_value)?;
        let bounds = (
            lower.as_ref().map(Vec::as_slice),
            upper.as_ref().map(Vec::as_slice),
        );

        if let TransactionState::Active {
            autocommit: false,
            txn,
        } = &self.state
        {
            let index = txn.open_multimap_table(IndexTableDefinition::new(&name))?;
            let data = txn.open_table(data_def)?;
            let rows = indexed_rows(index.range::<&[u8]>(bounds)?, data, asc)
                .collect::<Result<Vec<_>>>()?;
            return Ok(Box::new(rows.into_iter().map(Ok)));
        }

        let txn = self.db.begin_read()?;
        let index = txn.open_multimap_table(IndexTableDefinition::new(&name))?;
        let data = txn.open_table(data_def)?;
        Ok(indexed_rows(index.range::<&[u8]>(bounds)?, data, asc))
    }
}

fn index_columns(schema: &Schema) -> Vec<String> {
    schema
        .column_defs
        .iter()
        .flatten()
        .map(|column| column.name.clone())
        .collect()
}

fn evaluate_index_key(expr: &ExprPlan, columns: &[String], row: &[Value]) -> Result<Vec<u8>> {
    let context = Some(RowContext::RefVecData {
        columns,
        values: row,
    });
    let value: Value = evaluate_stateless(context, expr)?.try_into()?;
    Ok(value.to_cmp_be_bytes()?)
}

fn index_bounds(cmp_value: Option<(&IndexOperator, Value)>) -> Result<IndexBounds> {
    use Bound::{Excluded, Included, Unbounded};

    let Some((op, value)) = cmp_value else {
        return Ok((Unbounded, Unbounded));
    };
    let key = value.to_cmp_be_bytes()?;
    Ok(match op {
        IndexOperator::Eq => (Included(key.clone()), Included(key)),
        IndexOperator::Gt => (Excluded(key), Unbounded),
        IndexOperator::GtEq => (Included(key), Unbounded),
        IndexOperator::Lt => (Unbounded, Excluded(key)),
        IndexOperator::LtEq => (Unbounded, Included(key)),
    })
}

fn indexed_rows<'a>(
    mut range: MultimapRange<'a, &'static [u8], &'static [u8]>,
    data: impl ReadableTable<&'static [u8], Vec<u8>> + 'a,
    asc: Option<bool>,
) -> RedbRowIter<'a> {
    let mut keys: Option<redb::MultimapValue<'a, &'static [u8]>> = None;
    Box::new(iter::from_fn(move || {
        loop {
            if let Some(keys) = &mut keys
                && let Some(key) = keys.next()
            {
                return Some((|| {
                    let key = key?;
                    let payload = data
                        .get(key.value())?
                        .ok_or(IndexError::ConflictOnEmptyIndexValueScan)?;
                    Ok(deserialize(&payload.value())?)
                })());
            }
            let entry = if asc == Some(false) {
                range.next_back()
            } else {
                range.next()
            }?;
            match entry {
                Ok((_, values)) => keys = Some(values),
                Err(error) => return Some(Err(error.into())),
            }
        }
    }))
}
