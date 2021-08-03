#![cfg(feature = "index")]

use {
    super::{err_into, fetch_schema},
    crate::{
        ast::Expr, evaluate_stateless, utils::Vector, Error, IndexError, Result, Row, Schema,
        SchemaIndex, Value,
    },
    sled::{
        transaction::{
            ConflictableTransactionError, ConflictableTransactionResult, TransactionalTree,
        },
        Db, IVec,
    },
    std::{borrow::Cow, convert::TryInto},
};

pub struct IndexSync<'a> {
    table_name: &'a str,
    columns: Vec<String>,
    indexes: Cow<'a, Vec<SchemaIndex>>,
}

impl<'a> From<&'a Schema> for IndexSync<'a> {
    fn from(schema: &'a Schema) -> Self {
        let Schema {
            table_name,
            column_defs,
            indexes,
            ..
        } = schema;

        let columns = column_defs
            .iter()
            .map(|column_def| column_def.name.to_owned())
            .collect::<Vec<_>>();

        let indexes = Cow::Borrowed(indexes);

        Self {
            table_name,
            columns,
            indexes,
        }
    }
}

impl<'a> IndexSync<'a> {
    pub fn new(tree: &Db, table_name: &'a str) -> Result<Self> {
        let (_, schema) = fetch_schema(tree, table_name)?;
        let Schema {
            column_defs,
            indexes,
            ..
        } = schema.ok_or_else(|| IndexError::ConflictTableNotFound(table_name.to_owned()))?;

        let columns = column_defs
            .into_iter()
            .map(|column_def| column_def.name)
            .collect::<Vec<_>>();

        Ok(Self {
            table_name,
            columns,
            indexes: Cow::Owned(indexes),
        })
    }

    pub fn insert(
        &self,
        tree: &TransactionalTree,
        data_key: &IVec,
        row: &Row,
    ) -> ConflictableTransactionResult<(), Error> {
        for index in self.indexes.iter() {
            self.insert_index(tree, index, data_key, row)?;
        }

        Ok(())
    }

    pub fn insert_index(
        &self,
        tree: &TransactionalTree,
        index: &SchemaIndex,
        data_key: &IVec,
        row: &Row,
    ) -> ConflictableTransactionResult<(), Error> {
        let SchemaIndex {
            name: index_name,
            expr: index_expr,
            ..
        } = index;

        let index_key =
            &evaluate_index_key(self.table_name, index_name, index_expr, &self.columns, row)?;

        insert_index_data(tree, index_key, data_key)?;

        Ok(())
    }

    pub fn update(
        &self,
        tree: &TransactionalTree,
        data_key: &IVec,
        old_row: &Row,
        new_row: &Row,
    ) -> ConflictableTransactionResult<(), Error> {
        for index in self.indexes.iter() {
            let SchemaIndex {
                name: index_name,
                expr: index_expr,
                ..
            } = index;

            let old_index_key = &evaluate_index_key(
                self.table_name,
                index_name,
                index_expr,
                &self.columns,
                old_row,
            )?;

            let new_index_key = &evaluate_index_key(
                self.table_name,
                index_name,
                index_expr,
                &self.columns,
                new_row,
            )?;

            delete_index_data(tree, old_index_key, data_key)?;
            insert_index_data(tree, new_index_key, data_key)?;
        }

        Ok(())
    }

    pub fn delete(
        &self,
        tree: &TransactionalTree,
        data_key: &IVec,
        row: &Row,
    ) -> ConflictableTransactionResult<(), Error> {
        for index in self.indexes.iter() {
            self.delete_index(tree, index, data_key, row)?;
        }

        Ok(())
    }

    pub fn delete_index(
        &self,
        tree: &TransactionalTree,
        index: &SchemaIndex,
        data_key: &IVec,
        row: &Row,
    ) -> ConflictableTransactionResult<(), Error> {
        let SchemaIndex {
            name: index_name,
            expr: index_expr,
            ..
        } = index;

        let index_key =
            &evaluate_index_key(self.table_name, index_name, index_expr, &self.columns, row)?;

        delete_index_data(tree, index_key, data_key)?;

        Ok(())
    }
}

fn insert_index_data(
    tree: &TransactionalTree,
    index_key: &[u8],
    data_key: &IVec,
) -> ConflictableTransactionResult<(), Error> {
    let data_keys: Vec<Vec<u8>> = tree
        .get(index_key)?
        .map(|v| bincode::deserialize(&v))
        .transpose()
        .map_err(err_into)
        .map_err(ConflictableTransactionError::Abort)?
        .unwrap_or_default();

    let data_keys = Vector::from(data_keys).push(data_key.to_vec());
    let data_keys = bincode::serialize(&Vec::from(data_keys))
        .map_err(err_into)
        .map_err(ConflictableTransactionError::Abort)?;

    tree.insert(index_key, data_keys)?;

    Ok(())
}

fn delete_index_data(
    tree: &TransactionalTree,
    index_key: &[u8],
    data_key: &IVec,
) -> ConflictableTransactionResult<(), Error> {
    let data_keys: Vec<Vec<u8>> = tree
        .get(index_key)?
        .map(|v| bincode::deserialize(&v))
        .ok_or_else(|| IndexError::ConflictOnIndexDataDeleteSync.into())
        .map_err(ConflictableTransactionError::Abort)?
        .map_err(err_into)
        .map_err(ConflictableTransactionError::Abort)?;

    let data_keys = data_keys
        .into_iter()
        .filter(|k| k != data_key.as_ref())
        .collect::<Vec<_>>();

    if data_keys.is_empty() {
        tree.remove(index_key)?;
    } else {
        let data_keys = bincode::serialize(&data_keys)
            .map_err(err_into)
            .map_err(ConflictableTransactionError::Abort)?;

        tree.insert(index_key, data_keys)?;
    }

    Ok(())
}

fn evaluate_index_key(
    table_name: &str,
    index_name: &str,
    index_expr: &Expr,
    columns: &[String],
    row: &Row,
) -> ConflictableTransactionResult<Vec<u8>, Error> {
    let evaluated = evaluate_stateless(Some((columns, row)), index_expr)
        .map_err(ConflictableTransactionError::Abort)?;
    let value: Value = evaluated
        .try_into()
        .map_err(ConflictableTransactionError::Abort)?;

    Ok(build_index_key(table_name, index_name, value))
}

pub fn build_index_key_prefix(table_name: &str, index_name: &str) -> Vec<u8> {
    format!("index/{}/{}/", table_name, index_name).into_bytes()
}

pub fn build_index_key(table_name: &str, index_name: &str, value: Value) -> Vec<u8> {
    build_index_key_prefix(table_name, index_name)
        .into_iter()
        .chain(value.to_be_bytes().into_iter())
        .collect::<Vec<_>>()
}
