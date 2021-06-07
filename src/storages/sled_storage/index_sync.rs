#![cfg(feature = "index")]

use {
    super::fetch_schema,
    crate::{ast::Expr, evaluate_stateless, Error, IndexError, Result, Row, Schema, Value},
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
    indexes: Cow<'a, Vec<(String, Expr)>>,
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
        for (index_name, index_expr) in self.indexes.iter() {
            let index_key =
                &evaluate_index_key(self.table_name, index_name, index_expr, &self.columns, row)?;

            insert_index_data(tree, self.table_name, &index_key, &data_key)?;
        }

        Ok(())
    }

    pub fn update(
        &self,
        tree: &TransactionalTree,
        data_key: &IVec,
        old_row: &Row,
        new_row: &Row,
    ) -> ConflictableTransactionResult<(), Error> {
        for (index_name, index_expr) in self.indexes.iter() {
            let old_index_key = &evaluate_index_key(
                self.table_name,
                index_name,
                index_expr,
                &self.columns,
                &old_row,
            )?;

            let new_index_key = &evaluate_index_key(
                self.table_name,
                index_name,
                index_expr,
                &self.columns,
                &new_row,
            )?;

            delete_index_data(tree, self.table_name, &old_index_key, &data_key)?;
            insert_index_data(tree, self.table_name, &new_index_key, &data_key)?;
        }

        Ok(())
    }

    pub fn delete(
        &self,
        tree: &TransactionalTree,
        data_key: &IVec,
        row: &Row,
    ) -> ConflictableTransactionResult<(), Error> {
        for (index_name, index_expr) in self.indexes.iter() {
            let index_key =
                &evaluate_index_key(self.table_name, index_name, index_expr, &self.columns, row)?;

            delete_index_data(tree, self.table_name, &index_key, &data_key)?;
        }

        Ok(())
    }
}

fn insert_index_data(
    tree: &TransactionalTree,
    table_name: &str,
    index_key: &[u8],
    data_key: &IVec,
) -> ConflictableTransactionResult<(), Error> {
    let index_data_id = {
        if let Some(id) = tree.get(&index_key)? {
            id
        } else {
            let id = tree.generate_id()?.to_be_bytes();
            let id = IVec::from(&id);

            tree.insert(index_key, &id)?;

            id
        }
    };

    let index_data_key = make_index_data_key(table_name, &index_data_id, data_key);

    tree.insert(index_data_key, data_key)?;

    Ok(())
}

fn delete_index_data(
    tree: &TransactionalTree,
    table_name: &str,
    index_key: &[u8],
    data_key: &IVec,
) -> ConflictableTransactionResult<(), Error> {
    let index_data_id = tree
        .get(&index_key)?
        .ok_or_else(|| IndexError::ConflictOnIndexDataDeleteSync.into())
        .map_err(ConflictableTransactionError::Abort)?;

    let index_data_key = make_index_data_key(table_name, &index_data_id, data_key);

    tree.remove(index_data_key)?;

    Ok(())
}

fn make_index_data_key(table_name: &str, index_data_id: &IVec, data_key: &IVec) -> IVec {
    let data_prefix_len = "data/".as_bytes().len() + table_name.as_bytes().len();
    let data_key = data_key.subslice(data_prefix_len, data_key.as_ref().len() - data_prefix_len);
    let bytes = "indexdata/"
        .to_owned()
        .into_bytes()
        .into_iter()
        .chain(index_data_id.iter().copied())
        .chain(data_key.iter().copied())
        .collect::<Vec<_>>();

    IVec::from(bytes)
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
