#![cfg(feature = "index")]

use {
    super::{
        err_into, error::StorageError, fetch_schema, index_sync::IndexSync, scan_data, SledStorage,
    },
    crate::{
        ast::Expr,
        data::Schema,
        result::{MutResult, Result},
        store::IndexMut,
        IndexError,
    },
    async_trait::async_trait,
    sled::{
        transaction::{ConflictableTransactionError, TransactionError},
        IVec,
    },
    std::iter::once,
};

macro_rules! try_self {
    ($self: expr, $expr: expr) => {
        match $expr {
            Err(e) => {
                return Err(($self, e.into()));
            }
            Ok(v) => v,
        }
    };
}

macro_rules! try_into {
    ($self: expr, $expr: expr) => {
        match $expr.map_err(err_into) {
            Err(e) => {
                return Err(($self, e));
            }
            Ok(v) => v,
        }
    };
}

macro_rules! transaction {
    ($self: expr, $expr: expr) => {{
        let result = $self.tree.transaction($expr).map_err(|e| match e {
            TransactionError::Abort(e) => e,
            TransactionError::Storage(e) => StorageError::Sled(e).into(),
        });

        match result {
            Ok(_) => Ok(($self, ())),
            Err(e) => Err(($self, e)),
        }
    }};
}

#[async_trait(?Send)]
impl IndexMut<IVec> for SledStorage {
    async fn create_index(
        self,
        table_name: &str,
        index_name: &str,
        index_expr: &Expr,
    ) -> MutResult<Self, ()> {
        let (schema_key, schema) = try_self!(self, fetch_schema(&self.tree, table_name));
        let Schema {
            column_defs,
            indexes,
            ..
        } = try_into!(
            self,
            schema.ok_or_else(|| IndexError::ConflictTableNotFound(table_name.to_owned()))
        );

        if indexes.iter().any(|(name, _)| name == index_name) {
            return Err((
                self,
                IndexError::IndexNameAlreadyExists(index_name.to_owned()).into(),
            ));
        }

        let indexes = indexes
            .into_iter()
            .chain(once((index_name.to_owned(), index_expr.clone())))
            .collect::<Vec<_>>();

        let schema = Schema {
            table_name: table_name.to_owned(),
            column_defs,
            indexes,
        };

        let rows = try_self!(
            self,
            scan_data(&self.tree, table_name).collect::<Result<Vec<_>>>()
        );
        let index_sync = IndexSync::from(&schema);

        transaction!(self, |tree| {
            let schema_value = bincode::serialize(&schema)
                .map_err(err_into)
                .map_err(ConflictableTransactionError::Abort)?;

            tree.insert(schema_key.as_bytes(), schema_value)?;

            for (data_key, row) in rows.iter() {
                index_sync.insert(&tree, data_key, row)?;
            }

            Ok(())
        })
    }

    async fn drop_index(self, table_name: &str, index_name: &str) -> MutResult<Self, ()> {
        let (schema_key, schema) = try_self!(self, fetch_schema(&self.tree, table_name));
        let Schema {
            column_defs,
            indexes,
            ..
        } = try_into!(
            self,
            schema.ok_or_else(|| IndexError::TableNotFound(table_name.to_owned()))
        );

        if indexes.iter().all(|(name, _)| name != index_name) {
            return Err((
                self,
                IndexError::IndexNameDoesNotExist(index_name.to_owned()).into(),
            ));
        }

        let indexes = indexes
            .into_iter()
            .filter(|(name, _)| name != index_name)
            .collect::<Vec<_>>();

        let schema = Schema {
            table_name: table_name.to_owned(),
            column_defs,
            indexes,
        };

        let rows = try_self!(
            self,
            scan_data(&self.tree, table_name).collect::<Result<Vec<_>>>()
        );
        let index_sync = IndexSync::from(&schema);

        transaction!(self, |tree| {
            let schema_value = bincode::serialize(&schema)
                .map_err(err_into)
                .map_err(ConflictableTransactionError::Abort)?;

            tree.insert(schema_key.as_bytes(), schema_value)?;

            for (data_key, row) in rows.iter() {
                index_sync.delete(&tree, data_key, row)?;
            }

            Ok(())
        })
    }
}
