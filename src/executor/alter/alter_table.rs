#![cfg(feature = "alter-table")]

use {
    super::validate,
    crate::{
        ast::{AlterTableOperation, ObjectName},
        data::get_name,
        result::MutResult,
        store::{GStore, GStoreMut},
    },
    std::fmt::Debug,
};

#[cfg(feature = "index")]
use {
    super::AlterError,
    crate::{
        ast::Expr,
        data::{Schema, SchemaIndex},
    },
    futures::stream::{self, TryStreamExt},
};

macro_rules! try_into {
    ($storage: expr, $expr: expr) => {
        match $expr {
            Err(e) => {
                return Err(($storage, e.into()));
            }
            Ok(v) => v,
        }
    };
}

pub async fn alter_table<T: 'static + Debug, U: GStore<T> + GStoreMut<T>>(
    storage: U,
    name: &ObjectName,
    operation: &AlterTableOperation,
) -> MutResult<U, ()> {
    let table_name = try_into!(storage, get_name(name));

    match operation {
        AlterTableOperation::RenameTable {
            table_name: new_table_name,
        } => {
            let new_table_name = try_into!(storage, get_name(new_table_name));

            storage.rename_schema(table_name, new_table_name).await
        }
        AlterTableOperation::RenameColumn {
            old_column_name,
            new_column_name,
        } => {
            storage
                .rename_column(table_name, old_column_name, new_column_name)
                .await
        }
        AlterTableOperation::AddColumn { column_def } => {
            try_into!(storage, validate(column_def));

            storage.add_column(table_name, column_def).await
        }
        AlterTableOperation::DropColumn {
            column_name,
            if_exists,
        } => {
            #[cfg(feature = "index")]
            let storage = {
                let indexes = match storage.fetch_schema(table_name).await {
                    Ok(Some(Schema { indexes, .. })) => indexes,
                    Ok(None) => {
                        return Err((
                            storage,
                            AlterError::TableNotFound(table_name.to_owned()).into(),
                        ));
                    }
                    Err(e) => {
                        return Err((storage, e));
                    }
                };

                let indexes = indexes
                    .iter()
                    .filter(|SchemaIndex { expr, .. }| find_column(expr, column_name))
                    .map(Ok);

                stream::iter(indexes)
                    .try_fold(storage, |storage, SchemaIndex { name, .. }| async move {
                        storage
                            .drop_index(table_name, name)
                            .await
                            .map(|(storage, _)| storage)
                    })
                    .await?
            };

            storage
                .drop_column(table_name, column_name, *if_exists)
                .await
        }
    }
}

#[cfg(feature = "index")]
fn find_column(expr: &Expr, column_name: &str) -> bool {
    let find = |expr| find_column(expr, column_name);

    match expr {
        Expr::Identifier(ident) => ident == column_name,
        Expr::Nested(expr) => find(expr),
        Expr::BinaryOp { left, right, .. } => find(left) || find(right),
        Expr::UnaryOp { expr, .. } => find(expr),
        Expr::Cast { expr, .. } => find(expr),
        _ => false,
    }
}
