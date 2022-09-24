#![cfg(feature = "alter-table")]

use {
    super::validate,
    crate::{
        ast::AlterTableOperation,
        result::{MutResult, TrySelf},
        store::{GStore, GStoreMut},
    },
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

pub async fn alter_table<T: GStore + GStoreMut>(
    storage: T,
    table_name: &String,
    operation: &AlterTableOperation,
) -> MutResult<T, ()> {
    match operation {
        AlterTableOperation::RenameTable {
            table_name: new_table_name,
        } => storage.rename_schema(table_name, new_table_name).await,
        AlterTableOperation::RenameColumn {
            old_column_name,
            new_column_name,
        } => {
            storage
                .rename_column(table_name, old_column_name, new_column_name)
                .await
        }
        AlterTableOperation::AddColumn { column_def } => {
            validate(column_def)
                .try_self(storage)
                .map(|(storage, _)| storage)?
                .add_column(table_name, column_def)
                .await
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
