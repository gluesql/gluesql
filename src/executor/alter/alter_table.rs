use {
    super::AlterError,
    crate::{
        data::get_name,
        result::MutResult,
        store::{AlterTable, Store, StoreMut},
    },
    sqlparser::ast::{AlterTableOperation, ObjectName},
    std::fmt::Debug,
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

pub async fn alter_table<T: 'static + Debug, U: Store<T> + StoreMut<T> + AlterTable>(
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
                .rename_column(table_name, &old_column_name.value, &new_column_name.value)
                .await
        }
        AlterTableOperation::AddColumn { column_def } => {
            storage.add_column(table_name, column_def).await
        }
        AlterTableOperation::DropColumn {
            column_name,
            if_exists,
            ..
        } => {
            storage
                .drop_column(table_name, &column_name.value, *if_exists)
                .await
        }
        _ => Err((
            storage,
            AlterError::UnsupportedAlterTableOperation(operation.to_string()).into(),
        )),
    }
}
