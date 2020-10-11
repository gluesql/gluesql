use sqlparser::ast::AlterTableOperation;

use crate::result::MutResult;
use crate::store::AlterTable;

pub fn alter_table<T: AlterTable>(
    storage: T,
    table_name: &str,
    operation: &AlterTableOperation,
) -> MutResult<T, ()> {
    match operation {
        AlterTableOperation::RenameTable {
            table_name: new_table_name,
        } => storage.rename_schema(table_name, &new_table_name.value),
        AlterTableOperation::AddColumn { column_def } => storage.add_column(table_name, column_def),
        AlterTableOperation::RenameColumn {
            old_column_name,
            new_column_name,
        } => storage.rename_column(table_name, &old_column_name.value, &new_column_name.value),
        _ => Ok((storage, ())),
    }
}
