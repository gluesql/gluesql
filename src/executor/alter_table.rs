use sqlparser::ast::AlterTableOperation;

use crate::result::MutResult;
use crate::store::AlterTable;

pub fn alter_table<T: AlterTable>(
    storage: T,
    table_name: &str,
    operation: &AlterTableOperation,
) -> MutResult<T, ()> {
    match operation {
        /*
        AlterTableOperation::RenameColumn {
            old_column_name, // Ident
            new_column_name,
        } => {
        }
        */
        AlterTableOperation::RenameTable {
            table_name: new_table_name,
        } => storage.rename_schema(table_name, &new_table_name.value),
        _ => Ok((storage, ())),
    }
}
