#[path = "alter_table/foreign_key.rs"]
mod foreign_key;
#[path = "alter_table/rebuild.rs"]
mod rebuild;
#[path = "alter_table/rename.rs"]
mod rename;
#[path = "alter_table/validation.rs"]
mod validation;

use gluesql_core::{ast::ColumnDef, parse_sql::parse_column_def, translate::translate_column_def};

fn column(sql: &str) -> ColumnDef {
    translate_column_def(&parse_column_def(sql).unwrap(), &[]).unwrap()
}
