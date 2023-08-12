use gluesql_core::ast::ColumnDef;

pub fn get_primary_key(column_defs: &Vec<ColumnDef>) -> Option<&ColumnDef> {
    column_defs
        .into_iter()
        .find(|column_def| column_def.unique.map(|x| x.is_primary).unwrap_or(false))
}
