use sqlparser::ast::{ColumnDef, ColumnOption, Ident};

pub fn fetch_all_unique_columns(column_defs: &[ColumnDef]) -> Vec<(usize, String)> {
    column_defs
        .iter()
        .enumerate()
        .filter_map(|(i, table_col)| {
            if table_col
                .options
                .iter()
                .any(|opt_def| matches!(opt_def.option, ColumnOption::Unique { .. }))
            {
                Some((i, table_col.name.value.to_owned()))
            } else {
                None
            }
        })
        .collect()
}

// KG: Made this so that code isn't repeated... Perhaps this is inefficient though?
// KG: Unsure if we should keep this, I like how it works, code-wise and may be good if ever anything else needs specified columns.
pub fn specified_columns_only(
    matched_columns: Vec<(usize, String)>,
    specified_columns: &[Ident],
) -> Vec<(usize, String)> {
    matched_columns
        .iter()
        .enumerate()
        .filter_map(|(i, table_col)| {
            if specified_columns
                .iter()
                .any(|specified_col| specified_col.value == table_col.1)
            {
                Some((i, table_col.1.clone()))
            } else {
                None
            }
        })
        .collect()
}
