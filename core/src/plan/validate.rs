use {
    crate::{ast::Statement, data::Schema, result::Result},
    std::collections::HashMap,
};

pub fn validate(schema_map: &HashMap<String, Schema>, statement: Statement) -> Result<Statement> {
    // TODO #1. Check if selected column is unique across joined tables
    // TODO #2. If yes, correct statement to `{table_name}.{column}`
    // TODO #3. If not, omit error for the ambiguous column selection
    Ok(statement)
}

/*
{
    "TxAlter":
        Schema {
            table_name: "TxAlter",
            column_defs: [
                ColumnDef {
                    name: "id",
                    data_type: Int,
                    options: []
                },
                ColumnDef {
                    name: "num",
                    data_type: Int,
                    options: []
                }
            ],
            indexes: []
        }
}
*/
