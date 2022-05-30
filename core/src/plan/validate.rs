use {
    crate::{
        ast::{Expr, SelectItem, SetExpr, Statement},
        data::Schema,
        executor::SelectError,
        result::Result,
    },
    std::collections::HashMap,
};

pub fn validate(schema_map: &HashMap<String, Schema>, statement: Statement) -> Result<Statement> {
    if let Statement::Query(query) = &statement {
        if let SetExpr::Select(select) = &query.body {
            select
                .projection
                .iter()
                .map(|select_item| {
                    if let SelectItem::Expr {
                        expr: Expr::Identifier(ident),
                        ..
                    } = select_item
                    {
                        let tables_with_given_col = schema_map.iter().map(|(_, schema)| {
                            schema.column_defs.iter().find(|col| &col.name == ident)
                        });

                        if tables_with_given_col.len() > 1 {
                            return Err(
                                SelectError::ColumnReferenceAmbiguous(ident.to_string()).into()
                            );
                        }
                    }

                    Ok(())
                })
                .collect::<Result<Vec<()>>>()?;
        }
    }

    Ok(statement)
}
