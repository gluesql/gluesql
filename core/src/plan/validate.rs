use {
    super::PlanError,
    crate::{
        ast::{Expr, SelectItem, SetExpr, Statement},
        data::Schema,
        result::Result,
    },
    std::collections::HashMap,
};

/// Validate user select column should not be ambiguous
pub fn validate(schema_map: &HashMap<String, Schema>, statement: Statement) -> Result<Statement> {
    if let Statement::Query(query) = &statement {
        if let SetExpr::Select(select) = &query.body {
            if !select.from.joins.is_empty() {
                select
                    .projection
                    .iter()
                    .map(|select_item| {
                        if let SelectItem::Expr {
                            expr: Expr::Identifier(ident),
                            ..
                        } = select_item
                        {
                            let tables_with_given_col =
                                schema_map.iter().filter_map(|(_, schema)| {
                                    match schema.column_defs.as_ref() {
                                        Some(column_defs) => {
                                            column_defs.iter().find(|col| &col.name == ident)
                                        }
                                        None => None,
                                    }
                                });

                            if tables_with_given_col.count() > 1 {
                                return Err(
                                    PlanError::ColumnReferenceAmbiguous(ident.to_owned()).into()
                                );
                            }
                        }

                        Ok(())
                    })
                    .collect::<Result<Vec<()>>>()?;
            }
        }
    }

    Ok(statement)
}
