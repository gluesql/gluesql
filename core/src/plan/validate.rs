use std::rc::Rc;

use crate::ast::{Join, Query, TableAlias, TableFactor, TableWithJoins};

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
// pub fn validate(schema_map: &HashMap<String, Schema>, statement: Statement) -> Result<Statement> {
pub fn validate(
    schema_map: HashMap<(&String, &Option<TableAlias>), &Schema>,
    statement: &Statement,
) -> Result<()> {
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

    Ok(())
}

// enum SchemaContext {
//     Data {
//         schema: Schema,
//         alias: String,
//         next: Option<Rc<SchemaContext>>,
//     },
//     Bridge {
//         left: Option<Rc<SchemaContext>>,
//         right: Option<Rc<SchemaContext>>,
//     },
// }

// struct SchemaContext {
//
// }

pub fn update_schema_context<'a>(
    schema_map: &'a HashMap<String, Schema>,
    statement: &'a Statement,
) -> HashMap<(&'a String, &'a Option<TableAlias>), &'a Schema> {
    match statement {
        Statement::Query(Query { ref body, .. }) => match body {
            SetExpr::Select(select) => {
                let TableWithJoins { relation, joins } = &select.from;

                let by_table = match relation {
                    TableFactor::Table { name, alias, index } => {
                        let schema = schema_map.get(name);
                        schema
                            .map(|schema| HashMap::from([((name, alias), schema)]))
                            .unwrap_or(HashMap::new())
                    }
                    TableFactor::Derived { subquery, alias } => todo!(),
                    TableFactor::Series { alias, size } => todo!(),
                    TableFactor::Dictionary { dict, alias } => todo!(),
                };

                let by_join = joins
                    .into_iter()
                    .map(
                        |Join {
                             relation,
                             join_operator,
                             join_executor,
                         }| {
                            match relation {
                                TableFactor::Table { name, alias, index } => {
                                    let schema = schema_map.get(name);
                                    schema
                                        .map(|schema| HashMap::from([((name, alias), schema)]))
                                        .unwrap_or(HashMap::new())
                                }
                                TableFactor::Derived { subquery, alias } => todo!(),
                                TableFactor::Series { alias, size } => todo!(),
                                TableFactor::Dictionary { dict, alias } => todo!(),
                            }
                        },
                    )
                    .flatten()
                    .chain(by_table)
                    .collect();

                by_join
                // todo!()
            }
            SetExpr::Values(_) => todo!(),
        },
        _ => todo!(),
    }
}
