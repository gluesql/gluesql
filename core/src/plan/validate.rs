use {
    super::PlanError,
    crate::{
        ast::{
            Expr, Join, Query, SelectItem, SetExpr, Statement, TableAlias, TableFactor,
            TableWithJoins,
        },
        data::Schema,
        result::Result,
    },
    std::{collections::HashMap, rc::Rc},
};

/// Validate user select column should not be ambiguous
pub fn validate(
    validation_context: Option<Rc<ValidationContext>>,
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
                            let tables_with_given_col = validation_context
                                .as_ref()
                                .map(|context| context.count(ident))
                                .unwrap_or(0);

                            if tables_with_given_col > 1 {
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

pub enum ValidationContext<'a> {
    Data {
        table_name: &'a String,
        alias: Option<&'a TableAlias>,
        schema: &'a Schema,
        next: Option<Rc<ValidationContext<'a>>>,
    },
    Bridge {
        left: Rc<ValidationContext<'a>>,
        right: Rc<ValidationContext<'a>>,
    },
}

impl<'a> ValidationContext<'a> {
    fn new(
        table_name: &'a String,
        alias: Option<&'a TableAlias>,
        schema: &'a Schema,
        next: Option<Rc<ValidationContext<'a>>>,
    ) -> Self {
        Self::Data {
            table_name,
            alias,
            schema,
            next,
        }
    }

    fn concat(
        left: Option<Rc<ValidationContext<'a>>>,
        right: Option<Rc<ValidationContext<'a>>>,
    ) -> Option<Rc<Self>> {
        match (left, right) {
            (Some(left), Some(right)) => Some(Rc::new(Self::Bridge { left, right })),
            (context @ Some(_), None) | (None, context @ Some(_)) => context,
            (None, None) => None,
        }
    }

    fn count(&self, column_name: &String) -> i32 {
        match self {
            ValidationContext::Data { schema, next, .. } => {
                let current = schema
                    .column_defs
                    .map(|column_defs| {
                        column_defs
                            .iter()
                            .any(|column| column.name == column_name.to_owned())
                            .then_some(0)
                            .unwrap_or(0)
                    })
                    .unwrap_or(0);

                let next = next
                    .as_ref()
                    .map(|context| context.count(column_name))
                    .unwrap_or(0);

                current + next
            }
            ValidationContext::Bridge { left, right } => {
                left.count(column_name) + right.count(column_name)
            }
        }
    }
}

type SchemaMap = HashMap<String, Schema>;
pub fn update_validation_context<'a>(
    schema_map: &'a SchemaMap,
    statement: &'a Statement,
) -> Option<Rc<ValidationContext<'a>>> {
    match statement {
        Statement::Query(query) => update_validation_context_by_query(schema_map, query),
        Statement::Insert {
            table_name, source, ..
        } => {
            let table_context = schema_map.get(table_name).map(|schema| {
                Rc::from(ValidationContext::new(
                    &schema.table_name,
                    None,
                    schema,
                    None,
                ))
            });

            let source_context = update_validation_context_by_query(schema_map, source);

            ValidationContext::concat(table_context, source_context)
        }
        Statement::DropTable { names, .. } => names
            .iter()
            .map(|name| {
                let schema = schema_map.get(name);
                schema.map(|schema| Rc::from(ValidationContext::new(name, None, schema, None)))
            })
            .fold(None, |acc, cur| ValidationContext::concat(acc, cur)),
        _ => None,
    }
}

fn update_validation_context_by_query<'a>(
    schema_map: &'a SchemaMap,
    query: &'a Query,
) -> Option<Rc<ValidationContext<'a>>> {
    let Query { body, .. } = query;
    match body {
        SetExpr::Select(select) => {
            let TableWithJoins { relation, joins } = &select.from;

            let by_table = match relation {
                TableFactor::Table { name, alias, .. } => {
                    let schema = schema_map.get(name);
                    schema.map(|schema| {
                        Rc::from(ValidationContext::new(name, alias.as_ref(), schema, None))
                    })
                }
                TableFactor::Derived { subquery, .. } => {
                    update_validation_context_by_query(schema_map, subquery)
                }
                TableFactor::Series { .. } | TableFactor::Dictionary { .. } => None,
            }
            .map(Rc::from);

            let by_joins = joins
                .iter()
                .map(|Join { relation, .. }| match relation {
                    TableFactor::Table { name, alias, .. } => {
                        let schema = schema_map.get(name);
                        schema.map(|schema| {
                            Rc::from(ValidationContext::new(name, alias.as_ref(), schema, None))
                        })
                    }
                    TableFactor::Derived { subquery, .. } => {
                        update_validation_context_by_query(schema_map, subquery)
                    }
                    TableFactor::Series { .. } | TableFactor::Dictionary { .. } => None,
                })
                .map(|context| context.map(Rc::from))
                .fold(None, |acc, cur| ValidationContext::concat(acc, cur));

            ValidationContext::concat(by_table, by_joins)
        }
        SetExpr::Values(_) => None,
    }
}
