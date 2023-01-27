use {
    super::PlanError,
    crate::{
        ast::{Expr, Join, Query, SelectItem, SetExpr, Statement, TableFactor, TableWithJoins},
        data::Schema,
        result::Result,
    },
    std::{collections::HashMap, rc::Rc},
};

type SchemaMap = HashMap<String, Schema>;
/// Validate user select column should not be ambiguous
pub fn validate(schema_map: &SchemaMap, statement: &Statement) -> Result<()> {
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
                            let validation_context = contextualize_stmt(schema_map, statement);
                            let tables_with_given_col = validation_context
                                .as_ref()
                                .map(|context| context.count(ident))
                                .unwrap_or(Ok(false));

                            if let Err(_) = tables_with_given_col {
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

enum Context<'a> {
    Data {
        labels: Option<Vec<&'a String>>,
        next: Option<Rc<Context<'a>>>,
    },
    Bridge {
        left: Rc<Context<'a>>,
        right: Rc<Context<'a>>,
    },
}

impl<'a> Context<'a> {
    fn new(labels: Option<Vec<&'a String>>, next: Option<Rc<Context<'a>>>) -> Self {
        Self::Data { labels, next }
    }

    fn concat(left: Option<Rc<Context<'a>>>, right: Option<Rc<Context<'a>>>) -> Option<Rc<Self>> {
        match (left, right) {
            (Some(left), Some(right)) => Some(Rc::new(Self::Bridge { left, right })),
            (context @ Some(_), None) | (None, context @ Some(_)) => context,
            (None, None) => None,
        }
    }

    fn count(&self, column_name: &str) -> Result<bool> {
        match self {
            Context::Data { labels, next, .. } => {
                let current: Result<bool> = Ok(match labels {
                    Some(labels) => labels.iter().any(|label| *label == column_name),
                    None => false,
                });

                let next = next
                    .as_ref()
                    .map(|context| context.count(column_name))
                    .unwrap_or(Ok(false));

                match (current, next) {
                    (Ok(false), Ok(false)) => Ok(false),
                    (Ok(false), Ok(true)) | (Ok(true), Ok(false)) => Ok(true),
                    _ => Err(PlanError::ColumnReferenceAmbiguous(column_name.to_owned()).into()),
                }
            }
            Context::Bridge { left, right } => {
                match (left.count(column_name), right.count(column_name)) {
                    (Ok(false), Ok(false)) => Ok(false),
                    (Ok(false), Ok(true)) | (Ok(true), Ok(false)) => Ok(true),
                    _ => Err(PlanError::ColumnReferenceAmbiguous(column_name.to_owned()).into()),
                }
            }
        }
    }
}

fn get_lables(schema: &Schema) -> Option<Vec<&String>> {
    schema.column_defs.as_ref().map(|column_defs| {
        column_defs
            .iter()
            .map(|column_def| &column_def.name)
            .collect::<Vec<_>>()
    })
}

fn contextualize_stmt<'a>(
    schema_map: &'a SchemaMap,
    statement: &'a Statement,
) -> Option<Rc<Context<'a>>> {
    match statement {
        Statement::Query(query) => contextualize_query(schema_map, query),
        Statement::Insert {
            table_name, source, ..
        } => {
            let table_context = schema_map
                .get(table_name)
                .map(|schema| Rc::from(Context::new(get_lables(schema), None)));

            let source_context = contextualize_query(schema_map, source);

            Context::concat(table_context, source_context)
        }
        Statement::DropTable { names, .. } => names
            .iter()
            .map(|name| {
                let schema = schema_map.get(name);
                schema.map(|schema| Rc::from(Context::new(get_lables(schema), None)))
            })
            .fold(None, Context::concat),
        _ => None,
    }
}

fn contextualize_query<'a>(schema_map: &'a SchemaMap, query: &'a Query) -> Option<Rc<Context<'a>>> {
    let Query { body, .. } = query;
    match body {
        SetExpr::Select(select) => {
            let TableWithJoins { relation, joins } = &select.from;

            let by_table = match relation {
                TableFactor::Table { name, .. } => {
                    let schema = schema_map.get(name);
                    schema.map(|schema| Rc::from(Context::new(get_lables(schema), None)))
                }
                TableFactor::Derived { subquery, .. } => contextualize_query(schema_map, subquery),
                TableFactor::Series { .. } | TableFactor::Dictionary { .. } => None,
            }
            .map(Rc::from);

            let by_joins = joins
                .iter()
                .map(|Join { relation, .. }| contextualize_table_factor(schema_map, relation))
                .fold(None, Context::concat);

            Context::concat(by_table, by_joins)
        }
        SetExpr::Values(_) => None,
    }
}

fn contextualize_table_factor<'a>(
    schema_map: &'a SchemaMap,
    table_factor: &'a TableFactor,
) -> Option<Rc<Context<'a>>> {
    match table_factor {
        TableFactor::Table { name, .. } => {
            let schema = schema_map.get(name);
            schema.map(|schema| Rc::from(Context::new(get_lables(schema), None)))
        }
        TableFactor::Derived { subquery, .. } => contextualize_query(schema_map, subquery),
        TableFactor::Series { .. } | TableFactor::Dictionary { .. } => None,
    }
    .map(Rc::from)
}
