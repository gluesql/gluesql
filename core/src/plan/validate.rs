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
    std::{collections::HashMap, ops::Add, rc::Rc},
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
                                .unwrap_or(SchemaCount::Zero);

                            if let SchemaCount::Duplicated = tables_with_given_col {
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
        table_name: &'a str,
        alias: Option<&'a TableAlias>,
        schema: &'a Schema,
        next: Option<Rc<ValidationContext<'a>>>,
    },
    Bridge {
        left: Rc<ValidationContext<'a>>,
        right: Rc<ValidationContext<'a>>,
    },
}

#[derive(Debug)]
enum SchemaCount {
    Zero,
    One,
    Duplicated,
}

impl Add for SchemaCount {
    type Output = Self;

    fn add(self, rhs: Self) -> Self {
        match (self, rhs) {
            (Self::Zero, Self::Zero) => Self::Zero,
            (Self::Zero, Self::One) => Self::One,
            (Self::One, Self::Zero) => Self::One,
            (Self::One, Self::One) => Self::Duplicated,
            (Self::Duplicated, _) => Self::Duplicated,
            (_, Self::Duplicated) => Self::Duplicated,
        }
    }
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

    fn count(&self, column_name: &str) -> SchemaCount {
        match self {
            ValidationContext::Data { schema, next, .. } => {
                let current = schema
                    .column_defs
                    .as_ref()
                    .map(|column_defs| {
                        if column_defs.iter().any(|column| column.name == column_name) {
                            SchemaCount::One
                        } else {
                            SchemaCount::Zero
                        }
                    })
                    .unwrap_or(SchemaCount::Zero);

                let next = next
                    .as_ref()
                    .map(|context| context.count(column_name))
                    .unwrap_or(SchemaCount::Zero);

                current + next
            }
            ValidationContext::Bridge { left, right } => {
                left.count(column_name) + right.count(column_name)
            }
        }
    }
}

type SchemaMap = HashMap<String, Schema>;
pub fn contextualize_stmt<'a>(
    schema_map: &'a SchemaMap,
    statement: &'a Statement,
) -> Option<Rc<ValidationContext<'a>>> {
    match statement {
        Statement::Query(query) => contextualize_query(schema_map, query),
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

            let source_context = contextualize_query(schema_map, source);

            ValidationContext::concat(table_context, source_context)
        }
        Statement::DropTable { names, .. } => names
            .iter()
            .map(|name| {
                let schema = schema_map.get(name);
                schema.map(|schema| Rc::from(ValidationContext::new(name, None, schema, None)))
            })
            .fold(None, ValidationContext::concat),
        _ => None,
    }
}

fn contextualize_query<'a>(
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
                TableFactor::Derived { subquery, .. } => contextualize_query(schema_map, subquery),
                TableFactor::Series { .. } | TableFactor::Dictionary { .. } => None,
            }
            .map(Rc::from);

            let by_joins = joins
                .iter()
                .map(|Join { relation, .. }| contextualize_table_factor(schema_map, relation))
                .fold(None, ValidationContext::concat);

            ValidationContext::concat(by_table, by_joins)
        }
        SetExpr::Values(_) => None,
    }
}

fn contextualize_table_factor<'a>(
    schema_map: &'a SchemaMap,
    table_factor: &'a TableFactor,
) -> Option<Rc<ValidationContext<'a>>> {
    match table_factor {
        TableFactor::Table { name, alias, .. } => {
            let schema = schema_map.get(name);
            schema
                .map(|schema| Rc::from(ValidationContext::new(name, alias.as_ref(), schema, None)))
        }
        TableFactor::Derived { subquery, .. } => contextualize_query(schema_map, subquery),
        TableFactor::Series { .. } | TableFactor::Dictionary { .. } => None,
    }
    .map(Rc::from)
}
