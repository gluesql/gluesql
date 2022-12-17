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
    std::collections::HashMap,
};

/// Validate user select column should not be ambiguous
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

type SchemaContext<'a> = HashMap<(&'a String, &'a Option<TableAlias>), &'a Schema>;
type SchemaMap = HashMap<String, Schema>;

pub fn update_schema_context<'a>(
    schema_map: &'a SchemaMap,
    statement: &'a Statement,
) -> SchemaContext<'a> {
    match statement {
        Statement::Query(query) => update_schema_context_by_query(schema_map, query),
        Statement::Insert {
            table_name, source, ..
        } => {
            let table_schema = schema_map
                .get(table_name)
                .map(|schema| HashMap::from([((table_name, &None), schema)]))
                .unwrap_or_else(HashMap::new);
            let source_schema_list = update_schema_context_by_query(schema_map, source);

            table_schema.into_iter().chain(source_schema_list).collect()
        }
        Statement::DropTable { names, .. } => names
            .iter()
            .flat_map(|name| {
                let schema = schema_map.get(name);
                schema
                    .map(|schema| HashMap::from([((name, &None), schema)]))
                    .unwrap_or_else(HashMap::new)
            })
            .collect(),
        _ => HashMap::new(),
    }
}

fn update_schema_context_by_query<'a>(
    schema_map: &'a SchemaMap,
    query: &'a Query,
) -> SchemaContext<'a> {
    let Query { body, .. } = query;
    match body {
        SetExpr::Select(select) => {
            // todo!()
            let TableWithJoins { relation, joins } = &select.from;

            let by_table = match relation {
                TableFactor::Table { name, alias, .. } => {
                    let schema = schema_map.get(name);
                    schema
                        .map(|schema| HashMap::from([((name, alias), schema)]))
                        .unwrap_or_default()
                }
                TableFactor::Derived { subquery, .. } => {
                    update_schema_context_by_query(schema_map, subquery)
                }
                TableFactor::Series { .. } | TableFactor::Dictionary { .. } => HashMap::new(),
            };

            let by_joins = joins
                .iter()
                .flat_map(|Join { relation, .. }| match relation {
                    TableFactor::Table { name, alias, .. } => {
                        let schema = schema_map.get(name);
                        schema
                            .map(|schema| HashMap::from([((name, alias), schema)]))
                            .unwrap_or_default()
                    }
                    TableFactor::Derived { subquery, .. } => {
                        update_schema_context_by_query(schema_map, subquery)
                    }
                    TableFactor::Series { .. } | TableFactor::Dictionary { .. } => HashMap::new(),
                });

            by_joins.chain(by_table).collect()
        }
        SetExpr::Values(_) => HashMap::new(),
    }
}
