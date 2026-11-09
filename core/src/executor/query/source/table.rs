use {
    super::{
        super::{QueryError, SourceColumns},
        PreparedSource, SourceRows,
    },
    crate::{
        data::{Key, Row},
        executor::{
            context::{RowContext, StatementContext},
            evaluate::evaluate,
            fetch::fetch_columns,
        },
        plan::{TableAccessPlan, TableSourcePlan},
        result::Result,
        store::GStore,
    },
    std::{iter, rc::Rc},
};

pub(super) fn execute<'a, T: GStore>(
    storage: &'a T,
    stmt_context: StatementContext<'a>,
    table: &'a TableSourcePlan,
) -> Result<PreparedSource<'a>> {
    let names = fetch_columns(stmt_context, &table.name)?;
    let names = match &table.alias {
        None => names,
        Some(alias) if alias.columns.len() > names.len() => {
            return Err(QueryError::TooManyColumnAliases(
                table.name.clone(),
                names.len(),
                alias.columns.len(),
            )
            .into());
        }
        Some(alias) => alias
            .columns
            .iter()
            .cloned()
            .chain(names[alias.columns.len()..].iter().cloned())
            .collect(),
    };

    let output = SourceColumns {
        alias: table
            .alias
            .as_ref()
            .map_or(table.name.as_str(), |alias| alias.name.as_str()),
        names: Rc::from(names),
    };
    let source = SourceColumns {
        alias: output.alias,
        names: Rc::clone(&output.names),
    };
    let rows = Box::new(move |evaluation_context: Option<Rc<RowContext<'a>>>| {
        rows(
            storage,
            stmt_context,
            SourceColumns {
                alias: source.alias,
                names: Rc::clone(&source.names),
            },
            evaluation_context.as_ref(),
            table,
        )
    });

    Ok(PreparedSource { output, rows })
}

fn rows<'a, T: GStore>(
    storage: &'a T,
    stmt_context: StatementContext<'a>,
    source: SourceColumns<'a>,
    evaluation_context: Option<&Rc<RowContext<'a>>>,
    table: &'a TableSourcePlan,
) -> Result<SourceRows<'a>> {
    let columns = Rc::clone(&source.names);
    let rows = match &table.access {
        TableAccessPlan::FullScan => {
            let rows = storage.scan_data(&table.name)?.map({
                let columns = Rc::clone(&columns);

                move |row| {
                    let (_, values) = row?;
                    Ok(Row {
                        columns: Rc::clone(&columns),
                        values,
                    })
                }
            });

            Box::new(rows) as Box<dyn Iterator<Item = Result<Row>> + 'a>
        }
        TableAccessPlan::PrimaryKey { expr } => {
            let schema = stmt_context.schema_map.get(&table.name).ok_or_else(|| {
                QueryError::UnreachableTableSchemaMissingAfterPreparation(table.name.clone())
            })?;
            let evaluated = evaluate(storage, stmt_context, evaluation_context, None, expr)?;
            let column_def = schema
                .column_defs
                .as_ref()
                .and_then(|column_defs| {
                    column_defs
                        .iter()
                        .find(|column_def| column_def.unique.is_some_and(|u| u.is_primary))
                })
                .ok_or_else(|| QueryError::PrimaryKeyColumnNotFound(table.name.clone()))?;
            let value = evaluated.try_into_value(&column_def.data_type, column_def.nullable)?;
            let key = Key::try_from(value)?;

            match storage.fetch_data(&table.name, &key)? {
                Some(values) => Box::new(iter::once(Ok(Row {
                    columns: Rc::clone(&columns),
                    values,
                }))) as Box<dyn Iterator<Item = Result<Row>> + 'a>,
                None => Box::new(iter::empty()) as Box<dyn Iterator<Item = Result<Row>> + 'a>,
            }
        }
        TableAccessPlan::Index {
            name,
            asc,
            predicate,
        } => {
            let predicate = match predicate {
                Some(predicate) => {
                    let evaluated = evaluate(storage, stmt_context, None, None, &predicate.expr)?;

                    Some((&predicate.operator, evaluated.try_into()?))
                }
                None => None,
            };
            let rows = storage
                .scan_indexed_data(&table.name, name, *asc, predicate)?
                .map({
                    let columns = Rc::clone(&columns);

                    move |row| {
                        let (_, values) = row?;
                        Ok(Row {
                            columns: Rc::clone(&columns),
                            values,
                        })
                    }
                });

            Box::new(rows)
        }
    };

    Ok(SourceRows { source, rows })
}
