use {
    super::SourceRows,
    crate::{
        data::{Key, Row},
        executor::{
            context::RowContext,
            evaluate::evaluate,
            fetch::{FetchError, fetch_columns},
        },
        plan::{TableAccessPlan, TableSourcePlan},
        result::Result,
        store::GStore,
    },
    std::{iter, rc::Rc},
};

pub(super) fn execute<'a, T: GStore>(
    storage: &'a T,
    table: &'a TableSourcePlan,
    evaluation_context: Option<&Rc<RowContext<'a>>>,
) -> Result<SourceRows<'a>> {
    let columns = columns(storage, table)?;
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
            let schema = storage
                .fetch_schema(&table.name)?
                .ok_or(FetchError::Unreachable)?;
            let evaluated = evaluate(storage, evaluation_context, None, expr)?;
            let column_def = schema
                .column_defs
                .as_ref()
                .and_then(|column_defs| {
                    column_defs
                        .iter()
                        .find(|column_def| column_def.unique.is_some_and(|u| u.is_primary))
                })
                .ok_or(FetchError::Unreachable)?;
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
                    let evaluated = evaluate(storage, None, None, &predicate.expr)?;

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

    Ok(SourceRows {
        alias: table
            .alias
            .as_ref()
            .map_or(table.name.as_str(), |alias| alias.name.as_str()),
        columns,
        rows,
    })
}

pub(super) fn columns<T: GStore>(storage: &T, table: &TableSourcePlan) -> Result<Rc<[String]>> {
    let columns = fetch_columns(storage, &table.name)?;
    let columns = match &table.alias {
        None => columns,
        Some(alias) if alias.columns.len() > columns.len() => {
            return Err(FetchError::TooManyColumnAliases(
                table.name.clone(),
                columns.len(),
                alias.columns.len(),
            )
            .into());
        }
        Some(alias) => alias
            .columns
            .iter()
            .cloned()
            .chain(columns[alias.columns.len()..].iter().cloned())
            .collect(),
    };

    Ok(Rc::from(columns))
}
