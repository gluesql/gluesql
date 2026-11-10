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
        store::{GStore, RowIter, ScanColumn, ScanColumns},
    },
    std::rc::Rc,
};

pub(super) fn execute<'a, T: GStore>(
    storage: &'a T,
    stmt_context: StatementContext<'a>,
    table: &'a TableSourcePlan,
) -> Result<PreparedSource<'a>> {
    let original_names = fetch_columns(stmt_context, &table.name)?;
    let names = match &table.alias {
        None => original_names.clone(),
        Some(alias) if alias.columns.len() > original_names.len() => {
            return Err(QueryError::TooManyColumnAliases(
                table.name.clone(),
                original_names.len(),
                alias.columns.len(),
            )
            .into());
        }
        Some(alias) => alias
            .columns
            .iter()
            .cloned()
            .chain(original_names[alias.columns.len()..].iter().cloned())
            .collect(),
    };
    let source = SourceColumns {
        alias: table
            .alias
            .as_ref()
            .map_or(table.name.as_str(), |alias| alias.name.as_str()),
        names: Rc::from(names),
    };

    let prepared = match &table.access {
        TableAccessPlan::FullScan => prepare_source(source, move |_| {
            storage.scan_data(&table.name, ScanColumns::All)
        }),
        TableAccessPlan::ProjectedScan { columns } => {
            let mut source = source;
            let (scan_columns, names): (Vec<ScanColumn>, Vec<String>) = original_names
                .into_iter()
                .enumerate()
                .filter(|(_, name)| columns.contains(name))
                .map(|(index, name)| ((name, index), source.names[index].clone()))
                .unzip();

            assert!(
                scan_columns.windows(2).all(|pair| pair[0].1 < pair[1].1),
                "scan column indexes must be strictly increasing"
            );

            source.names = Rc::from(names);

            prepare_source(source, move |_| {
                storage.scan_data(&table.name, ScanColumns::Selected(&scan_columns))
            })
        }
        TableAccessPlan::PrimaryKey { expr } => prepare_source(source, move |evaluation_context| {
            let schema = stmt_context.schema_map.get(&table.name).ok_or_else(|| {
                QueryError::UnreachableTableSchemaMissingAfterPreparation(table.name.clone())
            })?;
            let evaluated = evaluate(
                storage,
                stmt_context,
                evaluation_context.as_ref(),
                None,
                expr,
            )?;
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
            let row = storage
                .fetch_data(&table.name, &key)?
                .map(|values| (key, values));

            Ok(Box::new(row.into_iter().map(Ok)))
        }),
        TableAccessPlan::Index {
            name,
            asc,
            predicate,
        } => prepare_source(source, move |_| {
            let predicate = match predicate {
                Some(predicate) => {
                    let evaluated = evaluate(storage, stmt_context, None, None, &predicate.expr)?;

                    Some((&predicate.operator, evaluated.try_into()?))
                }
                None => None,
            };

            storage.scan_indexed_data(&table.name, name, *asc, predicate)
        }),
    };

    Ok(prepared)
}

fn prepare_source<'a>(
    source: SourceColumns<'a>,
    rows: impl Fn(Option<Rc<RowContext<'a>>>) -> Result<RowIter<'a>> + 'a,
) -> PreparedSource<'a> {
    let output = SourceColumns {
        alias: source.alias,
        names: Rc::clone(&source.names),
    };
    let rows = Box::new(move |evaluation_context| {
        let columns = Rc::clone(&source.names);
        let rows = rows(evaluation_context)?.map(move |row| {
            let (_, values) = row?;
            Ok(Row {
                columns: Rc::clone(&columns),
                values,
            })
        });

        Ok(SourceRows {
            source: SourceColumns {
                alias: source.alias,
                names: Rc::clone(&source.names),
            },
            rows: Box::new(rows),
        })
    });

    PreparedSource { output, rows }
}
