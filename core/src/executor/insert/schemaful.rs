use {
    super::{InsertError, RowsData, values},
    crate::{
        ast::{ColumnUniqueOption, ForeignKey},
        data::{Key, Value},
        executor::{
            context::StatementContext,
            evaluate::{evaluate, evaluate_stateless},
            query,
            unique::create_unique_constraints,
        },
        plan::{ColumnDefPlan, ExprPlan, QueryPlan, ValuesPlan},
        result::Result,
        store::{GStore, ScanColumns},
    },
    std::{collections::HashSet, rc::Rc},
};

pub(super) fn fetch_rows<T: GStore>(
    storage: &T,
    stmt_context: StatementContext<'_>,
    table_name: &str,
    column_defs: &[ColumnDefPlan],
    columns: &[String],
    foreign_keys: Vec<ForeignKey>,
    source: &QueryPlan,
) -> Result<RowsData> {
    let column_defs = Rc::<[ColumnDefPlan]>::from(column_defs);

    let rows_iter: Box<dyn Iterator<Item = Result<Vec<Value>>> + '_> = if let Some(rows) =
        values::execute(
            storage,
            stmt_context,
            |plan| {
                values_rows(
                    storage,
                    stmt_context,
                    Rc::clone(&column_defs),
                    columns,
                    plan,
                )
            },
            source,
        )? {
        let rows = rows.map({
            let column_defs = Rc::clone(&column_defs);

            move |row| {
                let values = row?.into_values();

                assign_values(&column_defs, columns, &values)
            }
        });

        Box::new(rows)
    } else {
        let rows = query::execute(storage, stmt_context, None, source)?.map(|row| {
            let values = row?.into_values();

            column_defs
                .iter()
                .zip(values.iter())
                .try_for_each(|(column_def, value)| {
                    let ColumnDefPlan {
                        data_type,
                        nullable,
                        ..
                    } = column_def;

                    value.validate_type(data_type)?;
                    value.validate_null(*nullable)
                })?;

            Ok(values)
        });

        Box::new(rows)
    };
    let rows = rows_iter.collect::<Result<Vec<Vec<Value>>>>()?;

    let primary_key = column_defs
        .iter()
        .position(|column_def| column_def.unique == Some(ColumnUniqueOption { is_primary: true }));

    validate_unique(storage, table_name, &column_defs, primary_key, &rows)?;
    validate_foreign_key(storage, &column_defs, foreign_keys, &rows)?;

    match primary_key {
        Some(i) => rows
            .into_iter()
            .filter_map(|values: Vec<Value>| {
                values
                    .get(i)
                    .map(Key::try_from)
                    .map(|result| result.map(|key| (key, values)))
            })
            .collect::<Result<Vec<_>>>()
            .map(RowsData::Insert),
        None => Ok(RowsData::Append(rows)),
    }
}

fn validate_unique<T: GStore>(
    storage: &T,
    table_name: &str,
    column_defs: &[ColumnDefPlan],
    primary_key: Option<usize>,
    rows: &[Vec<Value>],
) -> Result<()> {
    if let Some(index) = primary_key
        && !column_defs
            .iter()
            .any(|column_def| column_def.unique == Some(ColumnUniqueOption { is_primary: false }))
    {
        let mut seen_keys = HashSet::new();
        for key in rows
            .iter()
            .filter_map(|row| row.get(index).map(Key::try_from))
        {
            let key = key?;
            if !seen_keys.insert(key.clone()) || storage.fetch_data(table_name, &key)?.is_some() {
                return Err(InsertError::DuplicateEntryOnPrimaryKeyField(key).into());
            }
        }

        return Ok(());
    }

    let unique_columns = column_defs
        .iter()
        .enumerate()
        .filter_map(|(index, column)| column.unique.map(|_| (index, column.name.clone())))
        .collect();

    let constraints = create_unique_constraints(unique_columns, &rows.iter().map(Vec::as_slice))?;
    if constraints.is_empty() {
        return Ok(());
    }

    for row in storage.scan_data(table_name, ScanColumns::All)? {
        let (_, values) = row?;
        for constraint in &constraints {
            let key = constraint.key_from_row(&values)?;
            constraint.check_duplicate(&key)?;
        }
    }

    Ok(())
}

fn values_rows<'a, T: GStore>(
    storage: &'a T,
    stmt_context: StatementContext<'a>,
    column_defs: Rc<[ColumnDefPlan]>,
    columns: &'a [String],
    ValuesPlan(values_list): &'a ValuesPlan,
) -> values::EvaluatedRows<'a> {
    let rows = values_list
        .iter()
        .map(move |exprs| evaluate_values(storage, stmt_context, &column_defs, columns, exprs));

    Box::new(rows)
}

fn evaluate_values<T: GStore>(
    storage: &T,
    stmt_context: StatementContext<'_>,
    column_defs: &[ColumnDefPlan],
    columns: &[String],
    exprs: &[ExprPlan],
) -> Result<Vec<Value>> {
    if !columns.is_empty() && exprs.len() != columns.len() {
        return Err(InsertError::ColumnAndValuesNotMatched.into());
    } else if exprs.len() > column_defs.len() {
        return Err(InsertError::TooManyValues.into());
    }

    if let Some(wrong_column_name) = columns.iter().find(|column_name| {
        !column_defs
            .iter()
            .any(|column_def| &&column_def.name == column_name)
    }) {
        return Err(InsertError::WrongColumnName(wrong_column_name.to_owned()).into());
    }

    if columns.is_empty() {
        column_defs
            .iter()
            .zip(exprs)
            .map(|(column_def, expr)| evaluate_value(storage, stmt_context, column_def, expr))
            .collect()
    } else {
        columns
            .iter()
            .zip(exprs)
            .map(|(name, expr)| {
                let column_def = column_defs
                    .iter()
                    .find(|column_def| &column_def.name == name)
                    .ok_or_else(|| InsertError::WrongColumnName(name.to_owned()))?;

                evaluate_value(storage, stmt_context, column_def, expr)
            })
            .collect()
    }
}

fn evaluate_value<T: GStore>(
    storage: &T,
    stmt_context: StatementContext<'_>,
    column_def: &ColumnDefPlan,
    expr: &ExprPlan,
) -> Result<Value> {
    evaluate(storage, stmt_context, None, None, expr)?
        .try_into_value(&column_def.data_type, column_def.nullable)
}

fn assign_values(
    column_defs: &[ColumnDefPlan],
    columns: &[String],
    values: &[Value],
) -> Result<Vec<Value>> {
    column_defs
        .iter()
        .enumerate()
        .map(|(index, column_def)| {
            let ColumnDefPlan {
                name: def_name,
                data_type,
                nullable,
                default,
                ..
            } = column_def;
            let value = if columns.is_empty() {
                values.get(index)
            } else {
                columns
                    .iter()
                    .position(|column| column == def_name)
                    .and_then(|index| values.get(index))
            };

            match (value, default, nullable) {
                (Some(value), _, nullable) => {
                    value.validate_type(data_type)?;
                    value.validate_null(*nullable)?;

                    Ok(value.clone())
                }
                (None, Some(expr), _) => {
                    evaluate_stateless(None, expr.planned())?.try_into_value(data_type, *nullable)
                }
                (None, None, true) => Ok(Value::Null),
                (None, None, false) => {
                    Err(InsertError::LackOfRequiredColumn(def_name.to_owned()).into())
                }
            }
        })
        .collect()
}

fn validate_foreign_key<T: GStore>(
    storage: &T,
    column_defs: &Rc<[ColumnDefPlan]>,
    foreign_keys: Vec<ForeignKey>,
    rows: &[Vec<Value>],
) -> Result<()> {
    for foreign_key in foreign_keys {
        let ForeignKey {
            referencing_column_name,
            referenced_table_name,
            referenced_column_name,
            ..
        } = &foreign_key;

        let target_index = column_defs
            .iter()
            .enumerate()
            .find(|(_, c)| &c.name == referencing_column_name)
            .ok_or_else(|| {
                InsertError::ConflictReferencingColumnName(referencing_column_name.to_owned())
            })?;

        for row in rows {
            let value =
                row.get(target_index.0)
                    .ok_or(InsertError::ConflictReferencingColumnName(
                        referencing_column_name.to_owned(),
                    ))?;

            if value == &Value::Null {
                continue;
            }

            let no_referenced = storage
                .fetch_data(referenced_table_name, &Key::try_from(value)?)?
                .is_none();

            if no_referenced {
                return Err(InsertError::CannotFindReferencedValue {
                    table_name: referenced_table_name.to_owned(),
                    column_name: referenced_column_name.to_owned(),
                    referenced_value: String::from(value),
                }
                .into());
            }
        }
    }

    Ok(())
}
