use {
    super::{InsertError, RowsData, values},
    crate::{
        ast::{ColumnDef, ColumnUniqueOption, ForeignKey},
        data::{Key, Value},
        executor::{
            evaluate::evaluate_stateless,
            query,
            validate::{ColumnValidation, validate_unique},
        },
        plan::{ExprPlan, QueryPlan, ValuesPlan, plan_scalar_expr},
        result::Result,
        store::GStore,
    },
    std::rc::Rc,
};

pub(super) fn fetch_rows<T: GStore>(
    storage: &T,
    table_name: &str,
    column_defs: Vec<ColumnDef>,
    columns: &[String],
    source: &QueryPlan,
    foreign_keys: Vec<ForeignKey>,
) -> Result<RowsData> {
    let column_defaults: Rc<[Option<ExprPlan>]> = Rc::from(
        column_defs
            .iter()
            .map(|column_def| column_def.default.clone().map(plan_scalar_expr))
            .collect::<Vec<_>>(),
    );
    let column_defs = Rc::from(column_defs);
    let column_validation = ColumnValidation::All(&column_defs);

    let rows_iter: Box<dyn Iterator<Item = Result<Vec<Value>>> + '_> = if let Some(rows) =
        values::execute(source, |plan| {
            values_rows(plan, Rc::clone(&column_defs), columns)
        })? {
        let rows = rows.map({
            let column_defs = Rc::clone(&column_defs);

            move |row| {
                let values = row?.into_values();

                assign_values(&column_defs, &column_defaults, columns, &values)
            }
        });

        Box::new(rows)
    } else {
        let rows = query::execute(storage, source, None)?.map(|row| {
            let values = row?.into_values();

            column_defs
                .iter()
                .zip(values.iter())
                .try_for_each(|(column_def, value)| {
                    let ColumnDef {
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

    validate_unique(
        storage,
        table_name,
        &column_validation,
        rows.iter().map(std::vec::Vec::as_slice),
    )?;

    validate_foreign_key(storage, &column_defs, foreign_keys, &rows)?;

    let primary_key = column_defs.iter().position(|ColumnDef { unique, .. }| {
        unique == &Some(ColumnUniqueOption { is_primary: true })
    });

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

fn values_rows<'a>(
    ValuesPlan(values_list): &'a ValuesPlan,
    column_defs: Rc<[ColumnDef]>,
    columns: &'a [String],
) -> values::EvaluatedRows<'a> {
    let rows = values_list
        .iter()
        .map(move |exprs| evaluate_values(&column_defs, columns, exprs));

    Box::new(rows)
}

fn evaluate_values(
    column_defs: &[ColumnDef],
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
            .map(|(column_def, expr)| evaluate_value(column_def, expr))
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

                evaluate_value(column_def, expr)
            })
            .collect()
    }
}

fn evaluate_value(column_def: &ColumnDef, expr: &ExprPlan) -> Result<Value> {
    evaluate_stateless(None, expr)?.try_into_value(&column_def.data_type, column_def.nullable)
}

fn assign_values(
    column_defs: &[ColumnDef],
    column_defaults: &[Option<ExprPlan>],
    columns: &[String],
    values: &[Value],
) -> Result<Vec<Value>> {
    column_defs
        .iter()
        .enumerate()
        .zip(column_defaults)
        .map(|((index, column_def), default)| {
            let ColumnDef {
                name: def_name,
                data_type,
                nullable,
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
                    evaluate_stateless(None, expr)?.try_into_value(data_type, *nullable)
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
    column_defs: &Rc<[ColumnDef]>,
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
