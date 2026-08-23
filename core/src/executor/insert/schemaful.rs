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

// Column positions for one foreign key, resolved against both tables' schemas.
struct ResolvedForeignKey {
    referencing_indexes: Vec<usize>,
    referenced_indexes: Vec<usize>,
    key_position: usize,
}

fn resolve_foreign_key(
    foreign_key: &ForeignKey,
    column_defs: &[ColumnDef],
    referenced_column_defs: &[ColumnDef],
) -> Result<ResolvedForeignKey> {
    let index_of = |defs: &[ColumnDef], name: &str| {
        defs.iter()
            .position(|column_def| column_def.name == name)
            .ok_or_else(|| InsertError::UnreachableForeignKeyColumn(name.to_owned()))
    };

    let referencing_indexes = foreign_key
        .referencing_column_names
        .iter()
        .map(|name| index_of(column_defs, name))
        .collect::<Result<Vec<_>, _>>()?;
    let referenced_indexes = foreign_key
        .referenced_column_names
        .iter()
        .map(|name| index_of(referenced_column_defs, name))
        .collect::<Result<Vec<_>, _>>()?;

    let key_position = foreign_key
        .referenced_column_names
        .iter()
        .position(|name| {
            referenced_column_defs.iter().any(|column_def| {
                column_def.name == *name
                    && column_def.unique == Some(ColumnUniqueOption { is_primary: true })
            })
        })
        .ok_or_else(|| {
            InsertError::UnreachableForeignKeyColumn(foreign_key.referenced_column_names.join(", "))
        })?;

    Ok(ResolvedForeignKey {
        referencing_indexes,
        referenced_indexes,
        key_position,
    })
}

fn validate_foreign_key<T: GStore>(
    storage: &T,
    column_defs: &Rc<[ColumnDef]>,
    foreign_keys: Vec<ForeignKey>,
    rows: &[Vec<Value>],
) -> Result<()> {
    for foreign_key in foreign_keys {
        let ForeignKey {
            referenced_table_name,
            referenced_column_names,
            ..
        } = &foreign_key;

        let referenced_column_defs = storage
            .fetch_schema(referenced_table_name)?
            .and_then(|schema| schema.column_defs)
            .unwrap_or_default();

        let ResolvedForeignKey {
            referencing_indexes,
            referenced_indexes,
            key_position,
        } = resolve_foreign_key(&foreign_key, column_defs, &referenced_column_defs)?;

        for row in rows {
            let values = referencing_indexes
                .iter()
                .map(|index| {
                    row.get(*index).ok_or_else(|| {
                        InsertError::UnreachableForeignKeyColumn(
                            foreign_key.referencing_column_names.join(", "),
                        )
                    })
                })
                .collect::<Result<Vec<_>, _>>()?;

            // MATCH SIMPLE: a NULL anywhere in the referencing tuple satisfies the constraint.
            if values.iter().any(|value| *value == &Value::Null) {
                continue;
            }

            let referenced_row =
                storage.fetch_data(referenced_table_name, &Key::try_from(values[key_position])?)?;

            let matched = referenced_row.is_some_and(|referenced_row| {
                values.iter().enumerate().all(|(position, value)| {
                    position == key_position
                        || referenced_row.get(referenced_indexes[position]) == Some(*value)
                })
            });

            if !matched {
                let referenced_value = values
                    .iter()
                    .map(|value| String::from((*value).clone()))
                    .collect::<Vec<_>>()
                    .join(", ");

                return Err(InsertError::CannotFindReferencedValue {
                    table_name: referenced_table_name.to_owned(),
                    column_name: referenced_column_names.join(", "),
                    referenced_value,
                }
                .into());
            }
        }
    }

    Ok(())
}
