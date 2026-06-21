use {
    super::{
        select::select,
        validate::{ColumnValidation, validate_unique},
    },
    crate::{
        ast::{ColumnDef, ColumnUniqueOption, ForeignKey},
        data::{Key, Row, SCHEMALESS_DOC_COLUMN, Schema, Value, value::BTreeMapJsonExt},
        executor::{evaluate::evaluate_stateless, limit::Limit},
        plan::{ExprPlan, QueryPlan, SetExprPlan, ValuesPlan, plan_scalar_expr},
        result::{Error, Result},
        store::{GStore, GStoreMut},
    },
    serde::Serialize,
    std::{collections::BTreeMap, fmt::Debug, rc::Rc},
    thiserror::Error as ThisError,
};

#[derive(ThisError, Serialize, Debug, PartialEq, Eq)]
pub enum InsertError {
    #[error("table not found: {0}")]
    TableNotFound(String),

    #[error("lack of required column: {0}")]
    LackOfRequiredColumn(String),

    #[error("wrong column name: {0}")]
    WrongColumnName(String),

    #[error("column and values not matched")]
    ColumnAndValuesNotMatched,

    #[error("literals have more values than target columns")]
    TooManyValues,

    #[error("only single value accepted for schemaless row insert: got {0}")]
    OnlySingleValueAcceptedForSchemalessRow(usize),

    #[error("map type required: {0}")]
    MapTypeValueRequired(String),

    #[error(
        "cannot find referenced value on {table_name}.{column_name} with value {referenced_value:?}"
    )]
    CannotFindReferencedValue {
        table_name: String,
        column_name: String,
        referenced_value: String,
    },

    #[error("unreachable referencing column name: {0}")]
    ConflictReferencingColumnName(String),
}

enum RowsData {
    Append(Vec<Vec<Value>>),
    Insert(Vec<(Key, Vec<Value>)>),
}

pub fn insert<T: GStore + GStoreMut>(
    storage: &mut T,
    table_name: &str,
    columns: &[String],
    source: &QueryPlan,
) -> Result<usize> {
    let Schema {
        column_defs,
        foreign_keys,
        ..
    } = storage
        .fetch_schema(table_name)?
        .ok_or_else(|| InsertError::TableNotFound(table_name.to_owned()))?;

    let rows = match column_defs {
        Some(column_defs) => fetch_vec_rows(
            storage,
            table_name,
            column_defs,
            columns,
            source,
            foreign_keys,
        ),
        None => fetch_schemaless_rows(storage, source).map(RowsData::Append),
    }?;

    match rows {
        RowsData::Append(rows) => {
            let num_rows = rows.len();

            storage.append_data(table_name, rows).map(|()| num_rows)
        }
        RowsData::Insert(rows) => {
            let num_rows = rows.len();

            storage.insert_data(table_name, rows).map(|()| num_rows)
        }
    }
}

fn fetch_vec_rows<T: GStore>(
    storage: &T,
    table_name: &str,
    column_defs: Vec<ColumnDef>,
    columns: &[String],
    source: &QueryPlan,
    foreign_keys: Vec<ForeignKey>,
) -> Result<RowsData> {
    let labels = Rc::from(
        column_defs
            .iter()
            .map(|column_def| column_def.name.clone())
            .collect::<Vec<_>>(),
    );
    let column_defs = Rc::from(column_defs);
    let column_validation = ColumnValidation::All(&column_defs);

    let rows_iter: Box<dyn Iterator<Item = Result<Vec<Value>>> + '_> = match &source.body {
        SetExprPlan::Values(ValuesPlan(values_list)) => {
            let limit = Limit::new(source.limit.as_ref(), source.offset.as_ref())?;
            let column_defaults: Rc<[Option<ExprPlan>]> = Rc::from(
                column_defs
                    .iter()
                    .map(|column_def| column_def.default.clone().map(plan_scalar_expr))
                    .collect::<Vec<_>>(),
            );
            let column_defs = Rc::clone(&column_defs);
            let labels = Rc::clone(&labels);
            let rows = values_list.iter().map(move |values| {
                let column_defs = Rc::clone(&column_defs);
                let column_defaults = Rc::clone(&column_defaults);
                let labels = Rc::clone(&labels);

                Ok(Row {
                    columns: labels,
                    values: fill_values(&column_defs, &column_defaults, columns, values)?,
                })
            });
            let rows = limit.apply(rows);
            let rows = rows.map(|row| Ok::<_, Error>(row?.into_values()));

            Box::new(rows) as Box<dyn Iterator<Item = Result<Vec<Value>>> + '_>
        }
        SetExprPlan::Select(_) => {
            let rows = select(storage, source, None)?.map(|row| {
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

            Box::new(rows) as Box<dyn Iterator<Item = Result<Vec<Value>>> + '_>
        }
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

fn fetch_schemaless_rows<T: GStore>(storage: &T, source: &QueryPlan) -> Result<Vec<Vec<Value>>> {
    let doc_column: Rc<[String]> = Rc::from(vec![SCHEMALESS_DOC_COLUMN.to_owned()]);

    let rows_iter: Box<dyn Iterator<Item = Result<Vec<Value>>> + '_> = match &source.body {
        SetExprPlan::Values(ValuesPlan(values_list)) => {
            let limit = Limit::new(source.limit.as_ref(), source.offset.as_ref())?;
            let rows = values_list.iter().map({
                let doc_column = Rc::clone(&doc_column);
                move |values| {
                    let doc_column = Rc::clone(&doc_column);

                    if values.len() > 1 {
                        return Err(InsertError::OnlySingleValueAcceptedForSchemalessRow(
                            values.len(),
                        )
                        .into());
                    }

                    let Some(value) = values.first() else {
                        return Err(InsertError::OnlySingleValueAcceptedForSchemalessRow(0).into());
                    };

                    let map: BTreeMap<String, Value> =
                        evaluate_stateless(None, value)?.try_into()?;

                    Ok(Row {
                        columns: doc_column,
                        values: vec![Value::Map(map)],
                    })
                }
            });
            let rows = limit.apply(rows);
            let rows = rows.map(|row| row.map(Row::into_values));

            Box::new(rows) as Box<dyn Iterator<Item = Result<Vec<Value>>> + '_>
        }
        SetExprPlan::Select(_) => {
            let rows = select(storage, source, None)?.map(|row| {
                let values = row?.into_values();

                if values.len() > 1 {
                    return Err(
                        InsertError::OnlySingleValueAcceptedForSchemalessRow(values.len()).into(),
                    );
                }

                let map = match values.into_iter().next() {
                    None => {
                        return Err(InsertError::OnlySingleValueAcceptedForSchemalessRow(0).into());
                    }
                    Some(Value::Map(map)) => map,
                    Some(Value::Str(s)) => BTreeMap::parse_json_object(&s)?,
                    Some(v) => return Err(InsertError::MapTypeValueRequired((&v).into()).into()),
                };

                Ok(vec![Value::Map(map)])
            });

            Box::new(rows) as Box<dyn Iterator<Item = Result<Vec<Value>>> + '_>
        }
    };
    let rows = rows_iter.collect::<Result<Vec<Vec<Value>>>>()?;

    Ok(rows)
}

fn fill_values(
    column_defs: &[ColumnDef],
    column_defaults: &[Option<ExprPlan>],
    columns: &[String],
    values: &[ExprPlan],
) -> Result<Vec<Value>> {
    #[derive(iter_enum::Iterator)]
    enum Columns<I1, I2> {
        All(I1),
        Specified(I2),
    }

    if !columns.is_empty() && values.len() != columns.len() {
        return Err(InsertError::ColumnAndValuesNotMatched.into());
    } else if values.len() > column_defs.len() {
        return Err(InsertError::TooManyValues.into());
    }

    if let Some(wrong_column_name) = columns.iter().find(|column_name| {
        !column_defs
            .iter()
            .any(|column_def| &&column_def.name == column_name)
    }) {
        return Err(InsertError::WrongColumnName(wrong_column_name.to_owned()).into());
    }

    let columns = if columns.is_empty() {
        Columns::All(column_defs.iter().map(|ColumnDef { name, .. }| name))
    } else {
        Columns::Specified(columns.iter())
    };

    let column_name_value_list = columns.zip(values.iter()).collect::<Vec<(_, _)>>();

    let values = column_defs
        .iter()
        .zip(column_defaults)
        .map(|(column_def, default)| {
            let ColumnDef {
                name: def_name,
                data_type,
                nullable,
                ..
            } = column_def;

            let value = column_name_value_list
                .iter()
                .find(|(name, _)| name == &def_name)
                .map(|(_, value)| value);

            match (value, default, nullable) {
                (Some(expr), _, _) => {
                    evaluate_stateless(None, expr)?.try_into_value(data_type, *nullable)
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
        .collect::<Result<Vec<Value>>>()?;

    Ok(values)
}
