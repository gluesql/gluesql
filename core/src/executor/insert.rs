use {
    super::{
        select::select,
        validate::{validate_unique, ColumnValidation},
    },
    crate::{
        ast::{ColumnDef, ColumnUniqueOption, Expr, Query, SetExpr, Values},
        data::{Key, Row, Schema, Value},
        executor::{evaluate::evaluate_stateless, limit::Limit},
        result::{MutResult, Result, TrySelf},
        store::{GStore, GStoreMut},
    },
    futures::stream::{self, TryStreamExt},
    serde::Serialize,
    std::{fmt::Debug, rc::Rc},
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
}

pub async fn insert<T: GStore + GStoreMut>(
    storage: T,
    table_name: &str,
    columns: &[String],
    source: &Query,
) -> MutResult<T, usize> {
    enum RowsData {
        Append(Vec<Vec<Value>>),
        Insert(Vec<(Key, Vec<Value>)>),
    }

    let rows = (|| async {
        let Schema { column_defs, .. } = storage
            .fetch_schema(table_name)
            .await?
            .ok_or_else(|| InsertError::TableNotFound(table_name.to_owned()))?;
        let labels = Rc::from(
            column_defs
                .iter()
                .map(|column_def| column_def.name.to_owned())
                .collect::<Vec<_>>(),
        );
        let column_defs = Rc::from(column_defs);
        let column_validation = ColumnValidation::All(Rc::clone(&column_defs));

        #[derive(futures_enum::Stream)]
        enum Rows<I1, I2> {
            Values(I1),
            Select(I2),
        }

        let rows = match &source.body {
            SetExpr::Values(Values(values_list)) => {
                let limit = Limit::new(source.limit.as_ref(), source.offset.as_ref())?;
                let rows = values_list.iter().map(|values| {
                    Ok(Row {
                        columns: Rc::clone(&labels),
                        values: fill_values(&column_defs, columns, values)?,
                    })
                });
                let rows = stream::iter(rows);
                let rows = limit.apply(rows);
                let rows = rows.map_ok(Into::into);

                Rows::Values(rows)
            }
            SetExpr::Select(_) => {
                let rows = select(&storage, source, None).await?.and_then(|row| {
                    let column_defs = Rc::clone(&column_defs);

                    async move {
                        validate_row(&row, &column_defs)?;

                        Ok(row.into())
                    }
                });

                Rows::Select(rows)
            }
        }
        .try_collect::<Vec<Vec<Value>>>()
        .await?;

        validate_unique(
            &storage,
            table_name,
            column_validation,
            rows.iter().map(|values| values.as_slice()),
        )
        .await?;

        let primary_key = column_defs
            .iter()
            .enumerate()
            .find(|(_, ColumnDef { unique, .. })| {
                unique == &Some(ColumnUniqueOption { is_primary: true })
            })
            .map(|(i, _)| i);

        let rows = match primary_key {
            Some(i) => rows
                .into_iter()
                .filter_map(|values| {
                    values
                        .get(i)
                        .map(Key::try_from)
                        .map(|result| result.map(|key| (key, values)))
                })
                .collect::<Result<Vec<_>>>()
                .map(RowsData::Insert)?,
            None => RowsData::Append(rows),
        };

        Ok(rows)
    })()
    .await;

    match rows.try_self(storage)? {
        (storage, RowsData::Append(rows)) => {
            let num_rows = rows.len();

            storage
                .append_data(table_name, rows)
                .await
                .map(|(storage, _)| (storage, num_rows))
        }
        (storage, RowsData::Insert(rows)) => {
            let num_rows = rows.len();

            storage
                .insert_data(table_name, rows)
                .await
                .map(|(storage, _)| (storage, num_rows))
        }
    }
}

fn fill_values(
    column_defs: &[ColumnDef],
    columns: &[String],
    values: &[Expr],
) -> Result<Vec<Value>> {
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

    #[derive(iter_enum::Iterator)]
    enum Columns<I1, I2> {
        All(I1),
        Specified(I2),
    }

    let columns = if columns.is_empty() {
        Columns::All(column_defs.iter().map(|ColumnDef { name, .. }| name))
    } else {
        Columns::Specified(columns.iter())
    };

    let column_name_value_list = columns.zip(values.iter()).collect::<Vec<(_, _)>>();

    let values = column_defs
        .iter()
        .map(|column_def| {
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

            match (value, &column_def.default, nullable) {
                (Some(&expr), _, _) | (None, Some(expr), _) => {
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

fn validate_row(row: &Row, column_defs: &[ColumnDef]) -> Result<()> {
    let items = column_defs
        .iter()
        .enumerate()
        .filter_map(|(index, column_def)| {
            let value = row.get_value_by_index(index);

            value.map(|v| (v, column_def))
        });

    for (value, column_def) in items {
        let ColumnDef {
            data_type,
            nullable,
            ..
        } = column_def;

        value.validate_type(data_type)?;
        value.validate_null(*nullable)?;
    }

    Ok(())
}
