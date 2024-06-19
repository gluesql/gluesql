use {
    super::{
        select::select,
        validate::{validate_unique, ColumnValidation},
    },
    crate::{
        ast::{ColumnDef, ColumnUniqueOption, Expr, ForeignKey, Query, SetExpr, Values},
        data::{Key, Row, Schema, Value},
        executor::{evaluate::evaluate_stateless, limit::Limit},
        result::Result,
        store::{DataRow, GStore, GStoreMut},
    },
    futures::stream::{self, StreamExt, TryStreamExt},
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

    #[error("only single value accepted for schemaless row insert")]
    OnlySingleValueAcceptedForSchemalessRow,

    #[error("map type required: {0}")]
    MapTypeValueRequired(String),

    #[error("cannot find referenced value on {table_name}.{column_name} with value {referenced_value:?}")]
    CannotFindReferencedValue {
        table_name: String,
        column_name: String,
        referenced_value: String,
    },

    #[error("unreachable referencing column name: {0}")]
    ConflictReferencingColumnName(String),
}

enum RowsData {
    Append(Vec<DataRow>),
    Insert(Vec<(Key, DataRow)>),
}

pub async fn insert<T: GStore + GStoreMut>(
    storage: &mut T,
    table_name: &str,
    columns: &[String],
    source: &Query,
) -> Result<usize> {
    let Schema {
        column_defs,
        foreign_keys,
        ..
    } = storage
        .fetch_schema(table_name)
        .await?
        .ok_or_else(|| InsertError::TableNotFound(table_name.to_owned()))?;

    let rows = match column_defs {
        Some(column_defs) => {
            fetch_vec_rows(
                storage,
                table_name,
                column_defs,
                columns,
                source,
                foreign_keys,
            )
            .await
        }
        None => fetch_map_rows(storage, source).await.map(RowsData::Append),
    }?;

    match rows {
        RowsData::Append(rows) => {
            let num_rows = rows.len();

            storage
                .append_data(table_name, rows)
                .await
                .map(|_| num_rows)
        }
        RowsData::Insert(rows) => {
            let num_rows = rows.len();

            storage
                .insert_data(table_name, rows)
                .await
                .map(|_| num_rows)
        }
    }
}

async fn fetch_vec_rows<T: GStore>(
    storage: &T,
    table_name: &str,
    column_defs: Vec<ColumnDef>,
    columns: &[String],
    source: &Query,
    foreign_keys: Vec<ForeignKey>,
) -> Result<RowsData> {
    let labels = Rc::from(
        column_defs
            .iter()
            .map(|column_def| column_def.name.to_owned())
            .collect::<Vec<_>>(),
    );
    let column_defs = Rc::from(column_defs);
    let column_validation = ColumnValidation::All(&column_defs);

    #[derive(futures_enum::Stream)]
    enum Rows<I1, I2> {
        Values(I1),
        Select(I2),
    }

    let rows = match &source.body {
        SetExpr::Values(Values(values_list)) => {
            let limit = Limit::new(source.limit.as_ref(), source.offset.as_ref()).await?;
            let rows = stream::iter(values_list).then(|values| {
                let column_defs = Rc::clone(&column_defs);
                let labels = Rc::clone(&labels);

                async move {
                    Ok(Row::Vec {
                        columns: labels,
                        values: fill_values(&column_defs, columns, values).await?,
                    })
                }
            });
            let rows = limit.apply(rows);
            let rows = rows.map(|row| row?.try_into_vec());

            Rows::Values(rows)
        }
        SetExpr::Select(_) => {
            let rows = select(storage, source, None).await?.map(|row| {
                let values = row?.try_into_vec()?;

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

            Rows::Select(rows)
        }
    }
    .try_collect::<Vec<Vec<Value>>>()
    .await?;

    validate_unique(
        storage,
        table_name,
        column_validation,
        rows.iter().map(|values| values.as_slice()),
    )
    .await?;

    validate_foreign_key(storage, &column_defs, foreign_keys, &rows).await?;

    let primary_key = column_defs.iter().position(|ColumnDef { unique, .. }| {
        unique == &Some(ColumnUniqueOption { is_primary: true })
    });

    match primary_key {
        Some(i) => rows
            .into_iter()
            .filter_map(|values| {
                values
                    .get(i)
                    .map(Key::try_from)
                    .map(|result| result.map(|key| (key, values.into())))
            })
            .collect::<Result<Vec<_>>>()
            .map(RowsData::Insert),
        None => Ok(RowsData::Append(rows.into_iter().map(Into::into).collect())),
    }
}

async fn validate_foreign_key<T: GStore>(
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

        for row in rows.iter() {
            let value =
                row.get(target_index.0)
                    .ok_or(InsertError::ConflictReferencingColumnName(
                        referencing_column_name.to_owned(),
                    ))?;

            if value == &Value::Null {
                continue;
            }

            let no_referenced = storage
                .fetch_data(referenced_table_name, &Key::try_from(value)?)
                .await?
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

async fn fetch_map_rows<T: GStore>(storage: &T, source: &Query) -> Result<Vec<DataRow>> {
    #[derive(futures_enum::Stream)]
    enum Rows<I1, I2> {
        Values(I1),
        Select(I2),
    }

    let rows = match &source.body {
        SetExpr::Values(Values(values_list)) => {
            let limit = Limit::new(source.limit.as_ref(), source.offset.as_ref()).await?;
            let rows = stream::iter(values_list).then(|values| async move {
                if values.len() > 1 {
                    return Err(InsertError::OnlySingleValueAcceptedForSchemalessRow.into());
                }

                evaluate_stateless(None, &values[0])
                    .await?
                    .try_into()
                    .map(Row::Map)
            });
            let rows = limit.apply(rows);
            let rows = rows.map_ok(Into::into);

            Rows::Values(rows)
        }
        SetExpr::Select(_) => {
            let rows = select(storage, source, None).await?.map(|row| {
                let row = row?;

                if let Row::Vec { values, .. } = &row {
                    if values.len() > 1 {
                        return Err(InsertError::OnlySingleValueAcceptedForSchemalessRow.into());
                    } else if !matches!(&values[0], Value::Map(_)) {
                        return Err(InsertError::MapTypeValueRequired((&values[0]).into()).into());
                    }
                }

                Ok(row.into())
            });

            Rows::Select(rows)
        }
    }
    .try_collect::<Vec<DataRow>>()
    .await?;

    Ok(rows)
}

async fn fill_values(
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

    let values = stream::iter(column_defs)
        .then(|column_def| {
            let column_name_value_list = &column_name_value_list;

            async move {
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
                    (Some(&expr), _, _) | (None, Some(expr), _) => evaluate_stateless(None, expr)
                        .await?
                        .try_into_value(data_type, *nullable),
                    (None, None, true) => Ok(Value::Null),
                    (None, None, false) => {
                        Err(InsertError::LackOfRequiredColumn(def_name.to_owned()).into())
                    }
                }
            }
        })
        .try_collect::<Vec<Value>>()
        .await?;

    Ok(values)
}
