use {
    super::{
        constraint::create_unique_constraints,
        fetch::{fetch_all_unique_columns, specified_columns_only},
        ColumnValidation, ValidateError,
    },
    crate::{
        data::{Row, Value},
        result::Result,
        store::Store,
    },
    sqlparser::ast::DataType,
    std::{fmt::Debug, rc::Rc},
};

pub async fn validate_unique<T: 'static + Debug>(
    storage: &impl Store<T>,
    table_name: &str,
    column_validation: ColumnValidation,
    row_iter: impl Iterator<Item = &Row> + Clone,
) -> Result<()> {
    let columns = match column_validation {
        ColumnValidation::All(column_defs) => fetch_all_unique_columns(&column_defs),
        ColumnValidation::SpecifiedColumns(column_defs, specified_columns) => {
            specified_columns_only(fetch_all_unique_columns(&column_defs), &specified_columns)
        }
    };

    let unique_constraints: Vec<_> = create_unique_constraints(columns, row_iter)?.into();
    if unique_constraints.is_empty() {
        return Ok(());
    }

    let unique_constraints = Rc::new(unique_constraints);
    storage.scan_data(table_name).await?.try_for_each(|result| {
        let (_, row) = result?;
        Rc::clone(&unique_constraints)
            .iter()
            .try_for_each(|constraint| {
                let col_idx = constraint.column_index;
                let val = row
                    .get_value(col_idx)
                    .ok_or(ValidateError::ConflictOnStorageColumnIndex(col_idx))?;
                constraint.check(val)?;
                Ok(())
            })
    })
}

pub async fn validate_types<T: 'static + Debug>(
    _storage: &impl Store<T>,
    _table_name: &str,
    column_validation: ColumnValidation,
    row_iter: impl Iterator<Item = &Row> + Clone,
) -> Result<()> {
    let columns = match column_validation {
        ColumnValidation::All(columns) => columns,
        ColumnValidation::SpecifiedColumns(columns, ..) => columns,
    };
    for row in row_iter.clone() {
        for (index, column) in columns.into_iter().enumerate() {
            let result: Result<()> = match row.get_value(index) {
                Some(row_data) => match (column.data_type.clone(), row_data) {
                    (DataType::Boolean, Value::Bool(_))
                    | (DataType::Boolean, Value::OptBool(Some(_)))
                    | (DataType::Text, Value::Str(_))
                    | (DataType::Text, Value::OptStr(Some(_)))
                    | (DataType::Int, Value::I64(_))
                    | (DataType::Int, Value::OptI64(Some(_)))
                    | (DataType::Float(_), Value::F64(_))
                    | (DataType::Float(_), Value::OptF64(Some(_)))
                    | (_, Value::OptBool(None))
                    | (_, Value::OptStr(None))
                    | (_, Value::OptI64(None))
                    | (_, Value::OptF64(None)) => Ok(()),
                    (_, row_data) => Err(ValidateError::IncompatibleTypeOnTypedField(
                        format!("{:?}", row_data),
                        column.name.value.to_string(),
                        column.data_type.to_string(),
                    )
                    .into()),
                },
                None => Ok(()),
            };
            result?
        }
    }
    Ok(())
}
