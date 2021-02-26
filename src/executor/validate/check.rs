use {
    im_rc::HashSet,
    sqlparser::ast::DataType,
    std::{
        fmt::Debug,
        rc::Rc,
    },
    crate::{
        data::Row,
        result::Result,
        store::Store,
    },
    super::{
        ColumnValidation,
        ValidateError,
        fetch::{
            fetch_all_unique_columns,
            fetch_all_columns_of_type,
            specified_columns_only,
        },
        constraint::{
            create_unique_constraints,
            UniqueConstraint,
        },
    },
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

pub async fn validate_type<T: 'static + Debug>(
    storage: &impl Store<T>,
    table_name: &str,
    column_validation: ColumnValidation,
    row_iter: impl Iterator<Item = &Row> + Clone,
) -> Result<()> {
    validate_specific_type(storage, table_name, column_validation.clone(), row_iter.clone(), DataType::Boolean).await?;
    validate_specific_type(storage, table_name, column_validation.clone(), row_iter.clone(), DataType::Int).await?;
    validate_specific_type(storage, table_name, column_validation.clone(), row_iter.clone(), DataType::Float(None)).await?;
    validate_specific_type(storage, table_name, column_validation.clone(), row_iter.clone(), DataType::Text).await?;
    Ok(())
}

async fn validate_specific_type<T: 'static + Debug>(
    storage: &impl Store<T>,
    table_name: &str,
    column_validation: ColumnValidation,
    row_iter: impl Iterator<Item = &Row> + Clone,
    data_type: DataType
) -> Result<()> {
    let columns = match column_validation {
        ColumnValidation::All(column_defs) => fetch_all_columns_of_type(&column_defs, data_type.clone()),
        ColumnValidation::SpecifiedColumns(all_column_defs, specified_columns) => {
            specified_columns_only(fetch_all_columns_of_type(&all_column_defs, data_type.clone()), &specified_columns)
        }
    };

    if columns.is_empty() {
        return Ok(());
    }

    println!("type {:?} columns: {:?}", data_type, columns);

    for column in columns.into_iter() {
        for row in row_iter.clone() {
            match row.get_value(column.0) {
                Some(column_type) => if !matches!(column_type, data_type) {
                    
                    return Err(ValidateError::IncompatibleTypeOnTypedField(data_type.to_string(), column.1).into());
                } else {println!("Col type: {:?} Dat type: {:?}", column_type, data_type);},
                None => (),
            }
       };
    };
    Ok(())
}