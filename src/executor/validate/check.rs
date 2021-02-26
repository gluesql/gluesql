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
            create_constraints,
            Constraint,
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

    println!("unique columns: {:?}", columns);

    let constraints: Vec<Constraint> = create_constraints(columns, row_iter, Constraint::UniqueConstraint { column_index: 0, column_name: "".to_string(), keys: HashSet::new() })?.into();
    if constraints.is_empty() {
        return Ok(());
    }

    println!("unique constraints: {:?}", constraints);

    throw_or_pass(storage, table_name, constraints).await
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

    println!("type {:?} columns: {:?}", data_type, columns);

    let constraints: Vec<Constraint> = create_constraints(columns, row_iter, Constraint::TypeConstraint { column_index: 0, column_name: "".to_string() })?.into();
    if constraints.is_empty() {
        return Ok(());
    }

    println!("type {:?} constraints: {:?}", data_type, constraints);

    throw_or_pass(storage, table_name, constraints).await
}

async fn throw_or_pass<T: 'static + Debug>(storage: &impl Store<T>,
    table_name: &str,
    constraints: Vec<Constraint>
    ) -> Result<()> {

    let constraints = Rc::new(constraints);
    storage.scan_data(table_name).await?.try_for_each(|result| {
        let (_, row) = result?;
        Rc::clone(&constraints)
            .iter()
            .try_for_each(|constraint| {
                let col_idx = match constraint {Constraint::UniqueConstraint {column_index, ..} | Constraint::TypeConstraint {column_index, ..} => column_index}.to_owned();
                let val = row
                    .get_value(col_idx)
                    .ok_or(ValidateError::ConflictOnStorageColumnIndex(col_idx))?;
                constraint.check(val)?;
                Ok(())
            })
    })
}