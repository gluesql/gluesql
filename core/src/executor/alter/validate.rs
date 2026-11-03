use {
    super::AlterError,
    crate::{
        ast::ColumnUniqueOption, executor::evaluate_stateless, plan::ColumnDefPlan,
        prelude::DataType, result::Result,
    },
};

pub fn validate(column_def: &ColumnDefPlan) -> Result<()> {
    validate_column_def(column_def)?;

    if let Some(expr) = &column_def.default {
        evaluate_stateless(None, expr.planned())?;
    }

    Ok(())
}

pub fn validate_column_def(column_def: &ColumnDefPlan) -> Result<()> {
    let ColumnDefPlan {
        data_type,
        unique,
        name,
        ..
    } = column_def;

    if matches!(
        data_type,
        DataType::Float | DataType::Float32 | DataType::Map
    ) && matches!(unique, Some(ColumnUniqueOption { .. }))
    {
        return Err(AlterError::UnsupportedDataTypeForUniqueColumn(
            name.to_owned(),
            data_type.clone(),
        )
        .into());
    }

    Ok(())
}

pub fn validate_column_names(column_defs: &[ColumnDefPlan]) -> Result<()> {
    let duplicate_column_name = column_defs
        .iter()
        .enumerate()
        .find(|(i, base_column)| {
            column_defs
                .iter()
                .skip(i + 1)
                .any(|target_column| base_column.name == target_column.name)
        })
        .map(|(_, column)| &column.name);

    match duplicate_column_name {
        Some(v) => Err(AlterError::DuplicateColumnName(v.to_owned()).into()),
        None => Ok(()),
    }
}
