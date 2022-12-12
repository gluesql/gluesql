use {
    super::AlterError,
    crate::{
        ast::{ColumnDef, ColumnUniqueOption, DataType},
        executor::evaluate_stateless,
        result::Result,
    },
};

pub fn validate(column_def: &ColumnDef) -> Result<()> {
    let ColumnDef {
        data_type,
        default,
        unique,
        name,
        ..
    } = column_def;

    // unique + data type
    if matches!(data_type, DataType::Float | DataType::Map)
        && matches!(unique, Some(ColumnUniqueOption { .. }))
    {
        return Err(AlterError::UnsupportedDataTypeForUniqueColumn(
            name.to_owned(),
            data_type.clone(),
        )
        .into());
    }

    if let Some(expr) = default {
        evaluate_stateless(None, expr)?;
    }

    Ok(())
}

pub fn validate_column_names(column_defs: &[ColumnDef]) -> Result<()> {
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
