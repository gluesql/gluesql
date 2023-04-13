use {
    super::AlterError,
    crate::{
        ast::{ColumnDef, ColumnUniqueOption, DataType, OperateFunctionArg},
        executor::evaluate_stateless,
        result::Result,
    },
};

pub async fn validate(column_def: &ColumnDef) -> Result<()> {
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
        evaluate_stateless(None, expr).await?;
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

pub fn validate_arg_names(args: &[OperateFunctionArg]) -> Result<()> {
    let duplicate_arg_name = args
        .iter()
        .enumerate()
        .find(|(i, base_arg)| {
            args.iter()
                .skip(i + 1)
                .any(|target_arg| base_arg.name == target_arg.name)
        })
        .map(|(_, arg)| arg.name.to_owned());

    match duplicate_arg_name {
        Some(v) => Err(AlterError::DuplicateArgName(v).into()),
        None => Ok(()),
    }
}

pub fn validate_default_args(args: &[OperateFunctionArg]) -> Result<()> {
    args.iter().enumerate().try_for_each(|(i, arg)| {
        arg.default
            .as_ref()
            .map(|expr| evaluate_stateless(None, expr))
            .transpose()?;
        match (
            arg.default.is_some(),
            args.get(i + 1).map(|v| v.default.is_some()).unwrap_or(true),
        ) {
            (true, false) => Err(AlterError::NonDefaultArgumentFollowsDefaultArgument.into()),
            _ => Ok(()),
        }
    })
}
