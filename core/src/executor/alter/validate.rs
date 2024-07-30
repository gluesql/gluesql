use {
    super::AlterError,
    crate::{
        ast::{ColumnDef, DataType, OperateFunctionArg},
        executor::evaluate_stateless,
        result::Result,
    },
};

/// Validates whether the column definition is self-consistent.
///
/// # Arguments
/// * `column_def` - The column definition to validate.
/// * `unique` - Whether the column is unique.
pub async fn validate(column_def: &ColumnDef, unique: bool) -> Result<()> {
    let ColumnDef {
        data_type,
        default,
        name,
        ..
    } = column_def;

    // unique + data type
    if matches!(data_type, DataType::Float | DataType::Map) && unique {
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

pub fn validate_column_names<'a, C: Clone + Iterator<Item = &'a str>>(
    column_names: C,
) -> Result<()> {
    let duplicate_column_name = column_names
        .clone()
        .enumerate()
        .find(|(i, base_column)| {
            column_names
                .clone()
                .skip(i + 1)
                .any(|target_column| base_column == &target_column)
        })
        .map(|(_, column)| column);

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

pub async fn validate_default_args(args: &[OperateFunctionArg]) -> Result<()> {
    for expr in args.iter().filter_map(|arg| arg.default.as_ref()) {
        evaluate_stateless(None, expr).await?;
    }

    if args
        .iter()
        .map(|arg| arg.default.as_ref())
        .skip_while(Option::is_none)
        .all(|default| default.is_some())
    {
        Ok(())
    } else {
        Err(AlterError::NonDefaultArgumentFollowsDefaultArgument.into())
    }
}
