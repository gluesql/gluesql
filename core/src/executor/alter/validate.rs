use {
    super::AlterError,
    crate::{
        ast::{ColumnUniqueOption, OperateFunctionArg},
        executor::evaluate_stateless,
        plan::{ColumnDefPlan, plan_scalar_expr},
        prelude::DataType,
        result::Result,
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

pub fn validate_arg_names(args: &[OperateFunctionArg]) -> Result<()> {
    let duplicate_arg_name = args
        .iter()
        .enumerate()
        .find(|(i, base_arg)| {
            args.iter()
                .skip(i + 1)
                .any(|target_arg| base_arg.name == target_arg.name)
        })
        .map(|(_, arg)| arg.name.clone());

    match duplicate_arg_name {
        Some(v) => Err(AlterError::DuplicateArgName(v).into()),
        None => Ok(()),
    }
}

pub fn validate_default_args(args: &[OperateFunctionArg]) -> Result<()> {
    for expr in args.iter().filter_map(|arg| arg.default.as_ref()) {
        let expr = plan_scalar_expr(expr.clone());

        evaluate_stateless(None, &expr)?;
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
