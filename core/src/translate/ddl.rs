use {
    super::{
        ParamLiteral, TranslateError, data_type::translate_data_type,
        expr::translate_expr_with_params, translate_object_name,
    },
    crate::{
        ast::{AlterTableOperation, ColumnDef, ColumnUniqueOption, OperateFunctionArg},
        result::Result,
    },
    sqlparser::ast::{
        AlterTableOperation as SqlAlterTableOperation, ColumnDef as SqlColumnDef,
        ColumnOption as SqlColumnOption, ColumnOptionDef as SqlColumnOptionDef,
        OperateFunctionArg as SqlOperateFunctionArg,
    },
};

pub(crate) fn translate_alter_table_operation_with_params(
    sql_alter_table_operation: &SqlAlterTableOperation,
    params: &[ParamLiteral],
) -> Result<AlterTableOperation> {
    match sql_alter_table_operation {
        SqlAlterTableOperation::AddColumn { column_def, .. } => {
            Ok(AlterTableOperation::AddColumn {
                column_def: translate_column_def_with_params(column_def, params)?,
            })
        }
        SqlAlterTableOperation::DropColumn {
            column_name,
            if_exists,
            ..
        } => Ok(AlterTableOperation::DropColumn {
            column_name: column_name.value.to_owned(),
            if_exists: *if_exists,
        }),
        SqlAlterTableOperation::RenameColumn {
            old_column_name,
            new_column_name,
        } => Ok(AlterTableOperation::RenameColumn {
            old_column_name: old_column_name.value.to_owned(),
            new_column_name: new_column_name.value.to_owned(),
        }),
        SqlAlterTableOperation::RenameTable { table_name } => {
            Ok(AlterTableOperation::RenameTable {
                table_name: translate_object_name(table_name)?,
            })
        }
        _ => Err(TranslateError::UnsupportedAlterTableOperation(
            sql_alter_table_operation.to_string(),
        )
        .into()),
    }
}

pub(crate) fn translate_column_def_with_params(
    sql_column_def: &SqlColumnDef,
    params: &[ParamLiteral],
) -> Result<ColumnDef> {
    let SqlColumnDef {
        name,
        data_type,
        options,
        ..
    } = sql_column_def;

    let (nullable, default, unique, comment) = options.iter().try_fold(
        (true, None, None, None),
        |(nullable, default, unique, comment), SqlColumnOptionDef { option, .. }| -> Result<_> {
            match option {
                SqlColumnOption::Null => Ok((nullable, default, unique, comment)),
                SqlColumnOption::NotNull => Ok((false, default, unique, comment)),
                SqlColumnOption::Default(default) => {
                    let default = translate_expr_with_params(default, params).map(Some)?;

                    Ok((nullable, default, unique, comment))
                }
                SqlColumnOption::Unique { is_primary, .. } => {
                    let nullable = if *is_primary { false } else { nullable };
                    let unique = Some(ColumnUniqueOption {
                        is_primary: *is_primary,
                    });

                    Ok((nullable, default, unique, comment))
                }
                SqlColumnOption::Comment(comment) => {
                    Ok((nullable, default, unique, Some(comment.to_string())))
                }
                _ => Err(TranslateError::UnsupportedColumnOption(option.to_string()).into()),
            }
        },
    )?;

    Ok(ColumnDef {
        name: name.value.to_owned(),
        data_type: translate_data_type(data_type)?,
        nullable,
        default,
        unique,
        comment,
    })
}

/// Translates a [`SqlColumnDef`] into GlueSQL's [`ColumnDef`] without parameters.
///
/// This is a convenience wrapper around [`translate_column_def_with_params`] that
/// delegates to the parameter-aware implementation with an empty parameter list.
///
/// # Errors
///
/// Returns an error when the column definition uses data types, default expressions,
/// or column options (for example `COLLATE`, unsupported constraints) that GlueSQL
/// does not support.
pub fn translate_column_def(sql_column_def: &SqlColumnDef) -> Result<ColumnDef> {
    const NO_PARAMS: [ParamLiteral; 0] = [];
    translate_column_def_with_params(sql_column_def, &NO_PARAMS)
}

/// Translates a [`SqlOperateFunctionArg`] into GlueSQL's [`OperateFunctionArg`] using the supplied parameters.
///
/// # Errors
///
/// Returns an error when converting the argument's data type fails or when its default
/// expression uses syntax GlueSQL does not support.
pub(crate) fn translate_operate_function_arg_with_params(
    arg: &SqlOperateFunctionArg,
    params: &[ParamLiteral],
) -> Result<OperateFunctionArg> {
    let name = arg
        .name
        .as_ref()
        .map(|v| v.value.to_owned())
        .ok_or(TranslateError::UnNamedFunctionArgNotSupported)?;
    let data_type = translate_data_type(&arg.data_type)?;
    let default = arg
        .default_expr
        .as_ref()
        .map(|expr| translate_expr_with_params(expr, params))
        .transpose()?;
    Ok(OperateFunctionArg {
        name,
        data_type,
        default,
    })
}
