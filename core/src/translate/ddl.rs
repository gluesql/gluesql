use {
    super::{
        data_type::translate_data_type, expr::translate_expr, translate_object_name, TranslateError,
    },
    crate::{
        ast::{
            AlterTableOperation, CheckConstraint, ColumnDef, ColumnUniqueOption, OperateFunctionArg,
        },
        result::Result,
    },
    sqlparser::ast::{
        AlterTableOperation as SqlAlterTableOperation, ColumnDef as SqlColumnDef,
        ColumnOption as SqlColumnOption, ColumnOptionDef as SqlColumnOptionDef,
        OperateFunctionArg as SqlOperateFunctionArg,
    },
};

pub fn translate_alter_table_operation(
    sql_alter_table_operation: &SqlAlterTableOperation,
) -> Result<AlterTableOperation> {
    match sql_alter_table_operation {
        SqlAlterTableOperation::AddColumn { column_def, .. } => {
            let (column_def, check) = translate_column_def(column_def)?;
            Ok(AlterTableOperation::AddColumn { column_def, check })
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

pub fn translate_column_def(
    sql_column_def: &SqlColumnDef,
) -> Result<(ColumnDef, Option<CheckConstraint>)> {
    let SqlColumnDef {
        name,
        data_type,
        options,
        ..
    } = sql_column_def;

    let (nullable, default, unique, check, comment) = options.iter().try_fold(
        (true, None, None, None, None),
        |(nullable, default, unique, check, comment),
         SqlColumnOptionDef { option, .. }|
         -> Result<_> {
            match option {
                SqlColumnOption::Null => Ok((nullable, default, unique, check, comment)),
                SqlColumnOption::NotNull => Ok((false, default, unique, check, comment)),
                SqlColumnOption::Default(default) => {
                    let default = translate_expr(default).map(Some)?;

                    Ok((nullable, default, unique, check, comment))
                }
                SqlColumnOption::Unique { is_primary, .. } => {
                    let nullable = if *is_primary { false } else { nullable };
                    let unique = Some(ColumnUniqueOption {
                        is_primary: *is_primary,
                    });

                    Ok((nullable, default, unique, check, comment))
                }
                SqlColumnOption::Comment(comment) => {
                    Ok((nullable, default, unique, check, Some(comment.to_string())))
                }
                SqlColumnOption::Check(expr) => {
                    let check = Some(CheckConstraint::anonymous(translate_expr(expr)?));
                    Ok((nullable, default, unique, check, comment))
                }
                _ => Err(TranslateError::UnsupportedColumnOption(option.to_string()).into()),
            }
        },
    )?;

    Ok((
        ColumnDef {
            name: name.value.to_owned(),
            data_type: translate_data_type(data_type)?,
            nullable,
            default,
            unique,
            comment,
        },
        check,
    ))
}

pub fn translate_operate_function_arg(arg: &SqlOperateFunctionArg) -> Result<OperateFunctionArg> {
    let name = arg
        .name
        .as_ref()
        .map(|v| v.value.to_owned())
        .ok_or(TranslateError::UnNamedFunctionArgNotSupported)?;
    let data_type = translate_data_type(&arg.data_type)?;
    let default = arg.default_expr.as_ref().map(translate_expr).transpose()?;
    Ok(OperateFunctionArg {
        name,
        data_type,
        default,
    })
}

#[cfg(test)]
mod tests {
    use crate::ast::{AstLiteral, Expr};

    use super::*;
    use sqlparser::ast::{
        ColumnDef as SqlColumnDef, ColumnOption, ColumnOptionDef, DataType as SqlDataType,
        Expr as SqlExpr, Ident as SqlIdent, Value as SqlValue,
    };

    #[test]
    /// Test to cover all of the possible cases of the `translate_column_def` function.
    fn test_translate_column_def() {
        // Case where Column Def is Materialized
        let sql_column_def = SqlColumnDef {
            name: SqlIdent {
                value: "column_name".to_owned(),
                quote_style: None,
            },
            data_type: SqlDataType::Int16,
            collation: None,
            options: vec![ColumnOptionDef {
                name: Some(SqlIdent {
                    value: "MATERIALIZED".to_owned(),
                    quote_style: None,
                }),
                option: ColumnOption::Materialized(SqlExpr::Value(SqlValue::Boolean(true))),
            }],
        };

        assert_eq!(
            translate_column_def(&sql_column_def),
            Err(TranslateError::UnsupportedColumnOption("MATERIALIZED true".to_owned()).into())
        );

        // Case where Column Def includes a Check Constraint
        let sql_column_def = SqlColumnDef {
            name: SqlIdent {
                value: "column_name".to_owned(),
                quote_style: None,
            },
            data_type: SqlDataType::Int16,
            collation: None,
            options: vec![ColumnOptionDef {
                name: Some(SqlIdent {
                    value: "CHECK".to_owned(),
                    quote_style: None,
                }),
                option: ColumnOption::Check(SqlExpr::Value(SqlValue::Boolean(true))),
            }],
        };

        assert_eq!(
            translate_column_def(&sql_column_def),
            Ok((
                ColumnDef {
                    name: "column_name".to_owned(),
                    data_type: crate::ast::DataType::Int16,
                    nullable: true,
                    default: None,
                    unique: None,
                    comment: None,
                },
                Some(CheckConstraint::anonymous(Expr::Literal(
                    AstLiteral::Boolean(true)
                )))
            ))
        );
    }
}
