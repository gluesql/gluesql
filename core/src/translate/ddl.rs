use {
    super::{
        data_type::translate_data_type, expr::translate_expr, translate_object_name, TranslateError,
    },
    crate::{
        ast::{AlterTableOperation, ColumnDef, ColumnUniqueOption},
        result::Result,
    },
    sqlparser::ast::{
        AlterTableOperation as SqlAlterTableOperation, ColumnDef as SqlColumnDef,
        ColumnOption as SqlColumnOption, ColumnOptionDef as SqlColumnOptionDef,
    },
};

pub fn translate_alter_table_operation(
    sql_alter_table_operation: &SqlAlterTableOperation,
) -> Result<AlterTableOperation> {
    match sql_alter_table_operation {
        SqlAlterTableOperation::AddColumn { column_def, .. } => {
            Ok(AlterTableOperation::AddColumn {
                column_def: translate_column_def(column_def)?,
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

pub fn translate_column_def(sql_column_def: &SqlColumnDef) -> Result<ColumnDef> {
    let SqlColumnDef {
        name,
        data_type,
        options,
        ..
    } = sql_column_def;

    let (nullable, default, unique) = options.iter().try_fold(
        (true, None, None),
        |(nullable, default, unique), SqlColumnOptionDef { option, .. }| -> Result<_> {
            match option {
                SqlColumnOption::Null => Ok((nullable, default, unique)),
                SqlColumnOption::NotNull => Ok((false, default, unique)),
                SqlColumnOption::Default(default) => {
                    let default = translate_expr(default).map(Some)?;

                    Ok((nullable, default, unique))
                }
                SqlColumnOption::Unique { is_primary } => {
                    let nullable = if *is_primary { false } else { nullable };
                    let unique = Some(ColumnUniqueOption {
                        is_primary: *is_primary,
                    });

                    Ok((nullable, default, unique))
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
    })
}
