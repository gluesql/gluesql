use {
    super::{data_type::translate_data_type, expr::translate_expr, TranslateError},
    crate::{
        ast::{ColumnDef, ColumnOption, ColumnOptionDef},
        result::Result,
    },
    sqlparser::ast::{
        ColumnDef as SqlColumnDef, ColumnOption as SqlColumnOption,
        ColumnOptionDef as SqlColumnOptionDef,
    },
};

#[cfg(feature = "alter-table")]
use {
    super::translate_object_name, crate::ast::AlterTableOperation,
    sqlparser::ast::AlterTableOperation as SqlAlterTableOperation,
};

#[cfg(feature = "alter-table")]
pub fn translate_alter_table_operation(
    sql_alter_table_operation: &SqlAlterTableOperation,
) -> Result<AlterTableOperation> {
    match sql_alter_table_operation {
        SqlAlterTableOperation::AddColumn { column_def } => Ok(AlterTableOperation::AddColumn {
            column_def: translate_column_def(column_def)?,
        }),
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
                table_name: translate_object_name(table_name),
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

    Ok(ColumnDef {
        name: name.value.to_owned(),
        data_type: translate_data_type(data_type)?,
        options: options
            .iter()
            .map(translate_column_option_def)
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .flatten()
            .collect(),
    })
}

fn translate_column_option_def(
    sql_column_option_def: &SqlColumnOptionDef,
) -> Result<Vec<ColumnOptionDef>> {
    let SqlColumnOptionDef { name, option } = sql_column_option_def;

    let name = name.as_ref().map(|name| name.value.to_owned());
    let option = match option {
        SqlColumnOption::Null => Ok(ColumnOption::Null),
        SqlColumnOption::NotNull => Ok(ColumnOption::NotNull),
        SqlColumnOption::Default(expr) => translate_expr(expr).map(ColumnOption::Default),
        SqlColumnOption::Unique { is_primary } if !is_primary => {
            Ok(ColumnOption::Unique { is_primary: false })
        }
        SqlColumnOption::Unique { .. } => {
            return Ok(vec![
                ColumnOptionDef {
                    name: name.clone(),
                    option: ColumnOption::Unique { is_primary: true },
                },
                ColumnOptionDef {
                    name,
                    option: ColumnOption::NotNull,
                },
            ]);
        }
        _ => Err(TranslateError::UnsupportedColumnOption(option.to_string()).into()),
    }?;

    Ok(vec![ColumnOptionDef { name, option }])
}
