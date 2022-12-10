use {
    super::{data_type::translate_data_type, expr::translate_expr, TranslateError},
    crate::{
        ast::{ColumnDef, ColumnOption},
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

    let nullable = !options.iter().any(|SqlColumnOptionDef { option, .. }| {
        option == &SqlColumnOption::NotNull
            || option == &SqlColumnOption::Unique { is_primary: true }
    });

    let default = options
        .iter()
        .find_map(|option| match option {
            SqlColumnOptionDef {
                option: SqlColumnOption::Default(default),
                ..
            } => Some(translate_expr(default)),
            _ => None,
        })
        .transpose()?;

    Ok(ColumnDef {
        name: name.value.to_owned(),
        data_type: translate_data_type(data_type)?,
        nullable,
        default,
        options: options
            .iter()
            .filter_map(|column_option_def| {
                translate_column_option_def(column_option_def).transpose()
            })
            .collect::<Result<Vec<ColumnOption>>>()?,
    })
}

/// Translate [`SqlColumnOptionDef`] to [`ColumnOption`].
///
/// `sql-parser` parses column option as `{ name, option }` type,
/// but in here we only need `option`.
fn translate_column_option_def(
    sql_column_option_def: &SqlColumnOptionDef,
) -> Result<Option<ColumnOption>> {
    let SqlColumnOptionDef { option, .. } = sql_column_option_def;

    match option {
        SqlColumnOption::Null | SqlColumnOption::NotNull | SqlColumnOption::Default(_) => Ok(None),
        SqlColumnOption::Unique { is_primary } => Ok(Some(ColumnOption::Unique {
            is_primary: *is_primary,
        })),
        _ => Err(TranslateError::UnsupportedColumnOption(option.to_string()).into()),
    }
}
