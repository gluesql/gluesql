use crate::ast::ToSql;
use {
    super::{DataType, Expr, ObjectName},
    serde::{Deserialize, Serialize},
};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum AlterTableOperation {
    /// `ADD [ COLUMN ] <column_def>`
    AddColumn { column_def: ColumnDef },
    /// `DROP [ COLUMN ] [ IF EXISTS ] <column_name> [ CASCADE ]`
    DropColumn {
        column_name: String,
        if_exists: bool,
    },
    /// `RENAME [ COLUMN ] <old_column_name> TO <new_column_name>`
    RenameColumn {
        old_column_name: String,
        new_column_name: String,
    },
    /// `RENAME TO <table_name>`
    RenameTable { table_name: ObjectName },
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ColumnDef {
    pub name: String,
    pub data_type: DataType,
    pub options: Vec<ColumnOptionDef>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ColumnOptionDef {
    pub name: Option<String>,
    pub option: ColumnOption,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ColumnOption {
    /// `NULL`
    Null,
    /// `NOT NULL`
    NotNull,
    /// `DEFAULT <restricted-expr>`
    Default(Expr),
    /// `{ PRIMARY KEY | UNIQUE }`
    Unique { is_primary: bool },
}

impl ToSql for AlterTableOperation {
    fn to_sql(&self) -> String {
        match self {
            AlterTableOperation::AddColumn { column_def } => format!("ADD {}", column_def.to_sql()),
            AlterTableOperation::DropColumn {
                column_name,
                if_exists,
            } => match if_exists {
                true => format!("DROP IF EXISTS {column_name}"),
                false => format!("DROP {column_name}"),
            },
            AlterTableOperation::RenameColumn {
                old_column_name,
                new_column_name,
            } => format!("RENAME {old_column_name} TO {new_column_name}"),
            AlterTableOperation::RenameTable { table_name } => format!("RENAME TO {table_name}"),
        }
    }
}

impl ToSql for ColumnDef {
    fn to_sql(&self) -> String {
        match self {
            ColumnDef {
                name,
                data_type,
                options,
            } => {
                let options = options
                    .iter()
                    .map(|op| op.option.to_sql()) // TODO name..
                    .collect::<Vec<_>>()
                    .join(" ");
                format!("{name} {data_type} {options}")
            }
        }
    }
}

impl ToSql for ColumnOption {
    fn to_sql(&self) -> String {
        match self {
            ColumnOption::Null => "NULL".to_string(),
            ColumnOption::NotNull => "NOT NULL".to_string(),
            ColumnOption::Default(expr) => format!("DEFAULT {}", expr.to_sql()),
            ColumnOption::Unique { is_primary } => {
                // TODO Q. simple bool match vs if else ????
                if is_primary {
                    "PRIMARY KEY".to_string()
                } else {
                    "UNIQUE".to_string()
                }
            }
        }
    }
}

// TODO move & add test code
