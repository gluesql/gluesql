use {
    super::{DataType, Expr},
    crate::ast::ToSql,
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
    RenameTable { table_name: String },
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ColumnDef {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
    /// `DEFAULT <restricted-expr>`
    pub default: Option<Expr>,
    pub options: Vec<ColumnOption>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ColumnOption {
    /// `{ PRIMARY KEY | UNIQUE }`
    Unique { is_primary: bool },
}

impl ToSql for AlterTableOperation {
    fn to_sql(&self) -> String {
        match self {
            AlterTableOperation::AddColumn { column_def } => {
                format!("ADD COLUMN {}", column_def.to_sql())
            }
            AlterTableOperation::DropColumn {
                column_name,
                if_exists,
            } => match if_exists {
                true => format!("DROP COLUMN IF EXISTS {column_name}"),
                false => format!("DROP COLUMN {column_name}"),
            },
            AlterTableOperation::RenameColumn {
                old_column_name,
                new_column_name,
            } => format!("RENAME COLUMN {old_column_name} TO {new_column_name}"),
            AlterTableOperation::RenameTable { table_name } => {
                format!("RENAME TO {table_name}")
            }
        }
    }
}

impl ToSql for ColumnDef {
    fn to_sql(&self) -> String {
        let ColumnDef {
            name,
            data_type,
            nullable,
            default,
            options,
        } = self;
        {
            let nullable = match nullable {
                true => "NULL",
                false => "NOT NULL",
            };
            let default = default.as_ref().map(ToSql::to_sql);
            let options = options
                .iter()
                .map(|option| option.to_sql())
                .collect::<Vec<_>>()
                .join(" ");

            let column_def = format!("{name} {data_type} {nullable}");

            match (default, options.is_empty()) {
                (None, true) => column_def,
                (None, false) => format!("{column_def} {options}"),
                (Some(default), true) => format!("{column_def} DEFAULT {default}"),
                (Some(default), false) => format!("{column_def} DEFAULT {default} {options}"),
            }
        }
    }
}

impl ToSql for ColumnOption {
    fn to_sql(&self) -> String {
        match self {
            ColumnOption::Unique { is_primary } => match is_primary {
                true => "PRIMARY KEY".to_owned(),
                false => "UNIQUE".to_owned(),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::ast::{AstLiteral, ColumnDef, ColumnOption, DataType, Expr, ToSql};

    #[test]
    fn to_sql_column_def() {
        assert_eq!(
            "name TEXT NOT NULL UNIQUE",
            ColumnDef {
                name: "name".to_owned(),
                data_type: DataType::Text,
                nullable: false,
                default: None,
                options: vec![ColumnOption::Unique { is_primary: false }]
            }
            .to_sql()
        );

        assert_eq!(
            "accepted BOOLEAN NULL",
            ColumnDef {
                name: "accepted".to_owned(),
                data_type: DataType::Boolean,
                nullable: true,
                default: None,
                options: Vec::new()
            }
            .to_sql()
        );

        assert_eq!(
            "id INT NOT NULL PRIMARY KEY",
            ColumnDef {
                name: "id".to_owned(),
                data_type: DataType::Int,
                nullable: false,
                default: None,
                options: vec![ColumnOption::Unique { is_primary: true }]
            }
            .to_sql()
        );

        assert_eq!(
            "accepted BOOLEAN NOT NULL DEFAULT FALSE",
            ColumnDef {
                name: "accepted".to_owned(),
                data_type: DataType::Boolean,
                nullable: false,
                default: Some(Expr::Literal(AstLiteral::Boolean(false))),
                options: Vec::new(),
            }
            .to_sql()
        );
    }
}
