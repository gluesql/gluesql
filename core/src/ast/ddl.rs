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
    /// `{ PRIMARY KEY | UNIQUE }`
    pub unique: Option<ColumnUniqueOption>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ColumnUniqueOption {
    pub is_primary: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct OperateFunctionArg {
    pub name: String,
    pub data_type: DataType,
    /// `DEFAULT <restricted-expr>`
    pub default: Option<Expr>,
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
                true => format!(r#"DROP COLUMN IF EXISTS "{column_name}""#),
                false => format!(r#"DROP COLUMN "{column_name}""#),
            },
            AlterTableOperation::RenameColumn {
                old_column_name,
                new_column_name,
            } => format!(r#"RENAME COLUMN "{old_column_name}" TO "{new_column_name}""#),
            AlterTableOperation::RenameTable { table_name } => {
                format!(r#"RENAME TO "{table_name}""#)
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
            unique,
        } = self;
        {
            let nullable = match nullable {
                true => "NULL",
                false => "NOT NULL",
            };
            let column_def = format!(r#""{name}" {data_type} {nullable}"#);
            let default = default
                .as_ref()
                .map(|expr| format!("DEFAULT {}", expr.to_sql()));
            let unique = unique.as_ref().map(ToSql::to_sql);

            [Some(column_def), default, unique]
                .into_iter()
                .flatten()
                .collect::<Vec<_>>()
                .join(" ")
        }
    }
}

impl ToSql for ColumnUniqueOption {
    fn to_sql(&self) -> String {
        if self.is_primary {
            "PRIMARY KEY"
        } else {
            "UNIQUE"
        }
        .to_owned()
    }
}

impl ToSql for OperateFunctionArg {
    fn to_sql(&self) -> String {
        let OperateFunctionArg {
            name,
            data_type,
            default,
        } = self;
        let default = default
            .as_ref()
            .map(|expr| format!(" DEFAULT {}", expr.to_sql()))
            .unwrap_or_else(|| "".to_owned());
        format!("{name} {data_type}{default}")
    }
}

#[cfg(test)]
mod tests {
    use crate::ast::{
        AstLiteral, ColumnDef, ColumnUniqueOption, DataType, Expr, OperateFunctionArg, ToSql,
    };

    #[test]
    fn to_sql_column_def() {
        assert_eq!(
            r#""name" TEXT NOT NULL UNIQUE"#,
            ColumnDef {
                name: "name".to_owned(),
                data_type: DataType::Text,
                nullable: false,
                default: None,
                unique: Some(ColumnUniqueOption { is_primary: false }),
            }
            .to_sql()
        );

        assert_eq!(
            r#""accepted" BOOLEAN NULL"#,
            ColumnDef {
                name: "accepted".to_owned(),
                data_type: DataType::Boolean,
                nullable: true,
                default: None,
                unique: None,
            }
            .to_sql()
        );

        assert_eq!(
            r#""id" INT NOT NULL PRIMARY KEY"#,
            ColumnDef {
                name: "id".to_owned(),
                data_type: DataType::Int,
                nullable: false,
                default: None,
                unique: Some(ColumnUniqueOption { is_primary: true }),
            }
            .to_sql()
        );

        assert_eq!(
            r#""accepted" BOOLEAN NOT NULL DEFAULT FALSE"#,
            ColumnDef {
                name: "accepted".to_owned(),
                data_type: DataType::Boolean,
                nullable: false,
                default: Some(Expr::Literal(AstLiteral::Boolean(false))),
                unique: None,
            }
            .to_sql()
        );

        assert_eq!(
            r#""accepted" BOOLEAN NOT NULL DEFAULT FALSE UNIQUE"#,
            ColumnDef {
                name: "accepted".to_owned(),
                data_type: DataType::Boolean,
                nullable: false,
                default: Some(Expr::Literal(AstLiteral::Boolean(false))),
                unique: Some(ColumnUniqueOption { is_primary: false }),
            }
            .to_sql()
        );
    }

    #[test]
    fn to_sql_operate_function_arg() {
        assert_eq!(
            "name TEXT",
            OperateFunctionArg {
                name: "name".to_owned(),
                data_type: DataType::Text,
                default: None,
            }
            .to_sql()
        );

        assert_eq!(
            "accepted BOOLEAN DEFAULT FALSE",
            OperateFunctionArg {
                name: "accepted".to_owned(),
                data_type: DataType::Boolean,
                default: Some(Expr::Literal(AstLiteral::Boolean(false))),
            }
            .to_sql()
        );
    }
}
