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
    pub unique: bool,
    pub comment: Option<String>,
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
            comment,
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
            let unique = unique.then_some("UNIQUE".to_owned());
            let comment = comment
                .as_ref()
                .map(|comment| format!("COMMENT '{}'", comment));

            [Some(column_def), default, unique, comment]
                .into_iter()
                .flatten()
                .collect::<Vec<_>>()
                .join(" ")
        }
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
        format!(r#""{name}" {data_type}{default}"#)
    }
}

#[cfg(test)]
mod tests {
    use crate::ast::{AstLiteral, ColumnDef, DataType, Expr, OperateFunctionArg, ToSql};

    #[test]
    fn to_sql_column_def() {
        assert_eq!(
            r#""name" TEXT NOT NULL UNIQUE"#,
            ColumnDef {
                name: "name".to_owned(),
                data_type: DataType::Text,
                nullable: false,
                default: None,
                unique: true,
                comment: None,
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
                unique: false,
                comment: None,
            }
            .to_sql()
        );

        assert_eq!(
            r#""id" INT NOT NULL"#,
            ColumnDef {
                name: "id".to_owned(),
                data_type: DataType::Int,
                nullable: false,
                default: None,
                unique: false,
                comment: None,
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
                unique: false,
                comment: None,
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
                unique: true,
                comment: None,
            }
            .to_sql()
        );

        assert_eq!(
            r#""accepted" BOOLEAN NOT NULL COMMENT 'this is comment'"#,
            ColumnDef {
                name: "accepted".to_owned(),
                data_type: DataType::Boolean,
                nullable: false,
                default: None,
                unique: false,
                comment: Some("this is comment".to_owned()),
            }
            .to_sql()
        );
    }

    #[test]
    fn to_sql_operate_function_arg() {
        assert_eq!(
            r#""name" TEXT"#,
            OperateFunctionArg {
                name: "name".to_owned(),
                data_type: DataType::Text,
                default: None,
            }
            .to_sql()
        );

        assert_eq!(
            r#""accepted" BOOLEAN DEFAULT FALSE"#,
            OperateFunctionArg {
                name: "accepted".to_owned(),
                data_type: DataType::Boolean,
                default: Some(Expr::Literal(AstLiteral::Boolean(false))),
            }
            .to_sql()
        );
    }
}
