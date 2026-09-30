use {
    super::expr::{ExprPlan, plan_scalar_expr},
    crate::ast::{self, ColumnUniqueOption, DataType},
    serde::{Deserialize, Serialize},
};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct DefaultExpr {
    stored: ast::Expr,
    planned: ExprPlan,
}

impl DefaultExpr {
    fn from_ast(expr: ast::Expr) -> Self {
        let planned = plan_scalar_expr(expr.clone());

        Self {
            stored: expr,
            planned,
        }
    }

    pub fn planned(&self) -> &ExprPlan {
        &self.planned
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ColumnDefPlan {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
    pub unique: Option<ColumnUniqueOption>,
    pub comment: Option<String>,
    pub default: Option<DefaultExpr>,
}

impl ColumnDefPlan {
    pub fn to_column_def(&self) -> ast::ColumnDef {
        ast::ColumnDef {
            name: self.name.clone(),
            data_type: self.data_type.clone(),
            nullable: self.nullable,
            default: self.default.as_ref().map(|default| default.stored.clone()),
            unique: self.unique,
            comment: self.comment.clone(),
        }
    }
}

impl From<ast::ColumnDef> for ColumnDefPlan {
    fn from(column_def: ast::ColumnDef) -> Self {
        let ast::ColumnDef {
            name,
            data_type,
            nullable,
            default,
            unique,
            comment,
        } = column_def;

        Self {
            name,
            data_type,
            nullable,
            default: default.map(DefaultExpr::from_ast),
            unique,
            comment,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum TableColumnsPlan {
    Unplanned,
    Schemaless,
    Columns(Vec<ColumnDefPlan>),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum AlterTableOperationPlan {
    AddColumn {
        column_def: ColumnDefPlan,
    },
    DropColumn {
        column_name: String,
        if_exists: bool,
    },
    RenameColumn {
        old_column_name: String,
        new_column_name: String,
    },
    RenameTable {
        table_name: String,
    },
}

impl From<ast::AlterTableOperation> for AlterTableOperationPlan {
    fn from(operation: ast::AlterTableOperation) -> Self {
        match operation {
            ast::AlterTableOperation::AddColumn { column_def } => Self::AddColumn {
                column_def: column_def.into(),
            },
            ast::AlterTableOperation::DropColumn {
                column_name,
                if_exists,
            } => Self::DropColumn {
                column_name,
                if_exists,
            },
            ast::AlterTableOperation::RenameColumn {
                old_column_name,
                new_column_name,
            } => Self::RenameColumn {
                old_column_name,
                new_column_name,
            },
            ast::AlterTableOperation::RenameTable { table_name } => {
                Self::RenameTable { table_name }
            }
        }
    }
}
