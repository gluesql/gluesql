use {
    crate::ast,
    serde::{Deserialize, Serialize},
};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum AlterTableOperationPlan {
    AddColumn {
        column_def: ast::ColumnDef,
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
            ast::AlterTableOperation::AddColumn { column_def } => Self::AddColumn { column_def },
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
