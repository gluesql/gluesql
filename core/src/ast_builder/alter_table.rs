use {
    super::Build,
    crate::{
        ast::{AlterTableOperation, Statement},
        ast_builder::ColumnDefNode,
        result::Result,
    },
};

#[derive(Clone, Debug)]
pub struct AlterTableNode {
    table_name: String,
}

impl AlterTableNode {
    pub fn new(table_name: String) -> Self {
        Self { table_name }
    }

    pub fn add_column<T: Into<ColumnDefNode>>(self, column: T) -> AddColumnNode {
        AddColumnNode {
            table_node: self,
            column_def: column.into(),
        }
    }

    pub fn drop_column(self, column_name: &str) -> DropColumnNode {
        DropColumnNode {
            table_node: self,
            column_name: column_name.to_owned(),
            if_exists: false,
        }
    }

    pub fn drop_column_if_exists(self, column_name: &str) -> DropColumnNode {
        DropColumnNode {
            table_node: self,
            column_name: column_name.to_owned(),
            if_exists: true,
        }
    }

    pub fn rename_column(self, old_name: &str, new_name: &str) -> RenameColumnNode {
        RenameColumnNode {
            table_node: self,
            old_column_name: old_name.to_owned(),
            new_column_name: new_name.to_owned(),
        }
    }

    pub fn rename_table(self, new_table_name: &str) -> RenameTableNode {
        RenameTableNode {
            table_node: self,
            new_table_name: new_table_name.to_owned(),
        }
    }
}

pub struct AddColumnNode {
    table_node: AlterTableNode,
    column_def: ColumnDefNode,
}

impl Build for AddColumnNode {
    fn build(self) -> Result<Statement> {
        let table_name = self.table_node.table_name;
        let operation = AlterTableOperation::AddColumn {
            column_def: self.column_def.try_into()?,
        };
        Ok(Statement::AlterTable {
            name: table_name,
            operation,
        })
    }
}

pub struct DropColumnNode {
    table_node: AlterTableNode,
    column_name: String,
    if_exists: bool,
}

impl Build for DropColumnNode {
    fn build(self) -> Result<Statement> {
        let table_name = self.table_node.table_name;
        let operation = AlterTableOperation::DropColumn {
            column_name: self.column_name,
            if_exists: self.if_exists,
        };
        Ok(Statement::AlterTable {
            name: table_name,
            operation,
        })
    }
}

pub struct RenameColumnNode {
    table_node: AlterTableNode,
    old_column_name: String,
    new_column_name: String,
}

impl Build for RenameColumnNode {
    fn build(self) -> Result<Statement> {
        let table_name = self.table_node.table_name;
        let operation = AlterTableOperation::RenameColumn {
            old_column_name: self.old_column_name,
            new_column_name: self.new_column_name,
        };
        Ok(Statement::AlterTable {
            name: table_name,
            operation,
        })
    }
}

pub struct RenameTableNode {
    table_node: AlterTableNode,
    new_table_name: String,
}

impl Build for RenameTableNode {
    fn build(self) -> Result<Statement> {
        let old_table_name = self.table_node.table_name;
        let operation = AlterTableOperation::RenameTable {
            table_name: self.new_table_name,
        };
        Ok(Statement::AlterTable {
            name: old_table_name,
            operation,
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::ast_builder::{table, test, Build};

    #[test]
    fn alter_table() {
        let actual = table("Foo")
            .alter_table()
            .add_column("opt BOOLEAN NULL")
            .build();
        let expected = "ALTER TABLE Foo ADD COLUMN opt BOOLEAN NULL";
        test(actual, expected);

        let actual = table("Foo").alter_table().drop_column("col_name").build();
        let expected = "ALTER TABLE Foo DROP COLUMN col_name";
        test(actual, expected);

        let actual = table("Foo")
            .alter_table()
            .drop_column_if_exists("col_name")
            .build();
        let expected = "ALTER TABLE Foo DROP COLUMN IF EXISTS col_name";
        test(actual, expected);

        let actual = table("Foo")
            .alter_table()
            .rename_column("old", "new")
            .build();
        let expected = "ALTER TABLE Foo RENAME COLUMN old TO new";
        test(actual, expected);

        let actual = table("Foo")
            .alter_table()
            .rename_table("new_table_name")
            .build();
        let expected = "ALTER TABLE Foo RENAME TO new_table_name";
        test(actual, expected);
    }
}
