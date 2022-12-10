use {
    super::Build,
    crate::{ast::Statement, ast_builder::ColumnDefNode, result::Result},
};

#[derive(Clone, Debug)]
pub struct CreateTableNode {
    table_name: String,
    if_not_exists: bool,
    columns: Vec<ColumnDefNode>,
}

impl CreateTableNode {
    pub fn new(table_name: String, not_exists: bool) -> Self {
        Self {
            table_name,
            if_not_exists: not_exists,
            columns: Vec::new(),
        }
    }

    pub fn add_column<T: Into<ColumnDefNode>>(mut self, column: T) -> Self {
        self.columns.push(column.into());
        self
    }
}

impl Build for CreateTableNode {
    fn build(self) -> Result<Statement> {
        let table_name = self.table_name;
        let columns = self
            .columns
            .into_iter()
            .map(TryInto::try_into)
            .collect::<Result<Vec<_>>>()?;

        Ok(Statement::CreateTable {
            name: table_name,
            if_not_exists: self.if_not_exists,
            columns,
            source: None,
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::ast_builder::{table, test, Build};

    #[test]
    fn create_table() {
        let actual = table("Foo")
            .create_table()
            .add_column("id INTEGER NULL")
            .add_column("num INTEGER")
            .add_column("name TEXT")
            .build();
        let expected = "CREATE TABLE Foo (id INTEGER NULL, num INTEGER, name TEXT)";
        test(actual, expected);

        let actual = table("Foo")
            .create_table_if_not_exists()
            .add_column("id UUID UNIQUE")
            .add_column("name TEXT")
            .build();
        let expected = "CREATE TABLE IF NOT EXISTS Foo (id UUID UNIQUE, name TEXT)";
        test(actual, expected);
    }

    #[test]
    fn create_table_without_column() {
        let actual = table("Foo").create_table().build();
        let expected = "CREATE TABLE Foo";
        test(actual, expected);
    }
}
