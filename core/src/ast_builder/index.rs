use {
    super::{Build, OrderByExprNode},
    crate::{ast::Statement, result::Result},
};

#[derive(Clone, Debug)]
pub struct CreateIndexNode<'a> {
    name: String,
    table_name: String,
    column: OrderByExprNode<'a>,
}

impl<'a> CreateIndexNode<'a> {
    pub fn new(table_name: String, name: String, column: OrderByExprNode<'a>) -> Self {
        Self {
            table_name,
            name,
            column,
        }
    }
}

impl<'a> Build for CreateIndexNode<'a> {
    fn build(self) -> Result<Statement> {
        let table_name = self.table_name;
        let name = self.name;
        let column = self.column.try_into()?;

        Ok(Statement::CreateIndex {
            name,
            table_name,
            column,
        })
    }
}

#[derive(Clone)]
pub struct DropIndexNode {
    name: String,
    table_name: String,
}

impl DropIndexNode {
    pub fn new(table_name: String, name: String) -> Self {
        Self { table_name, name }
    }
}

impl Build for DropIndexNode {
    fn build(self) -> Result<Statement> {
        let table_name = self.table_name;
        let name = self.name;

        Ok(Statement::DropIndex { name, table_name })
    }
}

#[cfg(test)]
mod tests {
    use crate::ast_builder::{table, test, Build};

    #[test]
    fn create_index() {
        let actual = table("Foo").create_index("nameIndex", "name asc").build();
        let expected = "CREATE INDEX nameIndex ON Foo (name Asc)";
        test(actual, expected);

        let actual = table("Foo").create_index("nameIndex", "name desc").build();
        let expected = "CREATE INDEX nameIndex ON Foo (name Desc)";
        test(actual, expected);
    }

    #[test]
    fn drop_index() {
        let actual = table("Foo").drop_index("nameIndex").build();
        let expected = "DROP INDEX Foo.nameIndex";
        test(actual, expected);
    }
}
