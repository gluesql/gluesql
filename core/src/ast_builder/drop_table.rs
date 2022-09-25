use crate::{ast::Statement, result::Result};

#[derive(Clone)]
pub struct DropTableNode {
    table_name: String,
    if_exists: bool,
}

impl DropTableNode {
    pub fn new(table_name: String, exists: bool) -> Self {
        Self {
            table_name,
            if_exists: exists,
        }
    }

    pub fn build(self) -> Result<Statement> {
        let names = vec![self.table_name];
        let if_exists = self.if_exists;

        Ok(Statement::DropTable { names, if_exists })
    }
}

#[cfg(test)]
mod tests {
    use crate::ast_builder::{table, test};

    #[test]
    fn drop_table() {
        let actual = table("Foo").drop_table().build();
        let expected = "DROP TABLE Foo";
        test(actual, expected);

        let actual = table("Foo").drop_table_if_exists().build();
        let expected = "DROP TABLE IF EXISTS Foo";
        test(actual, expected);
    }
}
