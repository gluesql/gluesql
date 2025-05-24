use {
    super::Build,
    crate::{ast::Statement, result::Result},
};

#[derive(Clone, Debug)]
pub struct ExplainTableNode {
    table_name: String,
}

impl ExplainTableNode {
    pub fn new(table_name: String) -> Self {
        Self { table_name }
    }
}

impl Build for ExplainTableNode {
    fn build(self) -> Result<Statement> {
        let table_name = self.table_name;
        Ok(Statement::ExplainTable { table_name })
    }
}

#[cfg(test)]
mod tests {
    use crate::ast_builder::{table, test, Build};

    #[test]
    fn explain_table() {
        let actual = table("Foo").explain().build();
        let expected = "EXPLAIN Foo";
        test(actual, expected);
    }
}
