use {
    super::Build,
    crate::{
        ast::Statement,
        result::Result,
    },
};

#[derive(Clone)]
pub struct ShowColumnsNode {
    table_name: String,
}

impl ShowColumnsNode {
    pub fn new(table_name: String) -> Self {
        Self { table_name }
    }
}

impl Build for ShowColumnsNode {
    fn build(self) -> Result<Statement> {
        let table_name = self.table_name;
        Ok(Statement::ShowColumns { table_name })
    }
}

#[cfg(test)]
mod tests {
    use crate::ast_builder::{table, test, Build};

    #[test]
    fn show_columns() {
        let actual = table("Foo").show_columns().build();
        let expected = "SHOW COLUMNS FROM Foo";
        test(actual, expected);
    }
}
