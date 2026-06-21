use {
    super::Build,
    crate::{ast::Statement, plan::StatementPlan, result::Result},
};

#[derive(Clone, Debug)]
pub struct ShowColumnsNode {
    table_name: String,
}

impl ShowColumnsNode {
    pub fn new(table_name: String) -> Self {
        Self { table_name }
    }
}

impl Build for ShowColumnsNode {
    fn build(self) -> Result<StatementPlan> {
        let table_name = self.table_name;
        Ok(Statement::ShowColumns { table_name }.into())
    }
}

#[cfg(test)]
mod tests {
    use crate::query_builder::{Build, table, test};

    #[test]
    fn show_columns() {
        let actual = table("Foo").show_columns().build();
        let expected = "SHOW COLUMNS FROM Foo";
        test(&actual, expected);
    }
}
