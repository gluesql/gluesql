use {
    super::Build,
    crate::{ast::Statement, result::Result},
};

#[derive(Clone, Debug)]
pub struct DropTableNode {
    table_name: String,
    if_exists: bool,
    cascade: bool,
}

impl DropTableNode {
    pub fn new(table_name: String, exists: bool, cascade: bool) -> Self {
        Self {
            table_name,
            if_exists: exists,
            cascade,
        }
    }
}

impl Build for DropTableNode {
    fn build(self) -> Result<Statement> {
        let names = vec![self.table_name];
        let if_exists = self.if_exists;
        let cascade = self.cascade;

        Ok(Statement::DropTable {
            names,
            if_exists,
            cascade,
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::ast_builder::{Build, table, test};

    #[test]
    fn drop_table() {
        let actual = table("Foo").drop_table().build();
        let expected = "DROP TABLE Foo";
        test(actual, expected);

        let actual = table("Foo").drop_table_if_exists().build();
        let expected = "DROP TABLE IF EXISTS Foo";
        test(actual, expected);

        let actual = table("Foo").drop_table_cascade().build();
        let expected = "DROP TABLE Foo CASCADE";
        test(actual, expected);

        let actual = table("Foo").drop_table_if_exists_cascade().build();
        let expected = "DROP TABLE IF EXISTS Foo CASCADE";
        test(actual, expected);
    }
}
