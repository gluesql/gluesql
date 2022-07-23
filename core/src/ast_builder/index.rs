#![cfg(feature = "index")]

use crate::{
    ast::{Expr, ObjectName, OrderByExpr, Statement},
    result::Result,
};

#[derive(Clone)]
pub struct CreateIndexNode {
    name: String,
    table_name: String,
    column: String,
    asc: bool,
}

impl CreateIndexNode {
    pub fn new(table_name: String, name: String) -> Self {
        Self {
            table_name,
            name,
            column: String::new(),
            asc: true,
        }
    }

    pub fn column(mut self, column: &str, asc: bool) -> Self {
        self.column = column.to_string();
        self.asc = asc;
        self
    }

    pub fn build(self) -> Result<Statement> {
        let table_name = ObjectName(vec![self.table_name]);
        let name = ObjectName(vec![self.name]);
        let column = OrderByExpr {
            expr: Expr::Identifier(self.column.clone()),
            asc: Some(self.asc),
        };

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

    pub fn build(self) -> Result<Statement> {
        let table_name = ObjectName(vec![self.table_name]);
        let name = ObjectName(vec![self.name]);

        Ok(Statement::DropIndex { name, table_name })
    }
}

#[cfg(feature = "index")]
#[cfg(test)]
mod tests {
    use crate::ast_builder::{table, test};

    #[test]
    fn create_index() {
        let actual = table("Foo")
            .create_index("nameIndex")
            .column("name", true)
            .build();
        let expected = "CREATE INDEX nameIndex ON Foo (name Asc)";
        test(actual, expected);

        let actual = table("Foo")
            .create_index("nameIndex")
            .column("name", false)
            .build();
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
