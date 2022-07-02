use {
    super::ExprNode,
    crate::{
        ast::{Expr, ObjectName, Statement},
        result::Result,
    },
};

#[derive(Clone)]
pub struct DeleteNode {
    table_name: String,
    filter_expr: Option<ExprNode>,
}

impl DeleteNode {
    pub fn new(table_name: String) -> Self {
        Self {
            table_name,
            filter_expr: None,
        }
    }

    pub fn filter<T: Into<ExprNode>>(mut self, expr: T) -> Self {
        self.filter_expr = Some(expr.into());

        self
    }

    pub fn build(self) -> Result<Statement> {
        let table_name = ObjectName(vec![self.table_name]);
        let selection = self.filter_expr.map(Expr::try_from).transpose()?;

        Ok(Statement::Delete {
            table_name,
            selection,
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        ast::Expr,
        ast_builder::{col, test, Builder},
    };

    #[test]
    fn delete() {
        let actual = Builder::table("Foo").delete().build();
        let expected = "DELETE FROM Foo";
        test(actual, expected);

        let actual = Builder::table("Bar")
            .delete()
            .filter("id < (1 + 3 + rate)")
            .build();
        let expected = "DELETE FROM Bar WHERE id < (1 + 3 + rate)";
        test(actual, expected);

        let actual = Builder::table("Person")
            .delete()
            .filter(Expr::IsNull(Box::new(Expr::Identifier("name".to_owned()))))
            .build();
        let expected = "DELETE FROM Person WHERE name IS NULL";
        test(actual, expected);

        let actual = Builder::table("Person")
            .delete()
            .filter(col("name").is_null())
            .build();
        let expected = "DELETE FROM Person WHERE name IS NULL";
        test(actual, expected);
    }
}
