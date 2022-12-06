use {
    super::{Build, ExprNode},
    crate::{
        ast::{Expr, Statement},
        result::Result,
    },
};

#[derive(Clone, Debug)]
pub struct DeleteNode<'a> {
    table_name: String,
    filter_expr: Option<ExprNode<'a>>,
}

impl<'a> DeleteNode<'a> {
    pub fn new(table_name: String) -> Self {
        Self {
            table_name,
            filter_expr: None,
        }
    }

    pub fn filter<T: Into<ExprNode<'a>>>(mut self, expr: T) -> Self {
        self.filter_expr = Some(expr.into());

        self
    }
}

impl<'a> Build for DeleteNode<'a> {
    fn build(self) -> Result<Statement> {
        let table_name = self.table_name;
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
        ast_builder::{col, table, test, Build},
    };

    #[test]
    fn delete() {
        let actual = table("Foo").delete().build();
        let expected = "DELETE FROM Foo";
        test(actual, expected);

        let actual = table("Bar").delete().filter("id < (1 + 3 + rate)").build();
        let expected = "DELETE FROM Bar WHERE id < (1 + 3 + rate)";
        test(actual, expected);

        let actual = table("Person")
            .delete()
            .filter(Expr::IsNull(Box::new(Expr::Identifier("name".to_owned()))))
            .build();
        let expected = "DELETE FROM Person WHERE name IS NULL";
        test(actual, expected);

        let actual = table("Person")
            .delete()
            .filter(col("name").is_null())
            .build();
        let expected = "DELETE FROM Person WHERE name IS NULL";
        test(actual, expected);
    }
}
