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
        ast::Statement, ast_builder::Builder, parse_sql::parse, result::Result,
        translate::translate,
    };

    fn stmt(sql: &str) -> Result<Statement> {
        let parsed = &parse(sql).unwrap()[0];

        translate(parsed)
    }

    #[test]
    fn delete() {
        let actual = Builder::table("Foo").delete().build();
        let expected = stmt("DELETE FROM Foo");
        assert_eq!(actual, expected);

        let actual = Builder::table("Bar")
            .delete()
            .filter("id < (1 + 3 + rate)")
            .build();
        let expected = stmt("DELETE FROM Bar WHERE id < (1 + 3 + rate)");
        assert_eq!(actual, expected);
    }
}
