use super::ExprNode;

impl<'a> ExprNode<'a> {
    pub fn nested(self) -> Self {
        nested(self)
    }
}

pub fn nested<'a, T: Into<ExprNode<'a>>>(expr: T) -> ExprNode<'a> {
    ExprNode::Nested(Box::new(expr.into()))
}

#[cfg(test)]
mod tests {
    use crate::ast_builder::{col, nested, test_expr};

    #[test]
    fn test_nested() {
        let actual = col("val1").add(col("val2")).nested();
        let expected = "(val1 + val2)";
        test_expr(actual, expected);

        let actual = nested(col("val1").add(col("val2")));
        let expected = "(val1 + val2)";
        test_expr(actual, expected);
    }
}
