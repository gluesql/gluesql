use super::ExprNode;

impl<'a> ExprNode<'a> {
    pub fn is_null(self) -> Self {
        Self::IsNull(Box::new(self))
    }

    pub fn is_not_null(self) -> Self {
        Self::IsNotNull(Box::new(self))
    }
}

#[cfg(test)]
mod tests {
    use crate::ast_builder::{col, num, test_expr};

    #[test]
    fn is_null() {
        let actual = col("id").is_null();
        let expected = "id IS NULL";
        test_expr(actual, expected);

        let actual = num(10).add("id").is_not_null();
        let expected = "10 + id IS NOT NULL";
        test_expr(actual, expected);
    }
}
