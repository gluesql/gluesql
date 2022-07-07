use {super::ExprNode, crate::ast::BinaryOperator};

impl ExprNode {
    fn binary_op<T: Into<Self>>(self, op: BinaryOperator, other: T) -> Self {
        Self::BinaryOp {
            left: Box::new(self),
            op,
            right: Box::new(other.into()),
        }
    }

    #[allow(clippy::should_implement_trait)]
    pub fn add<T: Into<Self>>(self, other: T) -> Self {
        self.binary_op(BinaryOperator::Plus, other)
    }

    #[allow(clippy::should_implement_trait)]
    pub fn sub<T: Into<Self>>(self, other: T) -> Self {
        self.binary_op(BinaryOperator::Minus, other)
    }

    #[allow(clippy::should_implement_trait)]
    pub fn mul<T: Into<Self>>(self, other: T) -> Self {
        self.binary_op(BinaryOperator::Multiply, other)
    }

    #[allow(clippy::should_implement_trait)]
    pub fn div<T: Into<Self>>(self, other: T) -> Self {
        self.binary_op(BinaryOperator::Divide, other)
    }

    pub fn concat<T: Into<Self>>(self, other: T) -> Self {
        self.binary_op(BinaryOperator::StringConcat, other)
    }

    pub fn eq<T: Into<Self>>(self, other: T) -> Self {
        self.binary_op(BinaryOperator::Eq, other)
    }

    pub fn neq<T: Into<Self>>(self, other: T) -> Self {
        self.binary_op(BinaryOperator::NotEq, other)
    }

    pub fn gt<T: Into<Self>>(self, other: T) -> Self {
        self.binary_op(BinaryOperator::Gt, other)
    }
}

#[cfg(test)]
mod tests {
    use crate::ast_builder::{col, num, test_expr, text};

    #[test]
    fn binary_op() {
        let actual = col("id").add(10);
        let expected = "id + 10";
        test_expr(actual, expected);

        let actual = num(10).sub(text("abc"));
        let expected = "10 - 'abc'";
        test_expr(actual, expected);

        let actual = col("rate").mul("amount");
        let expected = "rate * amount";
        test_expr(actual, expected);

        let actual = col("amount").div(30);
        let expected = "amount / 30";
        test_expr(actual, expected);

        let actual = text("hello").concat(r#""world""#);
        let expected = "'hello' || 'world'";
        test_expr(actual, expected);

        let actual = col("id").eq(10);
        let expected = "id = 10";
        test_expr(actual, expected);

        let actual = col("id").neq("'abcde'");
        let expected = r#"id != "abcde""#;
        test_expr(actual, expected);

        let actual = col("id").gt(col("Bar.id"));
        let expected = "id > Bar.id";
        test_expr(actual, expected);
    }
}
