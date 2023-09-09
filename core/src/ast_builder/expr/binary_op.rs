use {super::ExprNode, crate::ast::BinaryOperator};

impl<'a> ExprNode<'a> {
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

    pub fn modulo<T: Into<Self>>(self, other: T) -> Self {
        self.binary_op(BinaryOperator::Modulo, other)
    }

    pub fn concat<T: Into<Self>>(self, other: T) -> Self {
        self.binary_op(BinaryOperator::StringConcat, other)
    }

    pub fn gt<T: Into<Self>>(self, other: T) -> Self {
        self.binary_op(BinaryOperator::Gt, other)
    }

    pub fn lt<T: Into<Self>>(self, other: T) -> Self {
        self.binary_op(BinaryOperator::Lt, other)
    }

    pub fn gte<T: Into<Self>>(self, other: T) -> Self {
        self.binary_op(BinaryOperator::GtEq, other)
    }

    pub fn lte<T: Into<Self>>(self, other: T) -> Self {
        self.binary_op(BinaryOperator::LtEq, other)
    }

    pub fn eq<T: Into<Self>>(self, other: T) -> Self {
        self.binary_op(BinaryOperator::Eq, other)
    }

    pub fn neq<T: Into<Self>>(self, other: T) -> Self {
        self.binary_op(BinaryOperator::NotEq, other)
    }

    pub fn and<T: Into<Self>>(self, other: T) -> Self {
        self.binary_op(BinaryOperator::And, other)
    }

    pub fn or<T: Into<Self>>(self, other: T) -> Self {
        self.binary_op(BinaryOperator::Or, other)
    }

    pub fn bitwise_and<T: Into<Self>>(self, other: T) -> Self {
        self.binary_op(BinaryOperator::BitwiseAnd, other)
    }

    pub fn bitwise_shift_left<T: Into<Self>>(self, other: T) -> Self {
        self.binary_op(BinaryOperator::BitwiseShiftLeft, other)
    }

    pub fn bitwise_shift_right<T: Into<Self>>(self, other: T) -> Self {
        self.binary_op(BinaryOperator::BitwiseShiftRight, other)
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

        let actual = col("amount").modulo(30);
        let expected = "amount % 30";
        test_expr(actual, expected);

        let actual = text("hello").concat("'world'");
        let expected = "'hello' || 'world'";
        test_expr(actual, expected);

        let actual = col("id").gt(col("Bar.id"));
        let expected = "id > Bar.id";
        test_expr(actual, expected);

        let actual = col("id").lt(col("Bar.id"));
        let expected = "id < Bar.id";
        test_expr(actual, expected);

        let actual = col("id").gte(col("Bar.id"));
        let expected = "id >= Bar.id";
        test_expr(actual, expected);

        let actual = col("id").lte(col("Bar.id"));
        let expected = "id <= Bar.id";
        test_expr(actual, expected);

        let actual = col("id").eq(10);
        let expected = "id = 10";
        test_expr(actual, expected);

        let actual = col("id").neq("'abcde'");
        let expected = "id != 'abcde'";
        test_expr(actual, expected);

        let actual = (col("id").gt(num(10))).and(col("id").lt(num(20)));
        let expected = "id > 10 AND id < 20";
        test_expr(actual, expected);

        let actual = (col("id").gt(num(10))).or(col("id").lt(num(20)));
        let expected = "id > 10 OR id < 20";
        test_expr(actual, expected);

        let actual = col("id").bitwise_and(col("value"));
        let expected = "id & value";
        test_expr(actual, expected);

        let actual = col("id").bitwise_shift_left(num(1));
        let expected = "id << 1";
        test_expr(actual, expected);

        let actual = col("id").bitwise_shift_right(num(1));
        let expected = "id >> 1";
        test_expr(actual, expected);
    }
}
