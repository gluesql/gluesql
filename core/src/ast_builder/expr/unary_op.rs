use {super::ExprNode, crate::ast::UnaryOperator};

impl ExprNode<'_> {
    #[must_use]
    pub fn plus(self) -> Self {
        plus(self)
    }
    #[must_use]
    pub fn minus(self) -> Self {
        minus(self)
    }
    #[allow(clippy::should_implement_trait)]
    #[must_use]
    pub fn negate(self) -> Self {
        not(self)
    }
    #[must_use]
    pub fn factorial(self) -> Self {
        factorial(self)
    }
    #[must_use]
    pub fn bitwise_not(self) -> Self {
        bitwise_not(self)
    }
}

pub fn plus<'a, T: Into<ExprNode<'a>>>(expr: T) -> ExprNode<'a> {
    ExprNode::UnaryOp {
        op: UnaryOperator::Plus,
        expr: Box::new(expr.into()),
    }
}

pub fn minus<'a, T: Into<ExprNode<'a>>>(expr: T) -> ExprNode<'a> {
    ExprNode::UnaryOp {
        op: UnaryOperator::Minus,
        expr: Box::new(expr.into()),
    }
}

pub fn not<'a, T: Into<ExprNode<'a>>>(expr: T) -> ExprNode<'a> {
    ExprNode::UnaryOp {
        op: UnaryOperator::Not,
        expr: Box::new(expr.into()),
    }
}

pub fn factorial<'a, T: Into<ExprNode<'a>>>(expr: T) -> ExprNode<'a> {
    ExprNode::UnaryOp {
        op: UnaryOperator::Factorial,
        expr: Box::new(expr.into()),
    }
}

pub fn bitwise_not<'a, T: Into<ExprNode<'a>>>(expr: T) -> ExprNode<'a> {
    ExprNode::UnaryOp {
        op: UnaryOperator::BitwiseNot,
        expr: Box::new(expr.into()),
    }
}

#[cfg(test)]
mod tests {
    use crate::ast_builder::{col, num, test_expr};

    #[test]
    fn unary_op() {
        let actual = num(5).plus();
        let expected = "+5";
        test_expr(actual, expected);

        let actual = num(10).minus();
        let expected = "-10";
        test_expr(actual, expected);

        let actual = (col("count").gt(num(5))).negate();
        let expected = "NOT count > 5";
        test_expr(actual, expected);

        let actual = num(10).factorial();
        let expected = "10!";
        test_expr(actual, expected);

        let actual = num(10).bitwise_not();
        let expected = "~10";
        test_expr(actual, expected);
    }
}
