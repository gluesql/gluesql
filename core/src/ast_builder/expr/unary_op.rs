use {super::ExprNode, crate::ast::UnaryOperator};

impl ExprNode {
    pub fn plus(self) -> Self {
        plus(self)
    }
    pub fn minus(self) -> Self {
        minus(self)
    }
    pub fn not(self) -> Self {
        not(self)
    }
    pub fn factorial(self) -> Self {
        factorial(self)
    }
}

pub fn plus<T: Into<ExprNode>>(expr: T) -> ExprNode {
    ExprNode::UnaryOp {
        op: UnaryOperator::Plus,
        expr: Box::new(expr.into()),
    }
}

pub fn minus<T: Into<ExprNode>>(expr: T) -> ExprNode {
    ExprNode::UnaryOp {
        op: UnaryOperator::Minus,
        expr: Box::new(expr.into()),
    }
}

pub fn not<T: Into<ExprNode>>(expr: T) -> ExprNode {
    ExprNode::UnaryOp {
        op: UnaryOperator::Not,
        expr: Box::new(expr.into()),
    }
}

pub fn factorial<T: Into<ExprNode>>(expr: T) -> ExprNode {
    ExprNode::UnaryOp {
        op: UnaryOperator::Factorial,
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

        let actual = (col("count").gt(num(5))).not();
        let expected = "NOT count > 5";
        test_expr(actual, expected);

        let actual = num(10).factorial();
        let expected = "10!";
        test_expr(actual, expected);
    }
}
