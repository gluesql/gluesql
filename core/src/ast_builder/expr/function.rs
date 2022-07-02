use {
    super::ExprNode,
    crate::{
        ast::{Expr, Function},
        result::{Error, Result},
    },
};

#[derive(Clone)]
pub enum FunctionNode {
    Abs(ExprNode),
}

impl TryFrom<FunctionNode> for Expr {
    type Error = Error;

    fn try_from(func_node: FunctionNode) -> Result<Self> {
        match func_node {
            FunctionNode::Abs(expr_node) => expr_node
                .try_into()
                .map(Function::Abs)
                .map(Box::new)
                .map(Expr::Function),
        }
    }
}

pub fn abs<T: Into<ExprNode>>(expr: T) -> ExprNode {
    ExprNode::Function(Box::new(FunctionNode::Abs(expr.into())))
}

#[cfg(test)]
mod tests {
    use crate::ast_builder::{abs, col, num, test_expr};

    #[test]
    fn function() {
        // ABS
        let actual = abs(col("num"));
        let expected = "ABS(num)";
        test_expr(actual, expected);

        let actual = abs(num(10).add(col("base")));
        let expected = "ABS(10 + base)";
        test_expr(actual, expected);

        let actual = abs("base - 10");
        let expected = "ABS(base - 10)";
        test_expr(actual, expected);

        // TODO: It is sufficient to add a single unit test for each function.
    }
}
