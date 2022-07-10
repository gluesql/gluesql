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
    Floor(ExprNode),
    IfNull(ExprNode, ExprNode),
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
            FunctionNode::Floor(expr_node) => expr_node
                .try_into()
                .map(Function::Floor)
                .map(Box::new)
                .map(Expr::Function),
            FunctionNode::IfNull(expr_node, then_node) => expr_node.try_into().and_then(|expr| {
                then_node
                    .try_into()
                    .map(|then| Function::IfNull { expr, then })
                    .map(Box::new)
                    .map(Expr::Function)
            }),
        }
    }
}

impl ExprNode {
    pub fn abs(self) -> ExprNode {
        abs(self)
    }
    pub fn floor(self) -> ExprNode {
        floor(self)
    }
    pub fn ifnull(self, another: ExprNode) -> ExprNode {
        ifnull(self, another)
    }
}

pub fn abs<T: Into<ExprNode>>(expr: T) -> ExprNode {
    ExprNode::Function(Box::new(FunctionNode::Abs(expr.into())))
}

pub fn floor<T: Into<ExprNode>>(expr: T) -> ExprNode {
    ExprNode::Function(Box::new(FunctionNode::Floor(expr.into())))
}

pub fn ifnull<T: Into<ExprNode>, V: Into<ExprNode>>(expr: T, then: V) -> ExprNode {
    ExprNode::Function(Box::new(FunctionNode::IfNull(expr.into(), then.into())))
}

#[cfg(test)]
mod tests {
    use crate::ast_builder::{abs, col, expr, floor, test_expr, text};

    #[test]
    fn function_abs() {
        let actual = abs(col("num"));
        let expected = "ABS(num)";
        test_expr(actual, expected);

        let actual = expr("base - 10").abs();
        let expected = "ABS(base - 10)";
        test_expr(actual, expected);
    }

    #[test]
    fn function_floor() {
        let actual = floor(col("num"));
        let expected = "FLOOR(num)";
        test_expr(actual, expected);

        let actual = expr("base - 10").floor();
        let expected = "FLOOR(base - 10)";
        test_expr(actual, expected);
    }

    #[test]
    fn function_ifnull() {
        let actual = col("updated_at").ifnull(col("created_at"));
        let expected = "IFNULL(updated_at, created_at)";
        test_expr(actual, expected);

        let actual = text("HELLO").ifnull(text("WORLD"));
        let expected = "IFNULL('HELLO', 'WORLD')";
        test_expr(actual, expected);
    }
}
