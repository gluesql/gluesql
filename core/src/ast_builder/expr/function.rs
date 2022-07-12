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
    Upper(ExprNode),
    IfNull(ExprNode, ExprNode),
    Floor(ExprNode),
    Left(ExprNode, ExprNode),
    Right(ExprNode, ExprNode),
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
            FunctionNode::Upper(expr_node) => expr_node
                .try_into()
                .map(Function::Upper)
                .map(Box::new)
                .map(Expr::Function),
            FunctionNode::IfNull(expr_node, then_node) => expr_node.try_into().and_then(|expr| {
                then_node
                    .try_into()
                    .map(|then| Function::IfNull { expr, then })
                    .map(Box::new)
                    .map(Expr::Function)
            }),
            FunctionNode::Floor(expr_node) => expr_node
                .try_into()
                .map(Function::Floor)
                .map(Box::new)
                .map(Expr::Function),
            FunctionNode::Left(expr_node, size_node) => expr_node.try_into().and_then(|expr| {
                size_node
                    .try_into()
                    .map(|size| Function::Left { expr, size })
                    .map(Box::new)
                    .map(Expr::Function)
            }),
            FunctionNode::Right(expr_node, size_node) => expr_node.try_into().and_then(|expr| {
                size_node
                    .try_into()
                    .map(|size| Function::Right { expr, size })
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

    pub fn upper(self) -> ExprNode {
        upper(self)
    }
    pub fn ifnull(self, another: ExprNode) -> ExprNode {
        ifnull(self, another)
    }
    pub fn floor(self) -> ExprNode {
        floor(self)
    }

    pub fn left(self, size: Self) -> Self {
        left(self, size)
    }

    pub fn right(self, size: Self) -> Self {
        right(self, size)
    }
}

pub fn abs<T: Into<ExprNode>>(expr: T) -> ExprNode {
    ExprNode::Function(Box::new(FunctionNode::Abs(expr.into())))
}

pub fn upper<T: Into<ExprNode>>(expr: T) -> ExprNode {
    ExprNode::Function(Box::new(FunctionNode::Upper(expr.into())))
}
pub fn ifnull<T: Into<ExprNode>, V: Into<ExprNode>>(expr: T, then: V) -> ExprNode {
    ExprNode::Function(Box::new(FunctionNode::IfNull(expr.into(), then.into())))
}

pub fn floor<T: Into<ExprNode>>(expr: T) -> ExprNode {
    ExprNode::Function(Box::new(FunctionNode::Floor(expr.into())))
}

pub fn left<T: Into<ExprNode>, V: Into<ExprNode>>(expr: T, size: V) -> ExprNode {
    ExprNode::Function(Box::new(FunctionNode::Left(expr.into(), size.into())))
}

pub fn right<T: Into<ExprNode>, V: Into<ExprNode>>(expr: T, size: V) -> ExprNode {
    ExprNode::Function(Box::new(FunctionNode::Right(expr.into(), size.into())))
}

#[cfg(test)]
mod tests {
    use crate::ast_builder::{
        abs, col, expr, floor, ifnull, left, num, right, test_expr, text, upper,
    };

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
    fn function_upper() {
        // Upper
        let actual = upper(text("ABC"));
        let expected = "UPPER('ABC')";
        test_expr(actual, expected);

        let actual = expr("HoHo").upper();
        let expected = "UPPER(HoHo)";
        test_expr(actual, expected);
    }
    #[test]
    fn function_ifnull() {
        let actual = ifnull(text("HELLO"), text("WORLD"));
        let expected = "IFNULL('HELLO', 'WORLD')";
        test_expr(actual, expected);

        let actual = col("updated_at").ifnull(col("created_at"));
        let expected = "IFNULL(updated_at, created_at)";
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
    fn function_left() {
        let actual = left(text("GlueSQL"), num(2));
        let expected = "LEFT('GlueSQL', 2)";
        test_expr(actual, expected);

        let actual = expr("GlueSQL").left(num(2));
        let expected = "LEFT(GlueSQL, 2)";
        test_expr(actual, expected);
    }

    #[test]
    fn function_right() {
        let actual = right(text("GlueSQL"), num(2));
        let expected = "RIGHT('GlueSQL', 2)";
        test_expr(actual, expected);

        let actual = expr("GlueSQL").right(num(2));
        let expected = "RIGHT(GlueSQL, 2)";
        test_expr(actual, expected);
    }
}
