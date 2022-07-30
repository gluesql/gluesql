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
    Ceil(ExprNode),
    Round(ExprNode),
    Floor(ExprNode),
    Asin(ExprNode),
    Acos(ExprNode),
    Atan(ExprNode),
    Sin(ExprNode),
    Cos(ExprNode),
    Tan(ExprNode),
    Pi,
    Now,
    Left(ExprNode, ExprNode),
    Log2(ExprNode),
    Log10(ExprNode),
    Ln(ExprNode),
    Right(ExprNode, ExprNode),
    Reverse(ExprNode),
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
            FunctionNode::Ceil(expr_node) => expr_node
                .try_into()
                .map(Function::Ceil)
                .map(Box::new)
                .map(Expr::Function),
            FunctionNode::Round(expr_node) => expr_node
                .try_into()
                .map(Function::Round)
                .map(Box::new)
                .map(Expr::Function),
            FunctionNode::Floor(expr_node) => expr_node
                .try_into()
                .map(Function::Floor)
                .map(Box::new)
                .map(Expr::Function),
            FunctionNode::Asin(expr_node) => expr_node
                .try_into()
                .map(Function::Asin)
                .map(Box::new)
                .map(Expr::Function),
            FunctionNode::Acos(expr_node) => expr_node
                .try_into()
                .map(Function::Acos)
                .map(Box::new)
                .map(Expr::Function),
            FunctionNode::Atan(expr_node) => expr_node
                .try_into()
                .map(Function::Atan)
                .map(Box::new)
                .map(Expr::Function),
            FunctionNode::Sin(expr_node) => expr_node
                .try_into()
                .map(Function::Sin)
                .map(Box::new)
                .map(Expr::Function),
            FunctionNode::Cos(expr_node) => expr_node
                .try_into()
                .map(Function::Cos)
                .map(Box::new)
                .map(Expr::Function),
            FunctionNode::Tan(expr_node) => expr_node
                .try_into()
                .map(Function::Tan)
                .map(Box::new)
                .map(Expr::Function),
            FunctionNode::Pi => Ok(Expr::Function(Box::new(Function::Pi()))),
            FunctionNode::Now => Ok(Expr::Function(Box::new(Function::Now()))),
            FunctionNode::Left(expr_node, size_node) => expr_node.try_into().and_then(|expr| {
                size_node
                    .try_into()
                    .map(|size| Function::Left { expr, size })
                    .map(Box::new)
                    .map(Expr::Function)
            }),
            FunctionNode::Log2(expr_node) => expr_node
                .try_into()
                .map(Function::Log2)
                .map(Box::new)
                .map(Expr::Function),
            FunctionNode::Log10(expr_node) => expr_node
                .try_into()
                .map(Function::Log10)
                .map(Box::new)
                .map(Expr::Function),
            FunctionNode::Ln(expr_node) => expr_node
                .try_into()
                .map(Function::Ln)
                .map(Box::new)
                .map(Expr::Function),
            FunctionNode::Right(expr_node, size_node) => expr_node.try_into().and_then(|expr| {
                size_node
                    .try_into()
                    .map(|size| Function::Right { expr, size })
                    .map(Box::new)
                    .map(Expr::Function)
            }),
            FunctionNode::Reverse(expr_node) => expr_node
                .try_into()
                .map(Function::Reverse)
                .map(Box::new)
                .map(Expr::Function),
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
    pub fn ceil(self) -> ExprNode {
        ceil(self)
    }
    pub fn round(self) -> ExprNode {
        round(self)
    }
    pub fn floor(self) -> ExprNode {
        floor(self)
    }
    pub fn asin(self) -> ExprNode {
        asin(self)
    }
    pub fn acos(self) -> ExprNode {
        acos(self)
    }
    pub fn atan(self) -> ExprNode {
        atan(self)
    }
    pub fn sin(self) -> ExprNode {
        sin(self)
    }
    pub fn cos(self) -> ExprNode {
        cos(self)
    }
    pub fn tan(self) -> ExprNode {
        tan(self)
    }
    pub fn left(self, size: Self) -> Self {
        left(self, size)
    }
    pub fn log2(self) -> ExprNode {
        log2(self)
    }
    pub fn log10(self) -> ExprNode {
        log10(self)
    }
    pub fn ln(self) -> ExprNode {
        ln(self)
    }
    pub fn right(self, size: Self) -> Self {
        right(self, size)
    }

    pub fn reverse(self) -> ExprNode {
        reverse(self)
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
pub fn ceil<T: Into<ExprNode>>(expr: T) -> ExprNode {
    ExprNode::Function(Box::new(FunctionNode::Ceil(expr.into())))
}
pub fn round<T: Into<ExprNode>>(expr: T) -> ExprNode {
    ExprNode::Function(Box::new(FunctionNode::Round(expr.into())))
}
pub fn floor<T: Into<ExprNode>>(expr: T) -> ExprNode {
    ExprNode::Function(Box::new(FunctionNode::Floor(expr.into())))
}
pub fn asin<T: Into<ExprNode>>(expr: T) -> ExprNode {
    ExprNode::Function(Box::new(FunctionNode::Asin(expr.into())))
}
pub fn acos<T: Into<ExprNode>>(expr: T) -> ExprNode {
    ExprNode::Function(Box::new(FunctionNode::Acos(expr.into())))
}
pub fn atan<T: Into<ExprNode>>(expr: T) -> ExprNode {
    ExprNode::Function(Box::new(FunctionNode::Atan(expr.into())))
}
pub fn sin<T: Into<ExprNode>>(expr: T) -> ExprNode {
    ExprNode::Function(Box::new(FunctionNode::Sin(expr.into())))
}
pub fn cos<T: Into<ExprNode>>(expr: T) -> ExprNode {
    ExprNode::Function(Box::new(FunctionNode::Cos(expr.into())))
}
pub fn tan<T: Into<ExprNode>>(expr: T) -> ExprNode {
    ExprNode::Function(Box::new(FunctionNode::Tan(expr.into())))
}
pub fn pi() -> ExprNode {
    ExprNode::Function(Box::new(FunctionNode::Pi))
}
pub fn now() -> ExprNode {
    ExprNode::Function(Box::new(FunctionNode::Now))
}
pub fn left<T: Into<ExprNode>, V: Into<ExprNode>>(expr: T, size: V) -> ExprNode {
    ExprNode::Function(Box::new(FunctionNode::Left(expr.into(), size.into())))
}
pub fn log2<T: Into<ExprNode>>(expr: T) -> ExprNode {
    ExprNode::Function(Box::new(FunctionNode::Log2(expr.into())))
}
pub fn log10<T: Into<ExprNode>>(expr: T) -> ExprNode {
    ExprNode::Function(Box::new(FunctionNode::Log10(expr.into())))
}
pub fn ln<T: Into<ExprNode>>(expr: T) -> ExprNode {
    ExprNode::Function(Box::new(FunctionNode::Ln(expr.into())))
}
pub fn right<T: Into<ExprNode>, V: Into<ExprNode>>(expr: T, size: V) -> ExprNode {
    ExprNode::Function(Box::new(FunctionNode::Right(expr.into(), size.into())))
}

pub fn reverse<T: Into<ExprNode>>(expr: T) -> ExprNode {
    ExprNode::Function(Box::new(FunctionNode::Reverse(expr.into())))
}

#[cfg(test)]
mod tests {
    use crate::ast_builder::{
        abs, acos, asin, atan, ceil, col, cos, expr, floor, ifnull, left, ln, log10, log2, now,
        num, pi, reverse, right, round, sin, tan, test_expr, text, upper,
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
    fn function_ceil() {
        let actual = ceil(col("num"));
        let expected = "CEIL(num)";
        test_expr(actual, expected);

        let actual = expr("base - 10").ceil();
        let expected = "CEIL(base - 10)";
        test_expr(actual, expected);
    }

    #[test]
    fn function_round() {
        let actual = round(col("num"));
        let expected = "ROUND(num)";
        test_expr(actual, expected);

        let actual = expr("base - 10").round();
        let expected = "ROUND(base - 10)";
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
    fn function_trigonometrics() {
        // asin
        let actual = asin(col("num"));
        let expected = "ASIN(num)";
        test_expr(actual, expected);

        let actual = col("num").asin();
        let expected = "ASIN(num)";
        test_expr(actual, expected);

        // acos
        let actual = acos(col("num"));
        let expected = "ACOS(num)";
        test_expr(actual, expected);

        let actual = col("num").acos();
        let expected = "ACOS(num)";
        test_expr(actual, expected);

        // atan
        let actual = atan(col("num"));
        let expected = "ATAN(num)";
        test_expr(actual, expected);

        let actual = col("num").atan();
        let expected = "ATAN(num)";
        test_expr(actual, expected);

        // sin
        let actual = sin(col("num"));
        let expected = "SIN(num)";
        test_expr(actual, expected);

        let actual = col("num").sin();
        let expected = "SIN(num)";
        test_expr(actual, expected);

        // cos
        let actual = cos(col("num"));
        let expected = "COS(num)";
        test_expr(actual, expected);

        let actual = col("num").cos();
        let expected = "COS(num)";
        test_expr(actual, expected);

        // tan
        let actual = tan(col("num"));
        let expected = "TAN(num)";
        test_expr(actual, expected);

        let actual = col("num").tan();
        let expected = "TAN(num)";
        test_expr(actual, expected);

        // pi
        let actual = pi();
        let expected = "PI()";
        test_expr(actual, expected);
    }

    #[test]
    fn function_now() {
        let actual = now();
        let expected = "NOW()";
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
    fn function_log2() {
        let actual = log2(col("num"));
        let expected = "LOG2(num)";
        test_expr(actual, expected);

        let actual = col("num").log2();
        let expected = "LOG2(num)";
        test_expr(actual, expected);
    }

    #[test]
    fn function_log10() {
        let actual = log10(col("num"));
        let expected = "LOG10(num)";
        test_expr(actual, expected);

        let actual = col("num").log10();
        let expected = "LOG10(num)";
        test_expr(actual, expected);
    }

    #[test]
    fn function_ln() {
        let actual = ln(num(2));
        let expected = "LN(2)";
        test_expr(actual, expected);

        let actual = num(2).ln();
        let expected = "LN(2)";
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

    #[test]
    fn function_reverse() {
        let actual = reverse(text("GlueSQL"));
        let expected = "REVERSE('GlueSQL')";
        test_expr(actual, expected);

        let actual = expr("GlueSQL").reverse();
        let expected = "REVERSE(GlueSQL)";
        test_expr(actual, expected);
    }
}
