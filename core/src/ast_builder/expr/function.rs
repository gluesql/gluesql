use {
    super::ExprNode,
    crate::{
        ast::Function,
        ast_builder::ExprList,
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
    Log(ExprNode, ExprNode),
    Log2(ExprNode),
    Log10(ExprNode),
    Ln(ExprNode),
    Right(ExprNode, ExprNode),
    Reverse(ExprNode),
    Sign(ExprNode),
    Power(ExprNode, ExprNode),
    Sqrt(ExprNode),
    Gcd(ExprNode, ExprNode),
    Lcm(ExprNode, ExprNode),
    GenerateUuid,
    Repeat(ExprNode, ExprNode),
    Exp(ExprNode),
    Lpad {
        expr: ExprNode,
        size: ExprNode,
        fill: Option<ExprNode>,
    },
    Rpad {
        expr: ExprNode,
        size: ExprNode,
        fill: Option<ExprNode>,
    },
    Degrees(ExprNode),
    Radians(ExprNode),
    Concat(ExprList),
    Substr {
        expr: ExprNode,
        start: ExprNode,
        count: Option<ExprNode>,
    },
    Ltrim {
        expr: ExprNode,
        chars: Option<ExprNode>,
    },
    Rtrim {
        expr: ExprNode,
        chars: Option<ExprNode>,
    },
}

impl TryFrom<FunctionNode> for Function {
    type Error = Error;

    fn try_from(func_node: FunctionNode) -> Result<Self> {
        match func_node {
            FunctionNode::Abs(expr_node) => expr_node.try_into().map(Function::Abs),
            FunctionNode::Upper(expr_node) => expr_node.try_into().map(Function::Upper),
            FunctionNode::IfNull(expr_node, then_node) => expr_node.try_into().and_then(|expr| {
                then_node
                    .try_into()
                    .map(|then| Function::IfNull { expr, then })
            }),
            FunctionNode::Ceil(expr_node) => expr_node.try_into().map(Function::Ceil),
            FunctionNode::Round(expr_node) => expr_node.try_into().map(Function::Round),
            FunctionNode::Floor(expr_node) => expr_node.try_into().map(Function::Floor),
            FunctionNode::Asin(expr_node) => expr_node.try_into().map(Function::Asin),
            FunctionNode::Acos(expr_node) => expr_node.try_into().map(Function::Acos),
            FunctionNode::Atan(expr_node) => expr_node.try_into().map(Function::Atan),
            FunctionNode::Sin(expr_node) => expr_node.try_into().map(Function::Sin),
            FunctionNode::Cos(expr_node) => expr_node.try_into().map(Function::Cos),
            FunctionNode::Tan(expr_node) => expr_node.try_into().map(Function::Tan),
            FunctionNode::Pi => Ok(Function::Pi()),
            FunctionNode::Now => Ok(Function::Now()),
            FunctionNode::Left(expr_node, size_node) => expr_node.try_into().and_then(|expr| {
                size_node
                    .try_into()
                    .map(|size| Function::Left { expr, size })
            }),
            FunctionNode::Log(antilog_node, base_node) => {
                antilog_node.try_into().and_then(|antilog| {
                    base_node
                        .try_into()
                        .map(|base| Function::Log { antilog, base })
                })
            }
            FunctionNode::Log2(expr_node) => expr_node.try_into().map(Function::Log2),
            FunctionNode::Log10(expr_node) => expr_node.try_into().map(Function::Log10),
            FunctionNode::Ln(expr_node) => expr_node.try_into().map(Function::Ln),
            FunctionNode::Right(expr_node, size_node) => expr_node.try_into().and_then(|expr| {
                size_node
                    .try_into()
                    .map(|size| Function::Right { expr, size })
            }),
            FunctionNode::Reverse(expr_node) => expr_node.try_into().map(Function::Reverse),
            FunctionNode::Sign(expr_node) => expr_node.try_into().map(Function::Sign),
            FunctionNode::Power(expr_node, power_node) => expr_node.try_into().and_then(|expr| {
                power_node
                    .try_into()
                    .map(|power| Function::Power { expr, power })
            }),
            FunctionNode::Sqrt(expr_node) => expr_node.try_into().map(Function::Sqrt),
            FunctionNode::Gcd(left_node, right_node) => left_node.try_into().and_then(|left| {
                right_node
                    .try_into()
                    .map(|right| Function::Gcd { left, right })
            }),
            FunctionNode::Lcm(left_node, right_node) => left_node.try_into().and_then(|left| {
                right_node
                    .try_into()
                    .map(|right| Function::Lcm { left, right })
            }),
            FunctionNode::GenerateUuid => Ok(Function::GenerateUuid()),
            FunctionNode::Repeat(expr, num) => expr
                .try_into()
                .and_then(|expr| num.try_into().map(|num| Function::Repeat { expr, num })),
            FunctionNode::Lpad { expr, size, fill } => {
                let fill = fill.map(TryInto::try_into).transpose()?;
                let expr = expr.try_into()?;
                let size = size.try_into()?;
                Ok(Function::Lpad { expr, size, fill })
            }
            FunctionNode::Rpad { expr, size, fill } => {
                let fill = fill.map(TryInto::try_into).transpose()?;
                let expr = expr.try_into()?;
                let size = size.try_into()?;
                Ok(Function::Rpad { expr, size, fill })
            }
            FunctionNode::Concat(expr_list) => expr_list.try_into().map(Function::Concat),
            FunctionNode::Degrees(expr) => expr.try_into().map(Function::Degrees),
            FunctionNode::Radians(expr) => expr.try_into().map(Function::Radians),
            FunctionNode::Exp(expr) => expr.try_into().map(Function::Exp),
            FunctionNode::Substr { expr, start, count } => {
                let count = count.map(TryInto::try_into).transpose()?;
                let expr = expr.try_into()?;
                let start = start.try_into()?;
                Ok(Function::Substr { expr, start, count })
            }
            FunctionNode::Ltrim { expr, chars } => {
                let chars = chars.map(TryInto::try_into).transpose()?;
                let expr = expr.try_into()?;
                Ok(Function::Ltrim { expr, chars })
            }
            FunctionNode::Rtrim { expr, chars } => {
                let chars = chars.map(TryInto::try_into).transpose()?;
                let expr = expr.try_into()?;
                Ok(Function::Rtrim { expr, chars })
            }
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
    pub fn log(self, base: ExprNode) -> ExprNode {
        log(self, base)
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

    pub fn sign(self) -> ExprNode {
        sign(self)
    }

    pub fn power(self, pwr: ExprNode) -> ExprNode {
        power(self, pwr)
    }

    pub fn sqrt(self) -> ExprNode {
        sqrt(self)
    }
    pub fn gcd(self, right: ExprNode) -> ExprNode {
        gcd(self, right)
    }
    pub fn lcm(self, right: ExprNode) -> ExprNode {
        lcm(self, right)
    }
    pub fn repeat(self, num: ExprNode) -> ExprNode {
        repeat(self, num)
    }
    pub fn degrees(self) -> ExprNode {
        degrees(self)
    }
    pub fn radians(self) -> ExprNode {
        radians(self)
    }
    pub fn lpad(self, size: ExprNode, fill: Option<ExprNode>) -> ExprNode {
        lpad(self, size, fill)
    }
    pub fn rpad(self, size: ExprNode, fill: Option<ExprNode>) -> ExprNode {
        rpad(self, size, fill)
    }
    pub fn exp(self) -> ExprNode {
        exp(self)
    }
    pub fn substr(self, start: ExprNode, count: Option<ExprNode>) -> ExprNode {
        substr(self, start, count)
    }
    pub fn rtrim(self, chars: Option<ExprNode>) -> ExprNode {
        rtrim(self, chars)
    }
    pub fn ltrim(self, chars: Option<ExprNode>) -> ExprNode {
        ltrim(self, chars)
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
pub fn concat<T: Into<ExprList>>(expr: T) -> ExprNode {
    ExprNode::Function(Box::new(FunctionNode::Concat(expr.into())))
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
pub fn generate_uuid() -> ExprNode {
    ExprNode::Function(Box::new(FunctionNode::GenerateUuid))
}
pub fn now() -> ExprNode {
    ExprNode::Function(Box::new(FunctionNode::Now))
}
pub fn left<T: Into<ExprNode>, V: Into<ExprNode>>(expr: T, size: V) -> ExprNode {
    ExprNode::Function(Box::new(FunctionNode::Left(expr.into(), size.into())))
}
pub fn log<V: Into<ExprNode>>(expr: V, base: V) -> ExprNode {
    ExprNode::Function(Box::new(FunctionNode::Log(expr.into(), base.into())))
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

pub fn sign<T: Into<ExprNode>>(expr: T) -> ExprNode {
    ExprNode::Function(Box::new(FunctionNode::Sign(expr.into())))
}

pub fn power<V: Into<ExprNode>>(expr: V, power: V) -> ExprNode {
    ExprNode::Function(Box::new(FunctionNode::Power(expr.into(), power.into())))
}

pub fn sqrt<V: Into<ExprNode>>(expr: V) -> ExprNode {
    ExprNode::Function(Box::new(FunctionNode::Sqrt(expr.into())))
}

pub fn gcd<V: Into<ExprNode>>(left: V, right: V) -> ExprNode {
    ExprNode::Function(Box::new(FunctionNode::Gcd(left.into(), right.into())))
}

pub fn lcm<V: Into<ExprNode>>(left: V, right: V) -> ExprNode {
    ExprNode::Function(Box::new(FunctionNode::Lcm(left.into(), right.into())))
}

pub fn repeat<V: Into<ExprNode>>(expr: V, num: V) -> ExprNode {
    ExprNode::Function(Box::new(FunctionNode::Repeat(expr.into(), num.into())))
}

pub fn lpad<V: Into<ExprNode>>(expr: V, size: V, fill: Option<V>) -> ExprNode {
    ExprNode::Function(Box::new(FunctionNode::Lpad {
        expr: expr.into(),
        size: size.into(),
        fill: fill.map(|v| v.into()),
    }))
}

pub fn rpad<V: Into<ExprNode>>(expr: V, size: V, fill: Option<V>) -> ExprNode {
    ExprNode::Function(Box::new(FunctionNode::Rpad {
        expr: expr.into(),
        size: size.into(),
        fill: fill.map(|v| v.into()),
    }))
}

pub fn degrees<V: Into<ExprNode>>(expr: V) -> ExprNode {
    ExprNode::Function(Box::new(FunctionNode::Degrees(expr.into())))
}

pub fn radians<V: Into<ExprNode>>(expr: V) -> ExprNode {
    ExprNode::Function(Box::new(FunctionNode::Radians(expr.into())))
}

pub fn exp<V: Into<ExprNode>>(expr: V) -> ExprNode {
    ExprNode::Function(Box::new(FunctionNode::Exp(expr.into())))
}
pub fn substr<V: Into<ExprNode>>(expr: V, start: V, count: Option<V>) -> ExprNode {
    ExprNode::Function(Box::new(FunctionNode::Substr {
        expr: expr.into(),
        start: start.into(),
        count: count.map(|v| v.into()),
    }))
}

pub fn ltrim<T: Into<ExprNode>>(expr: T, chars: Option<T>) -> ExprNode {
    ExprNode::Function(Box::new(FunctionNode::Ltrim {
        expr: expr.into(),
        chars: chars.map(|t| t.into()),
    }))
}

pub fn rtrim<T: Into<ExprNode>>(expr: T, chars: Option<T>) -> ExprNode {
    ExprNode::Function(Box::new(FunctionNode::Rtrim {
        expr: expr.into(),
        chars: chars.map(|t| t.into()),
    }))
}

#[cfg(test)]
mod tests {
    use crate::ast_builder::{
        abs, acos, asin, atan, ceil, col, concat, cos, degrees, exp, expr, floor, gcd,
        generate_uuid, ifnull, lcm, left, ln, log, log10, log2, lpad, ltrim, now, num, pi, power,
        radians, repeat, reverse, right, round, rpad, rtrim, sign, sin, sqrt, substr, tan,
        test_expr, text, upper,
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
    fn function_generate_uuid() {
        let actual = generate_uuid();
        let expected = "GENERATE_UUID()";
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
    fn function_log() {
        let actual = log(num(64), num(8));
        let expected = "log(64,8)";
        test_expr(actual, expected);

        let actual = num(64).log(num(8));
        let expected = "LOG(64,8)";
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

    #[test]
    fn function_sign() {
        let actual = sign(col("id"));
        let expected = "SIGN(id)";
        test_expr(actual, expected);

        let actual = expr("id").sign();
        let expected = "SIGN(id)";
        test_expr(actual, expected);
    }
    #[test]
    fn function_power() {
        let actual = power(num(2), num(4));
        let expected = "POWER(2,4)";
        test_expr(actual, expected);

        let actual = num(2).power(num(4));
        let expected = "POWER(2,4)";
        test_expr(actual, expected);
    }

    #[test]
    fn function_sqrt() {
        let actual = sqrt(num(9));
        let expected = "SQRT(9)";
        test_expr(actual, expected);

        let actual = num(9).sqrt();
        let expected = "SQRT(9)";
        test_expr(actual, expected);
    }

    #[test]
    fn function_gcd() {
        let actual = gcd(num(64), num(8));
        let expected = "gcd(64,8)";
        test_expr(actual, expected);

        let actual = num(64).gcd(num(8));
        let expected = "GCD(64,8)";
        test_expr(actual, expected);
    }

    #[test]
    fn function_lcm() {
        let actual = lcm(num(64), num(8));
        let expected = "lcm(64,8)";
        test_expr(actual, expected);

        let actual = num(64).lcm(num(8));
        let expected = "LCM(64,8)";
        test_expr(actual, expected);
    }

    #[test]
    fn function_repeat() {
        let actual = repeat(text("GlueSQL"), num(2));
        let expected = "REPEAT('GlueSQL', 2)";
        test_expr(actual, expected);

        let actual = text("GlueSQL").repeat(num(2));
        let expected = "REPEAT('GlueSQL', 2)";
        test_expr(actual, expected);
    }

    #[test]
    fn function_degrees() {
        let actual = degrees(num(1));
        let expected = "DEGREES(1)";
        test_expr(actual, expected);

        let actual = num(1).degrees();
        let expected = "DEGREES(1)";
        test_expr(actual, expected);
    }

    #[test]
    fn function_radians() {
        let actual = radians(num(1));
        let expected = "RADIANS(1)";
        test_expr(actual, expected);

        let actual = num(1).radians();
        let expected = "RADIANS(1)";
        test_expr(actual, expected);
    }

    #[test]
    fn function_concat() {
        let actual = concat(vec![text("Glue"), text("SQL"), text("Go")]);
        let expected = "CONCAT('Glue','SQL','Go')";
        test_expr(actual, expected);

        let actual = concat(vec!["Glue", "SQL", "Go"]);
        let expected = "CONCAT(Glue, SQL, Go)";
        test_expr(actual, expected);
    }

    #[test]
    fn function_lpad() {
        let actual = lpad(text("GlueSQL"), num(10), Some(text("Go")));
        let expected = "LPAD('GlueSQL', 10, 'Go')";
        test_expr(actual, expected);

        let actual = lpad(text("GlueSQL"), num(10), None);
        let expected = "LPAD('GlueSQL', 10)";
        test_expr(actual, expected);

        let actual = text("GlueSQL").lpad(num(10), Some(text("Go")));
        let expected = "LPAD('GlueSQL', 10, 'Go')";
        test_expr(actual, expected);

        let actual = text("GlueSQL").lpad(num(10), None);
        let expected = "LPAD('GlueSQL', 10)";
        test_expr(actual, expected);
    }

    #[test]
    fn function_rpad() {
        let actual = rpad(text("GlueSQL"), num(10), Some(text("Go")));
        let expected = "RPAD('GlueSQL', 10, 'Go')";
        test_expr(actual, expected);

        let actual = rpad(text("GlueSQL"), num(10), None);
        let expected = "RPAD('GlueSQL', 10)";
        test_expr(actual, expected);

        let actual = text("GlueSQL").rpad(num(10), Some(text("Go")));
        let expected = "RPAD('GlueSQL', 10, 'Go')";
        test_expr(actual, expected);

        let actual = text("GlueSQL").rpad(num(10), None);
        let expected = "RPAD('GlueSQL', 10)";
        test_expr(actual, expected);
    }

    #[test]
    fn function_exp() {
        let actual = exp(num(2));
        let expected = "EXP(2)";
        test_expr(actual, expected);

        let actual = num(2).exp();
        let expected = "EXP(2)";
        test_expr(actual, expected);
    }

    #[test]
    fn function_substr() {
        let actual = substr(text("GlueSQL"), num(2), Some(num(4)));
        let expected = "SUBSTR('GlueSQL', 2, 4)";
        test_expr(actual, expected);

        let actual = substr(text("GlueSQL"), num(2), None);
        let expected = "SUBSTR('GlueSQL', 2)";
        test_expr(actual, expected);

        let actual = text("GlueSQL").substr(num(2), Some(num(4)));
        let expected = "SUBSTR('GlueSQL', 2, 4)";
        test_expr(actual, expected);

        let actual = text("GlueSQL").substr(num(2), None);
        let expected = "SUBSTR('GlueSQL', 2)";
        test_expr(actual, expected);
    }

    #[test]
    fn function_rtrim() {
        let actual = rtrim(text("GlueSQL      "), None);
        let expected = "RTRIM('GlueSQL      ')";
        test_expr(actual, expected);

        let actual = text("GlueSQL      ").rtrim(None);
        let expected = "RTRIM('GlueSQL      ')";
        test_expr(actual, expected);

        let actual = rtrim(text("GlueSQLABC"), Some(text("ABC")));
        let expected = "RTRIM('GlueSQLABC','ABC')";
        test_expr(actual, expected);

        let actual = text("GlueSQLABC").rtrim(Some(text("ABC")));
        let expected = "RTRIM('GlueSQLABC','ABC')";
        test_expr(actual, expected);
    }

    #[test]
    fn function_ltrim() {
        let actual = ltrim(text("      GlueSQL"), None);
        let expected = "LTRIM('      GlueSQL')";
        test_expr(actual, expected);

        let actual = text("      GlueSQL").ltrim(None);
        let expected = "LTRIM('      GlueSQL')";
        test_expr(actual, expected);

        let actual = ltrim(text("ABCGlueSQL"), Some(text("ABC")));
        let expected = "LTRIM('ABCGlueSQL','ABC')";
        test_expr(actual, expected);

        let actual = text("ABCGlueSQL").ltrim(Some(text("ABC")));
        let expected = "LTRIM('ABCGlueSQL','ABC')";
        test_expr(actual, expected);
    }
}
