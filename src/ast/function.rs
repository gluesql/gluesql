use {
    super::Expr,
    serde::{Deserialize, Serialize},
};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Function {
    Lower(Expr),
    Upper(Expr),
    Left { expr: Expr, size: Expr },
    Right { expr: Expr, size: Expr },
    Ceil(Expr),
    Round(Expr),
    Floor(Expr),
    Trim(Expr),
    Div { dividend: Expr, divisor: Expr },
    Mod { dividend: Expr, divisor: Expr },
    Gcd { left: Expr, right: Expr },
    Lcm { left: Expr, right: Expr },
    Sin(Expr),
    Cos(Expr),
    Tan(Expr),
}

impl Function {
    pub fn name(&self) -> &str {
        match self {
            Function::Lower(_) => "LOWER",
            Function::Upper(_) => "UPPER",
            Function::Left { .. } => "LEFT",
            Function::Right { .. } => "RIGHT",
            Function::Sin(_) => "SIN",
            Function::Cos(_) => "COS",
            Function::Tan(_) => "TAN",
            Function::Ceil(_) => "CEIL",
            Function::Round(_) => "ROUND",
            Function::Floor(_) => "FLOOR",
            Function::Trim(_) => "TRIM",
            Function::Div { .. } => "DIV",
            Function::Mod { .. } => "MOD",
            Function::Gcd { .. } => "GCD",
            Function::Lcm { .. } => "LCM",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Aggregate {
    Count(Expr),
    Sum(Expr),
    Max(Expr),
    Min(Expr),
}
