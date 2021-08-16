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
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Aggregate {
    Count(Expr),
    Sum(Expr),
    Max(Expr),
    Min(Expr),
}
