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
    Exp(Expr),
    Ln(Expr),
    Log2(Expr),
    Log10(Expr),
    Div { dividend: Expr, divisor: Expr },
    Mod { dividend: Expr, divisor: Expr },
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Aggregate {
    Count(Expr),
    Sum(Expr),
    Max(Expr),
    Min(Expr),
}
