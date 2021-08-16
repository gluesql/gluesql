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
    Sin(Expr),
    Cos(Expr),
    Tan(Expr),
}

impl Function {
    pub fn name(&self) -> &str {
        match self {
            Function::Lower(_) => "LOWER",
            Function::Upper(_) => "UPPER",
            Function::Left { expr: _expr, size: _size } => "LEFT",
            Function::Right{ expr: _expr, size: _size }  => "RIGHT",
            Function::Sin(_)=> "SIN",
            Function::Cos(_)=> "COS",
            Function::Tan(_)=> "TAN"
        }
    }

    pub fn trigonometric(&self, value : f64) -> Option<f64> {
        match self {
            Function::Sin(_) => Some(value.sin()),
            Function::Cos(_) => Some(value.cos()),
            Function::Tan(_) => Some(value.tan()),
            _ => None
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
