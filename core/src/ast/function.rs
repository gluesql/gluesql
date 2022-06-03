use {
    super::{ast_literal::TrimWhereField, Expr},
    crate::ast::ToSql,
    serde::{Deserialize, Serialize},
    strum_macros::Display,
};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, Display)]
#[strum(serialize_all = "SCREAMING_SNAKE_CASE")]
pub enum Function {
    Abs(Expr),
    Lower(Expr),
    Upper(Expr),
    Left {
        expr: Expr,
        size: Expr,
    },
    Right {
        expr: Expr,
        size: Expr,
    },
    Asin(Expr),
    Acos(Expr),
    Atan(Expr),
    Lpad {
        expr: Expr,
        size: Expr,
        fill: Option<Expr>,
    },
    Rpad {
        expr: Expr,
        size: Expr,
        fill: Option<Expr>,
    },
    Ceil(Expr),
    Concat(Vec<Expr>),
    IfNull {
        expr: Expr,
        then: Expr,
    },
    Round(Expr),
    Floor(Expr),
    Trim {
        expr: Expr,
        filter_chars: Option<Expr>,
        trim_where_field: Option<TrimWhereField>,
    },
    Exp(Expr),
    Ln(Expr),
    Log {
        antilog: Expr,
        base: Expr,
    },
    Log2(Expr),
    Log10(Expr),
    Div {
        dividend: Expr,
        divisor: Expr,
    },
    Mod {
        dividend: Expr,
        divisor: Expr,
    },
    Gcd {
        left: Expr,
        right: Expr,
    },
    Lcm {
        left: Expr,
        right: Expr,
    },
    Sin(Expr),
    Cos(Expr),
    Tan(Expr),
    Sqrt(Expr),
    Power {
        expr: Expr,
        power: Expr,
    },
    Radians(Expr),
    Degrees(Expr),
    Now(),
    Pi(),
    Ltrim {
        expr: Expr,
        chars: Option<Expr>,
    },
    Rtrim {
        expr: Expr,
        chars: Option<Expr>,
    },
    Reverse(Expr),
    Repeat {
        expr: Expr,
        num: Expr,
    },
    Sign(Expr),
    Substr {
        expr: Expr,
        start: Expr,
        count: Option<Expr>,
    },
    Unwrap {
        expr: Expr,
        selector: Expr,
    },
    GenerateUuid(),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Aggregate {
    Count(CountArgExpr),
    Sum(Expr),
    Max(Expr),
    Min(Expr),
    Avg(Expr),
}

impl ToSql for Aggregate {
    fn to_sql(&self) -> String {
        match self {
            Aggregate::Count(cae) => format!("Count({})", cae.to_sql()),
            Aggregate::Sum(e) => format!("Sum({})", e.to_sql()),
            Aggregate::Max(e) => format!("Max({})", e.to_sql()),
            Aggregate::Min(e) => format!("Min({})", e.to_sql()),
            Aggregate::Avg(e) => format!("Avg({})", e.to_sql()),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum CountArgExpr {
    Expr(Expr),
    Wildcard,
}

impl ToSql for CountArgExpr {
    fn to_sql(&self) -> String {
        match self {
            CountArgExpr::Expr(e) => e.to_sql(),
            CountArgExpr::Wildcard => "*".to_string(),
        }
    }
}
