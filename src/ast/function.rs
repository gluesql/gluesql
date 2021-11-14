use {
    super::{ast_literal::TrimWhereField, Expr},
    serde::{Deserialize, Serialize},
    strum_macros::Display,
};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, Display)]
pub enum Function {
    #[strum(to_string = "LOWER")]
    Lower(Expr),
    #[strum(to_string = "UPPER")]
    Upper(Expr),
    #[strum(to_string = "LEFT")]
    Left { expr: Expr, size: Expr },
    #[strum(to_string = "RIGHT")]
    Right { expr: Expr, size: Expr },
    #[strum(to_string = "ASIN")]
    ASin(Expr),
    #[strum(to_string = "ACOS")]
    ACos(Expr),
    #[strum(to_string = "ATAN")]
    ATan(Expr),
    #[strum(to_string = "LPAD")]
    Lpad {
        expr: Expr,
        size: Expr,
        fill: Option<Expr>,
    },
    #[strum(to_string = "RPAD")]
    Rpad {
        expr: Expr,
        size: Expr,
        fill: Option<Expr>,
    },
    #[strum(to_string = "CEIL")]
    Ceil(Expr),
    #[strum(to_string = "ROUND")]
    Round(Expr),
    #[strum(to_string = "FLOOR")]
    Floor(Expr),
    #[strum(to_string = "TRIM")]
    Trim {
        expr: Expr,
        filter_chars: Option<Expr>,
        trim_where_field: Option<TrimWhereField>,
    },
    #[strum(to_string = "EXP")]
    Exp(Expr),
    #[strum(to_string = "LN")]
    Ln(Expr),
    #[strum(to_string = "LOG")]
    Log { antilog: Expr, base: Expr },
    #[strum(to_string = "LOG2")]
    Log2(Expr),
    #[strum(to_string = "LOG10")]
    Log10(Expr),
    #[strum(to_string = "DIV")]
    Div { dividend: Expr, divisor: Expr },
    #[strum(to_string = "MOD")]
    Mod { dividend: Expr, divisor: Expr },
    #[strum(to_string = "GCD")]
    Gcd { left: Expr, right: Expr },
    #[strum(to_string = "LCM")]
    Lcm { left: Expr, right: Expr },
    #[strum(to_string = "SIN")]
    Sin(Expr),
    #[strum(to_string = "COS")]
    Cos(Expr),
    #[strum(to_string = "TAN")]
    Tan(Expr),
    #[strum(to_string = "SQRT")]
    Sqrt(Expr),
    #[strum(to_string = "POWER")]
    Power { expr: Expr, power: Expr },
    #[strum(to_string = "RADIANS")]
    Radians(Expr),
    #[strum(to_string = "DEGREES")]
    Degrees(Expr),
    #[strum(to_string = "PI")]
    Pi(),
    #[strum(to_string = "LTRIM")]
    Ltrim { expr: Expr, chars: Option<Expr> },
    #[strum(to_string = "RTRIM")]
    Rtrim { expr: Expr, chars: Option<Expr> },
    #[strum(to_string = "REVERSE")]
    Reverse(Expr),
    #[strum(to_string = "REPEAT")]
    Repeat { expr: Expr, num: Expr },
    #[strum(to_string = "SUBSTR")]
    Substr {
        expr: Expr,
        start: Expr,
        count: Option<Expr>,
    },
    #[strum(to_string = "UNWRAP")]
    Unwrap { expr: Expr, selector: Expr },
    #[strum(to_string = "GENERATE_UUID")]
    GenerateUuid(),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Aggregate {
    Count(Expr),
    Sum(Expr),
    Max(Expr),
    Min(Expr),
    Avg(Expr),
}
