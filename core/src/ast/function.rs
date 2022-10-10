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
    Format {
        expr: Expr,
        format: Expr,
    },
    ToDate {
        expr: Expr,
        format: Expr,
    },
    ToTimestamp {
        expr: Expr,
        format: Expr,
    },
    ToTime {
        expr: Expr,
        format: Expr,
    },
    Position {
        from_expr: Expr,
        sub_expr: Expr,
    },
}

impl ToSql for Function {
    fn to_sql(&self) -> String {
        match self {
            Function::Abs(e) => format!("ABS({})", e.to_sql()),
            Function::Lower(e) => format!("LOWER({})", e.to_sql()),
            Function::Upper(e) => format!("UPPER({})", e.to_sql()),
            Function::Left { expr, size } => format!("LEFT({}, {})", expr.to_sql(), size.to_sql()),
            Function::Right { expr, size } => {
                format!("RIGHT({}, {})", expr.to_sql(), size.to_sql())
            }
            Function::Asin(e) => format!("ASIN({})", e.to_sql()),
            Function::Acos(e) => format!("ACOS({})", e.to_sql()),
            Function::Atan(e) => format!("ATAN({})", e.to_sql()),
            Function::Lpad { expr, size, fill } => match fill {
                None => format!("LPAD({}, {})", expr.to_sql(), size.to_sql()),
                Some(fill) => format!(
                    "LPAD({}, {}, {})",
                    expr.to_sql(),
                    size.to_sql(),
                    fill.to_sql()
                ),
            },
            Function::Rpad { expr, size, fill } => match fill {
                None => format!("RPAD({}, {})", expr.to_sql(), size.to_sql()),
                Some(fill) => format!(
                    "RPAD({}, {}, {})",
                    expr.to_sql(),
                    size.to_sql(),
                    fill.to_sql()
                ),
            },
            Function::Ceil(e) => format!("CEIL({})", e.to_sql()),
            Function::Concat(items) => {
                let items = items
                    .iter()
                    .map(ToSql::to_sql)
                    .collect::<Vec<_>>()
                    .join(", ");
                format!("CONCAT({items})")
            }
            Function::IfNull { expr, then } => {
                format!("IFNULL({}, {})", expr.to_sql(), then.to_sql())
            }
            Function::Round(e) => format!("ROUND({})", e.to_sql()),
            Function::Floor(e) => format!("FLOOR({})", e.to_sql()),
            Function::Trim {
                expr,
                filter_chars,
                trim_where_field,
            } => match filter_chars {
                None => format!("TRIM({})", expr.to_sql()),
                Some(filter_chars) => match trim_where_field {
                    None => format!("TRIM({} FROM {})", filter_chars.to_sql(), expr.to_sql()),
                    Some(trim_where_field) => format!(
                        "TRIM({} {} FROM {})",
                        trim_where_field.to_sql(),
                        filter_chars.to_sql(),
                        expr.to_sql()
                    ),
                },
            },
            Function::Exp(e) => format!("EXP({})", e.to_sql()),
            Function::Ln(e) => format!("LN({})", e.to_sql()),
            Function::Log { antilog, base } => {
                format!("LOG({}, {})", antilog.to_sql(), base.to_sql())
            }
            Function::Log2(e) => format!("LOG2({})", e.to_sql()),
            Function::Log10(e) => format!("LOG10({})", e.to_sql()),
            Function::Div { dividend, divisor } => {
                format!("DIV({}, {})", dividend.to_sql(), divisor.to_sql())
            }
            Function::Mod { dividend, divisor } => {
                format!("MOD({}, {})", dividend.to_sql(), divisor.to_sql())
            }
            Function::Gcd { left, right } => format!("GCD({}, {})", left.to_sql(), right.to_sql()),
            Function::Lcm { left, right } => format!("LCM({}, {})", left.to_sql(), right.to_sql()),
            Function::Sin(e) => format!("SIN({})", e.to_sql()),
            Function::Cos(e) => format!("COS({})", e.to_sql()),
            Function::Tan(e) => format!("TAN({})", e.to_sql()),
            Function::Sqrt(e) => format!("SQRT({})", e.to_sql()),
            Function::Power { expr, power } => {
                format!("POWER({}, {})", expr.to_sql(), power.to_sql())
            }
            Function::Radians(e) => format!("RADIANS({})", e.to_sql()),
            Function::Degrees(e) => format!("DEGREES({})", e.to_sql()),
            Function::Now() => "NOW()".to_owned(),
            Function::Pi() => "PI()".to_owned(),
            Function::Ltrim { expr, chars } => match chars {
                None => format!("LTRIM({})", expr.to_sql()),
                Some(chars) => format!("LTRIM({}, {})", expr.to_sql(), chars.to_sql()),
            },
            Function::Rtrim { expr, chars } => match chars {
                None => format!("RTRIM({})", expr.to_sql()),
                Some(chars) => format!("RTRIM({}, {})", expr.to_sql(), chars.to_sql()),
            },
            Function::Reverse(e) => format!("REVERSE({})", e.to_sql()),
            Function::Repeat { expr, num } => {
                format!("REPEAT({}, {})", expr.to_sql(), num.to_sql())
            }
            Function::Sign(e) => format!("SIGN({})", e.to_sql()),
            Function::Substr { expr, start, count } => match count {
                None => format!("SUBSTR({}, {})", expr.to_sql(), start.to_sql()),
                Some(count) => format!(
                    "SUBSTR({}, {}, {})",
                    expr.to_sql(),
                    start.to_sql(),
                    count.to_sql()
                ),
            },
            Function::Unwrap { expr, selector } => {
                format!("UNWRAP({}, {})", expr.to_sql(), selector.to_sql())
            }
            Function::GenerateUuid() => "GENERATE_UUID()".to_owned(),
            Function::Format { expr, format } => {
                format!("FORMAT({}, {})", expr.to_sql(), format.to_sql())
            }
            Function::ToDate { expr, format } => {
                format!("TO_TIME({}, {})", expr.to_sql(), format.to_sql())
            }
            Function::ToTimestamp { expr, format } => {
                format!("TO_TIMESTAMP({}, {})", expr.to_sql(), format.to_sql())
            }
            Function::ToTime { expr, format } => {
                format!("TO_TIME({}, {})", expr.to_sql(), format.to_sql())
            }
            Function::Position {
                from_expr,
                sub_expr,
            } => format!("POSITION({}, IN {})", sub_expr.to_sql(), from_expr.to_sql()),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Aggregate {
    Count(CountArgExpr),
    Sum(Expr),
    Max(Expr),
    Min(Expr),
    Avg(Expr),
    Variance(Expr),
    Stdev(Expr),
}

impl ToSql for Aggregate {
    fn to_sql(&self) -> String {
        match self {
            Aggregate::Count(cae) => format!("COUNT({})", cae.to_sql()),
            Aggregate::Sum(e) => format!("SUM({})", e.to_sql()),
            Aggregate::Max(e) => format!("MAX({})", e.to_sql()),
            Aggregate::Min(e) => format!("MIN({})", e.to_sql()),
            Aggregate::Avg(e) => format!("AVG({})", e.to_sql()),
            Aggregate::Variance(e) => format!("VARIANCE({})", e.to_sql()),
            Aggregate::Stdev(e) => format!("STDEV({})", e.to_sql()),
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
            CountArgExpr::Wildcard => "*".to_owned(),
        }
    }
}
