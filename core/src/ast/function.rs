use {
    super::{ast_literal::TrimWhereField, DataType, DateTimeField, Expr},
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
    Cast {
        expr: Expr,
        data_type: DataType,
    },
    Ceil(Expr),
    Concat(Vec<Expr>),
    ConcatWs {
        separator: Expr,
        exprs: Vec<Expr>,
    },
    #[cfg(feature = "function")]
    Custom {
        name: String,
        exprs: Vec<Expr>,
    },
    IfNull {
        expr: Expr,
        then: Expr,
    },
    Rand(Option<Expr>),
    Round(Expr),
    Floor(Expr),
    Trim {
        expr: Expr,
        filter_chars: Option<Expr>,
        trim_where_field: Option<TrimWhereField>,
    },
    Exp(Expr),
    Extract {
        field: DateTimeField,
        expr: Expr,
    },
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
    Ascii(Expr),
    Chr(Expr),
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
            Function::Cast { expr, data_type } => {
                format!("CAST({} AS {data_type})", expr.to_sql())
            }
            Function::Ceil(e) => format!("CEIL({})", e.to_sql()),
            Function::Concat(items) => {
                let items = items
                    .iter()
                    .map(ToSql::to_sql)
                    .collect::<Vec<_>>()
                    .join(", ");
                format!("CONCAT({items})")
            }
            #[cfg(feature = "function")]
            Function::Custom { name, exprs } => {
                let exprs = exprs
                    .iter()
                    .map(ToSql::to_sql)
                    .collect::<Vec<_>>()
                    .join(", ");
                format!("{name}({exprs})")
            }
            Function::ConcatWs { separator, exprs } => {
                let exprs = exprs
                    .iter()
                    .map(ToSql::to_sql)
                    .collect::<Vec<_>>()
                    .join(", ");
                format!("CONCAT_WS({}, {})", separator.to_sql(), exprs)
            }
            Function::IfNull { expr, then } => {
                format!("IFNULL({}, {})", expr.to_sql(), then.to_sql())
            }
            Function::Rand(e) => match e {
                Some(v) => format!("RAND({})", v.to_sql()),
                None => "RAND()".to_owned(),
            },
            Function::Round(e) => format!("ROUND({})", e.to_sql()),
            Function::Floor(e) => format!("FLOOR({})", e.to_sql()),
            Function::Trim {
                expr,
                filter_chars,
                trim_where_field,
            } => {
                let trim_where_field = match trim_where_field {
                    None => "".to_owned(),
                    Some(t) => format!("{t} "),
                };

                match filter_chars {
                    None => format!("TRIM({}{})", trim_where_field, expr.to_sql()),
                    Some(filter_chars) => format!(
                        "TRIM({}{} FROM {})",
                        trim_where_field,
                        filter_chars.to_sql(),
                        expr.to_sql()
                    ),
                }
            }
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
                format!("TO_DATE({}, {})", expr.to_sql(), format.to_sql())
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
            } => format!("POSITION({} IN {})", sub_expr.to_sql(), from_expr.to_sql()),
            Function::Extract { field, expr } => {
                format!("EXTRACT({field} FROM '{}')", expr.to_sql())
            }
            Function::Ascii(e) => format!("ASCII({})", e.to_sql()),
            Function::Chr(e) => format!("CHR({})", e.to_sql()),
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

#[cfg(test)]
mod tests {
    use {
        crate::ast::{
            Aggregate, AstLiteral, CountArgExpr, DataType, DateTimeField, Expr, Function, ToSql,
            TrimWhereField,
        },
        bigdecimal::BigDecimal,
        std::str::FromStr,
    };

    #[test]
    fn to_sql_function() {
        assert_eq!(
            "ABS(num)",
            &Expr::Function(Box::new(Function::Abs(Expr::Identifier("num".to_owned())))).to_sql()
        );

        assert_eq!(
            "LOWER('Bye')",
            &Expr::Function(Box::new(Function::Lower(Expr::Literal(
                AstLiteral::QuotedString("Bye".to_owned())
            ))))
            .to_sql()
        );

        assert_eq!(
            "UPPER('Hi')",
            &Expr::Function(Box::new(Function::Upper(Expr::Literal(
                AstLiteral::QuotedString("Hi".to_owned())
            ))))
            .to_sql()
        );

        assert_eq!(
            "LEFT('GlueSQL', 2)",
            &Expr::Function(Box::new(Function::Left {
                expr: Expr::Literal(AstLiteral::QuotedString("GlueSQL".to_owned())),
                size: Expr::Literal(AstLiteral::Number(BigDecimal::from_str("2").unwrap()))
            }))
            .to_sql()
        );

        assert_eq!(
            "RIGHT('GlueSQL', 3)",
            &Expr::Function(Box::new(Function::Right {
                expr: Expr::Literal(AstLiteral::QuotedString("GlueSQL".to_owned())),
                size: Expr::Literal(AstLiteral::Number(BigDecimal::from_str("3").unwrap()))
            }))
            .to_sql()
        );

        assert_eq!(
            r#"ASIN(2)"#,
            &Expr::Function(Box::new(Function::Asin(Expr::Literal(AstLiteral::Number(
                BigDecimal::from_str("2").unwrap()
            )))))
            .to_sql()
        );

        assert_eq!(
            r#"ACOS(2)"#,
            &Expr::Function(Box::new(Function::Acos(Expr::Literal(AstLiteral::Number(
                BigDecimal::from_str("2").unwrap()
            )))))
            .to_sql()
        );

        assert_eq!(
            r#"ATAN(2)"#,
            &Expr::Function(Box::new(Function::Atan(Expr::Literal(AstLiteral::Number(
                BigDecimal::from_str("2").unwrap()
            )))))
            .to_sql()
        );

        assert_eq!(
            "LPAD('GlueSQL', 2)",
            &Expr::Function(Box::new(Function::Lpad {
                expr: Expr::Literal(AstLiteral::QuotedString("GlueSQL".to_owned())),
                size: Expr::Literal(AstLiteral::Number(BigDecimal::from_str("2").unwrap())),
                fill: None
            }))
            .to_sql()
        );

        assert_eq!(
            "LPAD('GlueSQL', 10, 'Go')",
            &Expr::Function(Box::new(Function::Lpad {
                expr: Expr::Literal(AstLiteral::QuotedString("GlueSQL".to_owned())),
                size: Expr::Literal(AstLiteral::Number(BigDecimal::from_str("10").unwrap())),
                fill: Some(Expr::Literal(AstLiteral::QuotedString("Go".to_owned())))
            }))
            .to_sql()
        );

        assert_eq!(
            "RPAD('GlueSQL', 10)",
            &Expr::Function(Box::new(Function::Rpad {
                expr: Expr::Literal(AstLiteral::QuotedString("GlueSQL".to_owned())),
                size: Expr::Literal(AstLiteral::Number(BigDecimal::from_str("10").unwrap())),
                fill: None
            }))
            .to_sql()
        );

        assert_eq!(
            "RPAD('GlueSQL', 10, 'Go')",
            &Expr::Function(Box::new(Function::Rpad {
                expr: Expr::Literal(AstLiteral::QuotedString("GlueSQL".to_owned())),
                size: Expr::Literal(AstLiteral::Number(BigDecimal::from_str("10").unwrap())),
                fill: Some(Expr::Literal(AstLiteral::QuotedString("Go".to_owned())))
            }))
            .to_sql()
        );

        assert_eq!(
            "CAST(1.0 AS INT)",
            &Expr::Function(Box::new(Function::Cast {
                expr: Expr::Literal(AstLiteral::Number(BigDecimal::from_str("1.0").unwrap())),
                data_type: DataType::Int
            }))
            .to_sql()
        );

        assert_eq!(
            "CEIL(num)",
            &Expr::Function(Box::new(Function::Ceil(Expr::Identifier("num".to_owned())))).to_sql()
        );

        #[cfg(feature = "function")]
        assert_eq!(
            "CUSTOM_FUNC(Tic, 1, num, 'abc')",
            &Expr::Function(Box::new(Function::Custom {
                name: "CUSTOM_FUNC".to_owned(),
                exprs: vec![
                    Expr::Identifier("Tic".to_owned()),
                    Expr::Literal(AstLiteral::Number(BigDecimal::from_str("1").unwrap())),
                    Expr::Identifier("num".to_owned()),
                    Expr::Literal(AstLiteral::QuotedString("abc".to_owned()))
                ]
            }))
            .to_sql()
        );
        #[cfg(feature = "function")]
        assert_eq!(
            "CUSTOM_FUNC(num)",
            &Expr::Function(Box::new(Function::Custom {
                name: "CUSTOM_FUNC".to_owned(),
                exprs: vec![Expr::Identifier("num".to_owned())]
            }))
            .to_sql()
        );
        #[cfg(feature = "function")]
        assert_eq!(
            "CUSTOM_FUNC()",
            &Expr::Function(Box::new(Function::Custom {
                name: "CUSTOM_FUNC".to_owned(),
                exprs: vec![]
            }))
            .to_sql()
        );

        assert_eq!(
            "CONCAT(Tic, tac, toe)",
            &Expr::Function(Box::new(Function::Concat(vec![
                Expr::Identifier("Tic".to_owned()),
                Expr::Identifier("tac".to_owned()),
                Expr::Identifier("toe".to_owned())
            ])))
            .to_sql()
        );

        assert_eq!(
            "CONCAT_WS(-, Tic, tac, toe)",
            &Expr::Function(Box::new(Function::ConcatWs {
                separator: Expr::Identifier("-".to_owned()),
                exprs: vec![
                    Expr::Identifier("Tic".to_owned()),
                    Expr::Identifier("tac".to_owned()),
                    Expr::Identifier("toe".to_owned())
                ]
            }))
            .to_sql()
        );

        assert_eq!(
            "IFNULL(updated_at, created_at)",
            &Expr::Function(Box::new(Function::IfNull {
                expr: Expr::Identifier("updated_at".to_owned()),
                then: Expr::Identifier("created_at".to_owned())
            }))
            .to_sql()
        );

        assert_eq!(
            "RAND()",
            &Expr::Function(Box::new(Function::Rand(None))).to_sql()
        );

        assert_eq!(
            "RAND(num)",
            &Expr::Function(Box::new(Function::Rand(Some(Expr::Identifier(
                "num".to_owned()
            )))))
            .to_sql()
        );

        assert_eq!(
            "ROUND(num)",
            &Expr::Function(Box::new(Function::Round(Expr::Identifier(
                "num".to_owned()
            ))))
            .to_sql()
        );

        assert_eq!(
            "FLOOR(num)",
            &Expr::Function(Box::new(Function::Floor(Expr::Identifier(
                "num".to_owned()
            ))))
            .to_sql()
        );

        assert_eq!(
            "TRIM(name)",
            &Expr::Function(Box::new(Function::Trim {
                expr: Expr::Identifier("name".to_owned()),
                filter_chars: None,
                trim_where_field: None
            }))
            .to_sql()
        );

        assert_eq!(
            "TRIM('*' FROM name)",
            &Expr::Function(Box::new(Function::Trim {
                expr: Expr::Identifier("name".to_owned()),
                filter_chars: Some(Expr::Literal(AstLiteral::QuotedString("*".to_owned()))),
                trim_where_field: None
            }))
            .to_sql()
        );

        assert_eq!(
            "TRIM(BOTH '*' FROM name)",
            &Expr::Function(Box::new(Function::Trim {
                expr: Expr::Identifier("name".to_owned()),
                filter_chars: Some(Expr::Literal(AstLiteral::QuotedString("*".to_owned()))),
                trim_where_field: Some(TrimWhereField::Both)
            }))
            .to_sql()
        );

        assert_eq!(
            "TRIM(LEADING '*' FROM name)",
            &Expr::Function(Box::new(Function::Trim {
                expr: Expr::Identifier("name".to_owned()),
                filter_chars: Some(Expr::Literal(AstLiteral::QuotedString("*".to_owned()))),
                trim_where_field: Some(TrimWhereField::Leading)
            }))
            .to_sql()
        );

        assert_eq!(
            r#"TRIM(LEADING name)"#,
            &Expr::Function(Box::new(Function::Trim {
                expr: Expr::Identifier("name".to_owned()),
                filter_chars: None,
                trim_where_field: Some(TrimWhereField::Leading)
            }))
            .to_sql()
        );

        assert_eq!(
            "EXP(1)",
            &Expr::Function(Box::new(Function::Exp(Expr::Literal(AstLiteral::Number(
                BigDecimal::from_str("1").unwrap()
            )))))
            .to_sql()
        );

        assert_eq!(
            "LN(1)",
            &Expr::Function(Box::new(Function::Ln(Expr::Literal(AstLiteral::Number(
                BigDecimal::from_str("1").unwrap()
            )))))
            .to_sql()
        );

        assert_eq!(
            "LOG(64, 8)",
            &Expr::Function(Box::new(Function::Log {
                antilog: Expr::Literal(AstLiteral::Number(BigDecimal::from_str("64").unwrap())),
                base: Expr::Literal(AstLiteral::Number(BigDecimal::from_str("8").unwrap()))
            }))
            .to_sql()
        );

        assert_eq!(
            "LOG2(num)",
            &Expr::Function(Box::new(Function::Log2(Expr::Identifier("num".to_owned())))).to_sql()
        );

        assert_eq!(
            "LOG10(num)",
            &Expr::Function(Box::new(Function::Log10(Expr::Identifier(
                "num".to_owned()
            ))))
            .to_sql()
        );

        assert_eq!(
            "DIV(64, 8)",
            &Expr::Function(Box::new(Function::Div {
                dividend: Expr::Literal(AstLiteral::Number(BigDecimal::from_str("64").unwrap())),
                divisor: Expr::Literal(AstLiteral::Number(BigDecimal::from_str("8").unwrap()))
            }))
            .to_sql()
        );

        assert_eq!(
            "MOD(64, 8)",
            &Expr::Function(Box::new(Function::Mod {
                dividend: Expr::Literal(AstLiteral::Number(BigDecimal::from_str("64").unwrap())),
                divisor: Expr::Literal(AstLiteral::Number(BigDecimal::from_str("8").unwrap()))
            }))
            .to_sql()
        );

        assert_eq!(
            "GCD(64, 8)",
            &Expr::Function(Box::new(Function::Gcd {
                left: Expr::Literal(AstLiteral::Number(BigDecimal::from_str("64").unwrap())),
                right: Expr::Literal(AstLiteral::Number(BigDecimal::from_str("8").unwrap()))
            }))
            .to_sql()
        );

        assert_eq!(
            "LCM(64, 8)",
            &Expr::Function(Box::new(Function::Lcm {
                left: Expr::Literal(AstLiteral::Number(BigDecimal::from_str("64").unwrap())),
                right: Expr::Literal(AstLiteral::Number(BigDecimal::from_str("8").unwrap()))
            }))
            .to_sql()
        );

        assert_eq!(
            "SIN(2)",
            &Expr::Function(Box::new(Function::Sin(Expr::Literal(AstLiteral::Number(
                BigDecimal::from_str("2").unwrap()
            )))))
            .to_sql()
        );

        assert_eq!(
            "COS(2)",
            &Expr::Function(Box::new(Function::Cos(Expr::Literal(AstLiteral::Number(
                BigDecimal::from_str("2").unwrap()
            )))))
            .to_sql()
        );

        assert_eq!(
            "TAN(2)",
            &Expr::Function(Box::new(Function::Tan(Expr::Literal(AstLiteral::Number(
                BigDecimal::from_str("2").unwrap()
            )))))
            .to_sql()
        );

        assert_eq!(
            "SQRT(2)",
            &Expr::Function(Box::new(Function::Sqrt(Expr::Literal(AstLiteral::Number(
                BigDecimal::from_str("2").unwrap()
            )))))
            .to_sql()
        );

        assert_eq!(
            "POWER(2, 10)",
            &Expr::Function(Box::new(Function::Power {
                expr: Expr::Literal(AstLiteral::Number(BigDecimal::from_str("2").unwrap())),
                power: Expr::Literal(AstLiteral::Number(BigDecimal::from_str("10").unwrap())),
            }))
            .to_sql()
        );

        assert_eq!(
            "RADIANS(1)",
            &Expr::Function(Box::new(Function::Radians(Expr::Literal(
                AstLiteral::Number(BigDecimal::from_str("1").unwrap())
            ))))
            .to_sql()
        );

        assert_eq!(
            "DEGREES(1)",
            &Expr::Function(Box::new(Function::Degrees(Expr::Literal(
                AstLiteral::Number(BigDecimal::from_str("1").unwrap())
            ))))
            .to_sql()
        );

        assert_eq!("NOW()", &Expr::Function(Box::new(Function::Now())).to_sql());

        assert_eq!("PI()", &Expr::Function(Box::new(Function::Pi())).to_sql());

        assert_eq!(
            "LTRIM('   HI ')",
            &Expr::Function(Box::new(Function::Ltrim {
                expr: Expr::Literal(AstLiteral::QuotedString("   HI ".to_owned())),
                chars: None
            }))
            .to_sql()
        );

        assert_eq!(
            "LTRIM('*IMPORTANT', '*')",
            &Expr::Function(Box::new(Function::Ltrim {
                expr: Expr::Literal(AstLiteral::QuotedString("*IMPORTANT".to_owned())),
                chars: Some(Expr::Literal(AstLiteral::QuotedString("*".to_owned()))),
            }))
            .to_sql()
        );

        assert_eq!(
            "RTRIM('   HI ')",
            &Expr::Function(Box::new(Function::Rtrim {
                expr: Expr::Literal(AstLiteral::QuotedString("   HI ".to_owned())),
                chars: None
            }))
            .to_sql()
        );

        assert_eq!(
            "RTRIM('IMPORTANT*', '*')",
            &Expr::Function(Box::new(Function::Rtrim {
                expr: Expr::Literal(AstLiteral::QuotedString("IMPORTANT*".to_owned())),
                chars: Some(Expr::Literal(AstLiteral::QuotedString("*".to_owned()))),
            }))
            .to_sql()
        );

        assert_eq!(
            "REVERSE(name)",
            &Expr::Function(Box::new(Function::Reverse(Expr::Identifier(
                "name".to_owned()
            ))))
            .to_sql()
        );

        assert_eq!(
            "REPEAT('Ha', 8)",
            &Expr::Function(Box::new(Function::Repeat {
                expr: Expr::Literal(AstLiteral::QuotedString("Ha".to_owned())),
                num: Expr::Literal(AstLiteral::Number(BigDecimal::from_str("8").unwrap()))
            }))
            .to_sql()
        );

        assert_eq!(
            "SIGN(1.0)",
            &Expr::Function(Box::new(Function::Sign(Expr::Literal(AstLiteral::Number(
                BigDecimal::from_str("1.0").unwrap()
            )))))
            .to_sql()
        );

        assert_eq!(
            "SUBSTR('GlueSQL', 2)",
            &Expr::Function(Box::new(Function::Substr {
                expr: Expr::Literal(AstLiteral::QuotedString("GlueSQL".to_owned())),
                start: Expr::Literal(AstLiteral::Number(BigDecimal::from_str("2").unwrap())),
                count: None
            }))
            .to_sql()
        );

        assert_eq!(
            "SUBSTR('GlueSQL', 1, 3)",
            &Expr::Function(Box::new(Function::Substr {
                expr: Expr::Literal(AstLiteral::QuotedString("GlueSQL".to_owned())),
                start: Expr::Literal(AstLiteral::Number(BigDecimal::from_str("1").unwrap())),
                count: Some(Expr::Literal(AstLiteral::Number(
                    BigDecimal::from_str("3").unwrap()
                )))
            }))
            .to_sql()
        );

        assert_eq!(
            "UNWRAP(nested, 'a.foo')",
            &Expr::Function(Box::new(Function::Unwrap {
                expr: Expr::Identifier("nested".to_owned()),
                selector: Expr::Literal(AstLiteral::QuotedString("a.foo".to_owned()))
            }))
            .to_sql()
        );

        assert_eq!(
            "GENERATE_UUID()",
            &Expr::Function(Box::new(Function::GenerateUuid())).to_sql()
        );

        assert_eq!(
            "FORMAT(DATE '2022-10-12', '%Y-%m')",
            &Expr::Function(Box::new(Function::Format {
                expr: Expr::TypedString {
                    data_type: DataType::Date,
                    value: "2022-10-12".to_owned()
                },
                format: Expr::Literal(AstLiteral::QuotedString("%Y-%m".to_owned()))
            }))
            .to_sql()
        );

        assert_eq!(
            "TO_DATE('2022-10-12', '%Y-%m-%d')",
            &Expr::Function(Box::new(Function::ToDate {
                expr: Expr::Literal(AstLiteral::QuotedString("2022-10-12".to_owned())),
                format: Expr::Literal(AstLiteral::QuotedString("%Y-%m-%d".to_owned()))
            }))
            .to_sql()
        );

        assert_eq!(
            "TO_TIMESTAMP('2022-10-12 00:34:23', '%Y-%m-%d %H:%M:%S')",
            &Expr::Function(Box::new(Function::ToTimestamp {
                expr: Expr::Literal(AstLiteral::QuotedString("2022-10-12 00:34:23".to_owned())),
                format: Expr::Literal(AstLiteral::QuotedString("%Y-%m-%d %H:%M:%S".to_owned()))
            }))
            .to_sql()
        );

        assert_eq!(
            "TO_TIME('00:34:23', '%H:%M:%S')",
            &Expr::Function(Box::new(Function::ToTime {
                expr: Expr::Literal(AstLiteral::QuotedString("00:34:23".to_owned())),
                format: Expr::Literal(AstLiteral::QuotedString("%H:%M:%S".to_owned()))
            }))
            .to_sql()
        );

        assert_eq!(
            "POSITION('cup' IN 'cupcake')",
            &Expr::Function(Box::new(Function::Position {
                from_expr: Expr::Literal(AstLiteral::QuotedString("cupcake".to_owned())),
                sub_expr: Expr::Literal(AstLiteral::QuotedString("cup".to_owned())),
            }))
            .to_sql()
        );

        assert_eq!(
            "ASCII('H')",
            &Expr::Function(Box::new(Function::Ascii(Expr::Literal(
                AstLiteral::QuotedString("H".to_owned())
            ))))
            .to_sql()
        );

        assert_eq!(
            r#"CHR(72)"#,
            &Expr::Function(Box::new(Function::Chr(Expr::Literal(AstLiteral::Number(
                BigDecimal::from_str("72").unwrap()
            )))))
            .to_sql()
        );

        assert_eq!(
            "EXTRACT(MINUTE FROM '2022-05-05 01:02:03')",
            &Expr::Function(Box::new(Function::Extract {
                field: DateTimeField::Minute,
                expr: Expr::Identifier("2022-05-05 01:02:03".to_owned())
            }))
            .to_sql()
        );
    }

    #[test]
    fn to_sql_aggregate() {
        assert_eq!(
            "MAX(id)",
            Expr::Aggregate(Box::new(Aggregate::Max(Expr::Identifier("id".to_owned())))).to_sql()
        );

        assert_eq!(
            "COUNT(*)",
            Expr::Aggregate(Box::new(Aggregate::Count(CountArgExpr::Wildcard))).to_sql()
        );

        assert_eq!(
            "MIN(id)",
            Expr::Aggregate(Box::new(Aggregate::Min(Expr::Identifier("id".to_owned())))).to_sql()
        );

        assert_eq!(
            "SUM(price)",
            &Expr::Aggregate(Box::new(Aggregate::Sum(Expr::Identifier(
                "price".to_owned()
            ))))
            .to_sql()
        );

        assert_eq!(
            "AVG(pay)",
            &Expr::Aggregate(Box::new(Aggregate::Avg(Expr::Identifier("pay".to_owned())))).to_sql()
        );
        assert_eq!(
            "VARIANCE(pay)",
            &Expr::Aggregate(Box::new(Aggregate::Variance(Expr::Identifier(
                "pay".to_owned()
            ))))
            .to_sql()
        );
        assert_eq!(
            "STDEV(total)",
            &Expr::Aggregate(Box::new(Aggregate::Stdev(Expr::Identifier(
                "total".to_owned()
            ))))
            .to_sql()
        );
    }
}
