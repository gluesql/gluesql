//! Functions embedded in the old schema, with v1 Expr arguments.

use {
    super::{SchemaDecodeError, expr::Expr},
    gluesql_core::ast::{self, DataType, DateTimeField, TrimWhereField},
    serde::Deserialize,
};

#[derive(Deserialize)]
pub(super) enum Function {
    Abs(Expr),
    AddMonth {
        expr: Expr,
        size: Expr,
    },
    Lower(Expr),
    Initcap(Expr),
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
    Replace {
        expr: Expr,
        old: Expr,
        new: Expr,
    },
    Cast {
        expr: Expr,
        data_type: DataType,
    },
    Ceil(Expr),
    Coalesce(Vec<Expr>),
    Concat(Vec<Expr>),
    ConcatWs {
        separator: Expr,
        exprs: Vec<Expr>,
    },
    Custom {
        name: String,
        exprs: Vec<Expr>,
    },
    IfNull {
        expr: Expr,
        then: Expr,
    },
    NullIf {
        expr1: Expr,
        expr2: Expr,
    },
    Rand(Option<Expr>),
    Round(Expr),
    Trunc(Expr),
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
    CurrentDate(),
    CurrentTime(),
    CurrentTimestamp(),
    Pi(),
    LastDay(Expr),
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
    Greatest(Vec<Expr>),
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
    FindIdx {
        from_expr: Expr,
        sub_expr: Expr,
        start: Option<Expr>,
    },
    Ascii(Expr),
    Chr(Expr),
    Md5(Expr),
    Hex(Expr),
    Append {
        expr: Expr,
        value: Expr,
    },
    Sort {
        expr: Expr,
        order: Option<Expr>,
    },
    Slice {
        expr: Expr,
        start: Expr,
        length: Expr,
    },
    Prepend {
        expr: Expr,
        value: Expr,
    },
    Skip {
        expr: Expr,
        size: Expr,
    },
    Take {
        expr: Expr,
        size: Expr,
    },
    GetX(Expr),
    GetY(Expr),
    Point {
        x: Expr,
        y: Expr,
    },
    CalcDistance {
        geometry1: Expr,
        geometry2: Expr,
    },
    IsEmpty(Expr),
    Length(Expr),
    Entries(Expr),
    Keys(Expr),
    Values(Expr),
    Splice {
        list_data: Expr,
        begin_index: Expr,
        end_index: Expr,
        values: Option<Expr>,
    },
    Dedup(Expr),
}

impl Function {
    pub(super) fn into_current(self) -> Result<ast::Function, SchemaDecodeError> {
        Ok(match self {
            Self::Abs(expr) => ast::Function::Abs(expr.into_current()?),
            Self::AddMonth { expr, size } => ast::Function::AddMonth {
                expr: expr.into_current()?,
                size: size.into_current()?,
            },
            Self::Lower(expr) => ast::Function::Lower(expr.into_current()?),
            Self::Initcap(expr) => ast::Function::Initcap(expr.into_current()?),
            Self::Upper(expr) => ast::Function::Upper(expr.into_current()?),
            Self::Left { expr, size } => ast::Function::Left {
                expr: expr.into_current()?,
                size: size.into_current()?,
            },
            Self::Right { expr, size } => ast::Function::Right {
                expr: expr.into_current()?,
                size: size.into_current()?,
            },
            Self::Asin(expr) => ast::Function::Asin(expr.into_current()?),
            Self::Acos(expr) => ast::Function::Acos(expr.into_current()?),
            Self::Atan(expr) => ast::Function::Atan(expr.into_current()?),
            Self::Lpad { expr, size, fill } => ast::Function::Lpad {
                expr: expr.into_current()?,
                size: size.into_current()?,
                fill: fill.map(Expr::into_current).transpose()?,
            },
            Self::Rpad { expr, size, fill } => ast::Function::Rpad {
                expr: expr.into_current()?,
                size: size.into_current()?,
                fill: fill.map(Expr::into_current).transpose()?,
            },
            Self::Replace { expr, old, new } => ast::Function::Replace {
                expr: expr.into_current()?,
                old: old.into_current()?,
                new: new.into_current()?,
            },
            Self::Cast { expr, data_type } => ast::Function::Cast {
                expr: expr.into_current()?,
                data_type,
            },
            Self::Ceil(expr) => ast::Function::Ceil(expr.into_current()?),
            Self::Coalesce(expr) => ast::Function::Coalesce(
                expr.into_iter()
                    .map(Expr::into_current)
                    .collect::<Result<_, _>>()?,
            ),
            Self::Concat(expr) => ast::Function::Concat(
                expr.into_iter()
                    .map(Expr::into_current)
                    .collect::<Result<_, _>>()?,
            ),
            Self::ConcatWs { separator, exprs } => ast::Function::ConcatWs {
                separator: separator.into_current()?,
                exprs: exprs
                    .into_iter()
                    .map(Expr::into_current)
                    .collect::<Result<_, _>>()?,
            },
            Self::Custom { name, exprs } => ast::Function::Custom {
                name,
                exprs: exprs
                    .into_iter()
                    .map(Expr::into_current)
                    .collect::<Result<_, _>>()?,
            },
            Self::IfNull { expr, then } => ast::Function::IfNull {
                expr: expr.into_current()?,
                then: then.into_current()?,
            },
            Self::NullIf { expr1, expr2 } => ast::Function::NullIf {
                expr1: expr1.into_current()?,
                expr2: expr2.into_current()?,
            },
            Self::Rand(expr) => ast::Function::Rand(expr.map(Expr::into_current).transpose()?),
            Self::Round(expr) => ast::Function::Round(expr.into_current()?),
            Self::Trunc(expr) => ast::Function::Trunc(expr.into_current()?),
            Self::Floor(expr) => ast::Function::Floor(expr.into_current()?),
            Self::Trim {
                expr,
                filter_chars,
                trim_where_field,
            } => ast::Function::Trim {
                expr: expr.into_current()?,
                filter_chars: filter_chars.map(Expr::into_current).transpose()?,
                trim_where_field,
            },
            Self::Exp(expr) => ast::Function::Exp(expr.into_current()?),
            Self::Extract { field, expr } => ast::Function::Extract {
                field,
                expr: expr.into_current()?,
            },
            Self::Ln(expr) => ast::Function::Ln(expr.into_current()?),
            Self::Log { antilog, base } => ast::Function::Log {
                antilog: antilog.into_current()?,
                base: base.into_current()?,
            },
            Self::Log2(expr) => ast::Function::Log2(expr.into_current()?),
            Self::Log10(expr) => ast::Function::Log10(expr.into_current()?),
            Self::Div { dividend, divisor } => ast::Function::Div {
                dividend: dividend.into_current()?,
                divisor: divisor.into_current()?,
            },
            Self::Mod { dividend, divisor } => ast::Function::Mod {
                dividend: dividend.into_current()?,
                divisor: divisor.into_current()?,
            },
            Self::Gcd { left, right } => ast::Function::Gcd {
                left: left.into_current()?,
                right: right.into_current()?,
            },
            Self::Lcm { left, right } => ast::Function::Lcm {
                left: left.into_current()?,
                right: right.into_current()?,
            },
            Self::Sin(expr) => ast::Function::Sin(expr.into_current()?),
            Self::Cos(expr) => ast::Function::Cos(expr.into_current()?),
            Self::Tan(expr) => ast::Function::Tan(expr.into_current()?),
            Self::Sqrt(expr) => ast::Function::Sqrt(expr.into_current()?),
            Self::Power { expr, power } => ast::Function::Power {
                expr: expr.into_current()?,
                power: power.into_current()?,
            },
            Self::Radians(expr) => ast::Function::Radians(expr.into_current()?),
            Self::Degrees(expr) => ast::Function::Degrees(expr.into_current()?),
            Self::Now() => ast::Function::Now(),
            Self::CurrentDate() => ast::Function::CurrentDate(),
            Self::CurrentTime() => ast::Function::CurrentTime(),
            Self::CurrentTimestamp() => ast::Function::CurrentTimestamp(),
            Self::Pi() => ast::Function::Pi(),
            Self::LastDay(expr) => ast::Function::LastDay(expr.into_current()?),
            Self::Ltrim { expr, chars } => ast::Function::Ltrim {
                expr: expr.into_current()?,
                chars: chars.map(Expr::into_current).transpose()?,
            },
            Self::Rtrim { expr, chars } => ast::Function::Rtrim {
                expr: expr.into_current()?,
                chars: chars.map(Expr::into_current).transpose()?,
            },
            Self::Reverse(expr) => ast::Function::Reverse(expr.into_current()?),
            Self::Repeat { expr, num } => ast::Function::Repeat {
                expr: expr.into_current()?,
                num: num.into_current()?,
            },
            Self::Sign(expr) => ast::Function::Sign(expr.into_current()?),
            Self::Substr { expr, start, count } => ast::Function::Substr {
                expr: expr.into_current()?,
                start: start.into_current()?,
                count: count.map(Expr::into_current).transpose()?,
            },
            Self::Unwrap { expr, selector } => ast::Function::Unwrap {
                expr: expr.into_current()?,
                selector: selector.into_current()?,
            },
            Self::GenerateUuid() => ast::Function::GenerateUuid(),
            Self::Greatest(expr) => ast::Function::Greatest(
                expr.into_iter()
                    .map(Expr::into_current)
                    .collect::<Result<_, _>>()?,
            ),
            Self::Format { expr, format } => ast::Function::Format {
                expr: expr.into_current()?,
                format: format.into_current()?,
            },
            Self::ToDate { expr, format } => ast::Function::ToDate {
                expr: expr.into_current()?,
                format: format.into_current()?,
            },
            Self::ToTimestamp { expr, format } => ast::Function::ToTimestamp {
                expr: expr.into_current()?,
                format: format.into_current()?,
            },
            Self::ToTime { expr, format } => ast::Function::ToTime {
                expr: expr.into_current()?,
                format: format.into_current()?,
            },
            Self::Position {
                from_expr,
                sub_expr,
            } => ast::Function::Position {
                from_expr: from_expr.into_current()?,
                sub_expr: sub_expr.into_current()?,
            },
            Self::FindIdx {
                from_expr,
                sub_expr,
                start,
            } => ast::Function::FindIdx {
                from_expr: from_expr.into_current()?,
                sub_expr: sub_expr.into_current()?,
                start: start.map(Expr::into_current).transpose()?,
            },
            Self::Ascii(expr) => ast::Function::Ascii(expr.into_current()?),
            Self::Chr(expr) => ast::Function::Chr(expr.into_current()?),
            Self::Md5(expr) => ast::Function::Md5(expr.into_current()?),
            Self::Hex(expr) => ast::Function::Hex(expr.into_current()?),
            Self::Append { expr, value } => ast::Function::Append {
                expr: expr.into_current()?,
                value: value.into_current()?,
            },
            Self::Sort { expr, order } => ast::Function::Sort {
                expr: expr.into_current()?,
                order: order.map(Expr::into_current).transpose()?,
            },
            Self::Slice {
                expr,
                start,
                length,
            } => ast::Function::Slice {
                expr: expr.into_current()?,
                start: start.into_current()?,
                length: length.into_current()?,
            },
            Self::Prepend { expr, value } => ast::Function::Prepend {
                expr: expr.into_current()?,
                value: value.into_current()?,
            },
            Self::Skip { expr, size } => ast::Function::Skip {
                expr: expr.into_current()?,
                size: size.into_current()?,
            },
            Self::Take { expr, size } => ast::Function::Take {
                expr: expr.into_current()?,
                size: size.into_current()?,
            },
            Self::GetX(expr) => ast::Function::GetX(expr.into_current()?),
            Self::GetY(expr) => ast::Function::GetY(expr.into_current()?),
            Self::Point { x, y } => ast::Function::Point {
                x: x.into_current()?,
                y: y.into_current()?,
            },
            Self::CalcDistance {
                geometry1,
                geometry2,
            } => ast::Function::CalcDistance {
                geometry1: geometry1.into_current()?,
                geometry2: geometry2.into_current()?,
            },
            Self::IsEmpty(expr) => ast::Function::IsEmpty(expr.into_current()?),
            Self::Length(expr) => ast::Function::Length(expr.into_current()?),
            Self::Entries(expr) => ast::Function::Entries(expr.into_current()?),
            Self::Keys(expr) => ast::Function::Keys(expr.into_current()?),
            Self::Values(expr) => ast::Function::Values(expr.into_current()?),
            Self::Splice {
                list_data,
                begin_index,
                end_index,
                values,
            } => ast::Function::Splice {
                list_data: list_data.into_current()?,
                begin_index: begin_index.into_current()?,
                end_index: end_index.into_current()?,
                values: values.map(Expr::into_current).transpose()?,
            },
            Self::Dedup(expr) => ast::Function::Dedup(expr.into_current()?),
        })
    }
}

#[derive(Deserialize)]
enum AggregateFunction {
    Count(CountArgExpr),
    Sum(Expr),
    Max(Expr),
    Min(Expr),
    Avg(Expr),
    Variance(Expr),
    Stdev(Expr),
}

impl AggregateFunction {
    fn into_current(self) -> Result<ast::AggregateFunction, SchemaDecodeError> {
        Ok(match self {
            Self::Count(count) => ast::AggregateFunction::Count(count.into_current()?),
            Self::Sum(expr) => ast::AggregateFunction::Sum(expr.into_current()?),
            Self::Max(expr) => ast::AggregateFunction::Max(expr.into_current()?),
            Self::Min(expr) => ast::AggregateFunction::Min(expr.into_current()?),
            Self::Avg(expr) => ast::AggregateFunction::Avg(expr.into_current()?),
            Self::Variance(expr) => ast::AggregateFunction::Variance(expr.into_current()?),
            Self::Stdev(expr) => ast::AggregateFunction::Stdev(expr.into_current()?),
        })
    }
}

#[derive(Deserialize)]
pub(super) struct Aggregate {
    func: AggregateFunction,
    distinct: bool,
}

impl Aggregate {
    pub(super) fn into_current(self) -> Result<ast::Aggregate, SchemaDecodeError> {
        Ok(ast::Aggregate {
            func: self.func.into_current()?,
            distinct: self.distinct,
        })
    }
}

#[derive(Deserialize)]
enum CountArgExpr {
    Expr(Expr),
    Wildcard,
}

impl CountArgExpr {
    fn into_current(self) -> Result<ast::CountArgExpr, SchemaDecodeError> {
        Ok(match self {
            Self::Expr(expr) => ast::CountArgExpr::Expr(expr.into_current()?),
            Self::Wildcard => ast::CountArgExpr::Wildcard,
        })
    }
}

#[cfg(test)]
mod nested_expr_tags {
    use {
        super::super::expr::Expr,
        crate::migration::decode,
        gluesql_core::{ast, data::Value},
    };

    #[test]
    fn restores_old_expr_tags_in_function_arguments() {
        // v1 Expr::Function(15) -> Function::Abs(0) -> Expr::Value(13).
        // The function tag stays the same, but both Expr tags shifted in v0.20.0.
        let payload = bincode::serialize(&(15_u32, 0_u32, 13_u32, Value::I64(-7))).unwrap();
        assert_eq!(
            decode::deserialize::<Expr>(&payload)
                .unwrap()
                .into_current()
                .unwrap(),
            ast::Expr::Function(Box::new(ast::Function::Abs(ast::Expr::Value(Value::I64(
                -7
            )))))
        );
    }

    #[test]
    fn restores_old_expr_tags_in_aggregate_arguments() {
        // v1 Expr::Aggregate(16) -> AggregateFunction::Sum(1) -> Expr::Value(13),
        // followed by Aggregate::distinct. This takes a separate path from Function.
        let payload = bincode::serialize(&(16_u32, 1_u32, 13_u32, Value::I64(7), true)).unwrap();
        assert_eq!(
            decode::deserialize::<Expr>(&payload)
                .unwrap()
                .into_current()
                .unwrap(),
            ast::Expr::Aggregate(Box::new(ast::Aggregate {
                func: ast::AggregateFunction::Sum(ast::Expr::Value(Value::I64(7))),
                distinct: true,
            }))
        );
    }
}

#[cfg(test)]
mod function_conversion {
    use {
        super::super::function as old,
        gluesql_core::ast::{self as current, DataType, DateTimeField, TrimWhereField},
    };

    // Construct each side independently; only the construction syntax is shared.
    macro_rules! test {
        ($case:expr) => {{
            let input = {
                #[allow(unused_imports)]
                use old::{Aggregate, AggregateFunction, CountArgExpr, Function};
                #[allow(dead_code)]
                fn ident(name: &str) -> old::Expr {
                    old::Expr::Identifier(name.into())
                }
                $case
            };
            let expected = {
                #[allow(unused_imports)]
                use current::{Aggregate, AggregateFunction, CountArgExpr, Function};
                #[allow(dead_code)]
                fn ident(name: &str) -> current::Expr {
                    current::Expr::Identifier(name.into())
                }
                $case
            };
            let actual = input.into_current().unwrap();
            assert_eq!(actual, expected, "{}", stringify!($case));
        }};
    }

    #[test]
    fn function_variants() {
        test!(Function::Abs(ident("value")));
        test!(Function::AddMonth {
            expr: ident("date"),
            size: ident("months"),
        });
        test!(Function::Lower(ident("text")));
        test!(Function::Initcap(ident("text")));
        test!(Function::Upper(ident("text")));
        test!(Function::Left {
            expr: ident("text"),
            size: ident("size"),
        });
        test!(Function::Right {
            expr: ident("text"),
            size: ident("size"),
        });
        test!(Function::Asin(ident("value")));
        test!(Function::Acos(ident("value")));
        test!(Function::Atan(ident("value")));
        test!(Function::Lpad {
            expr: ident("text"),
            size: ident("size"),
            fill: None,
        });
        test!(Function::Lpad {
            expr: ident("text"),
            size: ident("size"),
            fill: Some(ident("fill")),
        });
        test!(Function::Rpad {
            expr: ident("text"),
            size: ident("size"),
            fill: None,
        });
        test!(Function::Rpad {
            expr: ident("text"),
            size: ident("size"),
            fill: Some(ident("fill")),
        });
        test!(Function::Replace {
            expr: ident("text"),
            old: ident("old_text"),
            new: ident("new_text"),
        });
        test!(Function::Cast {
            expr: ident("value"),
            data_type: DataType::Text,
        });
        test!(Function::Ceil(ident("value")));
        test!(Function::Coalesce(vec![]));
        test!(Function::Coalesce(vec![ident("first"), ident("second")]));
        test!(Function::Concat(vec![]));
        test!(Function::Concat(vec![ident("first"), ident("second")]));
        test!(Function::ConcatWs {
            separator: ident("separator"),
            exprs: vec![],
        });
        test!(Function::ConcatWs {
            separator: ident("separator"),
            exprs: vec![ident("first"), ident("second")],
        });
        test!(Function::Custom {
            name: "custom_name".into(),
            exprs: vec![],
        });
        test!(Function::Custom {
            name: "custom_name".into(),
            exprs: vec![ident("first"), ident("second")],
        });
        test!(Function::IfNull {
            expr: ident("value"),
            then: ident("fallback"),
        });
        test!(Function::NullIf {
            expr1: ident("first"),
            expr2: ident("second"),
        });
        test!(Function::Rand(None));
        test!(Function::Rand(Some(ident("seed"))));
        test!(Function::Round(ident("value")));
        test!(Function::Trunc(ident("value")));
        test!(Function::Floor(ident("value")));
        test!(Function::Trim {
            expr: ident("text"),
            filter_chars: None,
            trim_where_field: None,
        });
        test!(Function::Trim {
            expr: ident("text"),
            filter_chars: Some(ident("chars")),
            trim_where_field: Some(TrimWhereField::Leading),
        });
        test!(Function::Exp(ident("value")));
        test!(Function::Extract {
            field: DateTimeField::Month,
            expr: ident("date"),
        });
        test!(Function::Ln(ident("value")));
        test!(Function::Log {
            antilog: ident("antilog"),
            base: ident("base"),
        });
        test!(Function::Log2(ident("value")));
        test!(Function::Log10(ident("value")));
        test!(Function::Div {
            dividend: ident("dividend"),
            divisor: ident("divisor"),
        });
        test!(Function::Mod {
            dividend: ident("dividend"),
            divisor: ident("divisor"),
        });
        test!(Function::Gcd {
            left: ident("left"),
            right: ident("right"),
        });
        test!(Function::Lcm {
            left: ident("left"),
            right: ident("right"),
        });
        test!(Function::Sin(ident("value")));
        test!(Function::Cos(ident("value")));
        test!(Function::Tan(ident("value")));
        test!(Function::Sqrt(ident("value")));
        test!(Function::Power {
            expr: ident("value"),
            power: ident("power"),
        });
        test!(Function::Radians(ident("value")));
        test!(Function::Degrees(ident("value")));
        test!(Function::Now());
        test!(Function::CurrentDate());
        test!(Function::CurrentTime());
        test!(Function::CurrentTimestamp());
        test!(Function::Pi());
        test!(Function::LastDay(ident("date")));
        test!(Function::Ltrim {
            expr: ident("text"),
            chars: None,
        });
        test!(Function::Ltrim {
            expr: ident("text"),
            chars: Some(ident("chars")),
        });
        test!(Function::Rtrim {
            expr: ident("text"),
            chars: None,
        });
        test!(Function::Rtrim {
            expr: ident("text"),
            chars: Some(ident("chars")),
        });
        test!(Function::Reverse(ident("text")));
        test!(Function::Repeat {
            expr: ident("text"),
            num: ident("times"),
        });
        test!(Function::Sign(ident("value")));
        test!(Function::Substr {
            expr: ident("text"),
            start: ident("start"),
            count: None,
        });
        test!(Function::Substr {
            expr: ident("text"),
            start: ident("start"),
            count: Some(ident("count")),
        });
        test!(Function::Unwrap {
            expr: ident("object"),
            selector: ident("selector"),
        });
        test!(Function::GenerateUuid());
        test!(Function::Greatest(vec![]));
        test!(Function::Greatest(vec![ident("first"), ident("second")]));
        test!(Function::Format {
            expr: ident("value"),
            format: ident("format"),
        });
        test!(Function::ToDate {
            expr: ident("text"),
            format: ident("format"),
        });
        test!(Function::ToTimestamp {
            expr: ident("text"),
            format: ident("format"),
        });
        test!(Function::ToTime {
            expr: ident("text"),
            format: ident("format"),
        });
        test!(Function::Position {
            from_expr: ident("text"),
            sub_expr: ident("substring"),
        });
        test!(Function::FindIdx {
            from_expr: ident("text"),
            sub_expr: ident("substring"),
            start: None,
        });
        test!(Function::FindIdx {
            from_expr: ident("text"),
            sub_expr: ident("substring"),
            start: Some(ident("start")),
        });
        test!(Function::Ascii(ident("text")));
        test!(Function::Chr(ident("code")));
        test!(Function::Md5(ident("text")));
        test!(Function::Hex(ident("value")));
        test!(Function::Append {
            expr: ident("list"),
            value: ident("value"),
        });
        test!(Function::Sort {
            expr: ident("list"),
            order: None,
        });
        test!(Function::Sort {
            expr: ident("list"),
            order: Some(ident("order")),
        });
        test!(Function::Slice {
            expr: ident("list"),
            start: ident("start"),
            length: ident("length"),
        });
        test!(Function::Prepend {
            expr: ident("list"),
            value: ident("value"),
        });
        test!(Function::Skip {
            expr: ident("list"),
            size: ident("size"),
        });
        test!(Function::Take {
            expr: ident("list"),
            size: ident("size"),
        });
        test!(Function::GetX(ident("point")));
        test!(Function::GetY(ident("point")));
        test!(Function::Point {
            x: ident("x"),
            y: ident("y"),
        });
        test!(Function::CalcDistance {
            geometry1: ident("first"),
            geometry2: ident("second"),
        });
        test!(Function::IsEmpty(ident("list")));
        test!(Function::Length(ident("list")));
        test!(Function::Entries(ident("map")));
        test!(Function::Keys(ident("map")));
        test!(Function::Values(ident("map")));
        test!(Function::Splice {
            list_data: ident("list"),
            begin_index: ident("begin"),
            end_index: ident("end"),
            values: None,
        });
        test!(Function::Splice {
            list_data: ident("list"),
            begin_index: ident("begin"),
            end_index: ident("end"),
            values: Some(ident("replacement")),
        });
        test!(Function::Dedup(ident("list")));
    }

    #[test]
    fn aggregate_variants() {
        test!(Aggregate {
            func: AggregateFunction::Count(CountArgExpr::Expr(ident("value"))),
            distinct: true,
        });
        test!(Aggregate {
            func: AggregateFunction::Count(CountArgExpr::Wildcard),
            distinct: false,
        });
        test!(Aggregate {
            func: AggregateFunction::Sum(ident("value")),
            distinct: false,
        });
        test!(Aggregate {
            func: AggregateFunction::Max(ident("value")),
            distinct: true,
        });
        test!(Aggregate {
            func: AggregateFunction::Min(ident("value")),
            distinct: false,
        });
        test!(Aggregate {
            func: AggregateFunction::Avg(ident("value")),
            distinct: true,
        });
        test!(Aggregate {
            func: AggregateFunction::Variance(ident("value")),
            distinct: false,
        });
        test!(Aggregate {
            func: AggregateFunction::Stdev(ident("value")),
            distinct: true,
        });
    }
}
