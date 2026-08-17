use {
    super::{ExprPlan, fmt_expr, fmt_expr_list},
    crate::{
        ast::{self, DataType, DateTimeField, TrimWhereField},
        plan::explain::{Explain, ExplainContext},
    },
    serde::{Deserialize, Serialize},
    strum_macros::Display,
};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, Display)]
#[strum(serialize_all = "SCREAMING_SNAKE_CASE")]
pub enum FunctionExprPlan {
    Abs(ExprPlan),
    AddMonth {
        expr: ExprPlan,
        size: ExprPlan,
    },
    Lower(ExprPlan),
    Initcap(ExprPlan),
    Upper(ExprPlan),
    Left {
        expr: ExprPlan,
        size: ExprPlan,
    },
    Right {
        expr: ExprPlan,
        size: ExprPlan,
    },
    Asin(ExprPlan),
    Acos(ExprPlan),
    Atan(ExprPlan),
    Lpad {
        expr: ExprPlan,
        size: ExprPlan,
        fill: Option<ExprPlan>,
    },
    Rpad {
        expr: ExprPlan,
        size: ExprPlan,
        fill: Option<ExprPlan>,
    },
    Replace {
        expr: ExprPlan,
        old: ExprPlan,
        new: ExprPlan,
    },
    Cast {
        expr: ExprPlan,
        data_type: DataType,
    },
    Ceil(ExprPlan),
    Coalesce(Vec<ExprPlan>),
    Concat(Vec<ExprPlan>),
    ConcatWs {
        separator: ExprPlan,
        exprs: Vec<ExprPlan>,
    },
    Custom {
        name: String,
        exprs: Vec<ExprPlan>,
    },
    IfNull {
        expr: ExprPlan,
        then: ExprPlan,
    },
    NullIf {
        expr1: ExprPlan,
        expr2: ExprPlan,
    },
    Rand(Option<ExprPlan>),
    Round(ExprPlan),
    Trunc(ExprPlan),
    Floor(ExprPlan),
    Trim {
        expr: ExprPlan,
        filter_chars: Option<ExprPlan>,
        trim_where_field: Option<TrimWhereField>,
    },
    Exp(ExprPlan),
    Extract {
        field: DateTimeField,
        expr: ExprPlan,
    },
    Ln(ExprPlan),
    Log {
        antilog: ExprPlan,
        base: ExprPlan,
    },
    Log2(ExprPlan),
    Log10(ExprPlan),
    Div {
        dividend: ExprPlan,
        divisor: ExprPlan,
    },
    Mod {
        dividend: ExprPlan,
        divisor: ExprPlan,
    },
    Gcd {
        left: ExprPlan,
        right: ExprPlan,
    },
    Lcm {
        left: ExprPlan,
        right: ExprPlan,
    },
    Sin(ExprPlan),
    Cos(ExprPlan),
    Tan(ExprPlan),
    Sqrt(ExprPlan),
    Power {
        expr: ExprPlan,
        power: ExprPlan,
    },
    Radians(ExprPlan),
    Degrees(ExprPlan),
    Now(),
    CurrentDate(),
    CurrentTime(),
    CurrentTimestamp(),
    Pi(),
    LastDay(ExprPlan),
    Ltrim {
        expr: ExprPlan,
        chars: Option<ExprPlan>,
    },
    Rtrim {
        expr: ExprPlan,
        chars: Option<ExprPlan>,
    },
    Reverse(ExprPlan),
    Repeat {
        expr: ExprPlan,
        num: ExprPlan,
    },
    Sign(ExprPlan),
    Substr {
        expr: ExprPlan,
        start: ExprPlan,
        count: Option<ExprPlan>,
    },
    Unwrap {
        expr: ExprPlan,
        selector: ExprPlan,
    },
    GenerateUuid(),
    Greatest(Vec<ExprPlan>),
    Format {
        expr: ExprPlan,
        format: ExprPlan,
    },
    ToDate {
        expr: ExprPlan,
        format: ExprPlan,
    },
    ToTimestamp {
        expr: ExprPlan,
        format: ExprPlan,
    },
    ToTime {
        expr: ExprPlan,
        format: ExprPlan,
    },
    Position {
        from_expr: ExprPlan,
        sub_expr: ExprPlan,
    },
    FindIdx {
        from_expr: ExprPlan,
        sub_expr: ExprPlan,
        start: Option<ExprPlan>,
    },
    Ascii(ExprPlan),
    Chr(ExprPlan),
    Md5(ExprPlan),
    Hex(ExprPlan),
    Append {
        expr: ExprPlan,
        value: ExprPlan,
    },
    Sort {
        expr: ExprPlan,
        order: Option<ExprPlan>,
    },
    Slice {
        expr: ExprPlan,
        start: ExprPlan,
        length: ExprPlan,
    },
    Prepend {
        expr: ExprPlan,
        value: ExprPlan,
    },
    Skip {
        expr: ExprPlan,
        size: ExprPlan,
    },
    Take {
        expr: ExprPlan,
        size: ExprPlan,
    },
    GetX(ExprPlan),
    GetY(ExprPlan),
    Point {
        x: ExprPlan,
        y: ExprPlan,
    },
    CalcDistance {
        geometry1: ExprPlan,
        geometry2: ExprPlan,
    },
    IsEmpty(ExprPlan),
    Length(ExprPlan),
    Entries(ExprPlan),
    Keys(ExprPlan),
    Values(ExprPlan),
    Splice {
        list_data: ExprPlan,
        begin_index: ExprPlan,
        end_index: ExprPlan,
        values: Option<ExprPlan>,
    },
    Dedup(ExprPlan),
}

impl From<ast::Function> for FunctionExprPlan {
    fn from(function: ast::Function) -> Self {
        match function {
            ast::Function::Abs(expr) => Self::Abs(expr.into()),
            ast::Function::AddMonth { expr, size } => Self::AddMonth {
                expr: expr.into(),
                size: size.into(),
            },
            ast::Function::Lower(expr) => Self::Lower(expr.into()),
            ast::Function::Initcap(expr) => Self::Initcap(expr.into()),
            ast::Function::Upper(expr) => Self::Upper(expr.into()),
            ast::Function::Left { expr, size } => Self::Left {
                expr: expr.into(),
                size: size.into(),
            },
            ast::Function::Right { expr, size } => Self::Right {
                expr: expr.into(),
                size: size.into(),
            },
            ast::Function::Asin(expr) => Self::Asin(expr.into()),
            ast::Function::Acos(expr) => Self::Acos(expr.into()),
            ast::Function::Atan(expr) => Self::Atan(expr.into()),
            ast::Function::Lpad { expr, size, fill } => Self::Lpad {
                expr: expr.into(),
                size: size.into(),
                fill: fill.map(Into::into),
            },
            ast::Function::Rpad { expr, size, fill } => Self::Rpad {
                expr: expr.into(),
                size: size.into(),
                fill: fill.map(Into::into),
            },
            ast::Function::Replace { expr, old, new } => Self::Replace {
                expr: expr.into(),
                old: old.into(),
                new: new.into(),
            },
            ast::Function::Cast { expr, data_type } => Self::Cast {
                expr: expr.into(),
                data_type,
            },
            ast::Function::Ceil(expr) => Self::Ceil(expr.into()),
            ast::Function::Coalesce(exprs) => {
                Self::Coalesce(exprs.into_iter().map(Into::into).collect())
            }
            ast::Function::Concat(exprs) => {
                Self::Concat(exprs.into_iter().map(Into::into).collect())
            }
            ast::Function::ConcatWs { separator, exprs } => Self::ConcatWs {
                separator: separator.into(),
                exprs: exprs.into_iter().map(Into::into).collect(),
            },
            ast::Function::Custom { name, exprs } => Self::Custom {
                name,
                exprs: exprs.into_iter().map(Into::into).collect(),
            },
            ast::Function::IfNull { expr, then } => Self::IfNull {
                expr: expr.into(),
                then: then.into(),
            },
            ast::Function::NullIf { expr1, expr2 } => Self::NullIf {
                expr1: expr1.into(),
                expr2: expr2.into(),
            },
            ast::Function::Rand(expr) => Self::Rand(expr.map(Into::into)),
            ast::Function::Round(expr) => Self::Round(expr.into()),
            ast::Function::Trunc(expr) => Self::Trunc(expr.into()),
            ast::Function::Floor(expr) => Self::Floor(expr.into()),
            ast::Function::Trim {
                expr,
                filter_chars,
                trim_where_field,
            } => Self::Trim {
                expr: expr.into(),
                filter_chars: filter_chars.map(Into::into),
                trim_where_field,
            },
            ast::Function::Exp(expr) => Self::Exp(expr.into()),
            ast::Function::Extract { field, expr } => Self::Extract {
                field,
                expr: expr.into(),
            },
            ast::Function::Ln(expr) => Self::Ln(expr.into()),
            ast::Function::Log { antilog, base } => Self::Log {
                antilog: antilog.into(),
                base: base.into(),
            },
            ast::Function::Log2(expr) => Self::Log2(expr.into()),
            ast::Function::Log10(expr) => Self::Log10(expr.into()),
            ast::Function::Div { dividend, divisor } => Self::Div {
                dividend: dividend.into(),
                divisor: divisor.into(),
            },
            ast::Function::Mod { dividend, divisor } => Self::Mod {
                dividend: dividend.into(),
                divisor: divisor.into(),
            },
            ast::Function::Gcd { left, right } => Self::Gcd {
                left: left.into(),
                right: right.into(),
            },
            ast::Function::Lcm { left, right } => Self::Lcm {
                left: left.into(),
                right: right.into(),
            },
            ast::Function::Sin(expr) => Self::Sin(expr.into()),
            ast::Function::Cos(expr) => Self::Cos(expr.into()),
            ast::Function::Tan(expr) => Self::Tan(expr.into()),
            ast::Function::Sqrt(expr) => Self::Sqrt(expr.into()),
            ast::Function::Power { expr, power } => Self::Power {
                expr: expr.into(),
                power: power.into(),
            },
            ast::Function::Radians(expr) => Self::Radians(expr.into()),
            ast::Function::Degrees(expr) => Self::Degrees(expr.into()),
            ast::Function::Now() => Self::Now(),
            ast::Function::CurrentDate() => Self::CurrentDate(),
            ast::Function::CurrentTime() => Self::CurrentTime(),
            ast::Function::CurrentTimestamp() => Self::CurrentTimestamp(),
            ast::Function::Pi() => Self::Pi(),
            ast::Function::LastDay(expr) => Self::LastDay(expr.into()),
            ast::Function::Ltrim { expr, chars } => Self::Ltrim {
                expr: expr.into(),
                chars: chars.map(Into::into),
            },
            ast::Function::Rtrim { expr, chars } => Self::Rtrim {
                expr: expr.into(),
                chars: chars.map(Into::into),
            },
            ast::Function::Reverse(expr) => Self::Reverse(expr.into()),
            ast::Function::Repeat { expr, num } => Self::Repeat {
                expr: expr.into(),
                num: num.into(),
            },
            ast::Function::Sign(expr) => Self::Sign(expr.into()),
            ast::Function::Substr { expr, start, count } => Self::Substr {
                expr: expr.into(),
                start: start.into(),
                count: count.map(Into::into),
            },
            ast::Function::Unwrap { expr, selector } => Self::Unwrap {
                expr: expr.into(),
                selector: selector.into(),
            },
            ast::Function::GenerateUuid() => Self::GenerateUuid(),
            ast::Function::Greatest(exprs) => {
                Self::Greatest(exprs.into_iter().map(Into::into).collect())
            }
            ast::Function::Format { expr, format } => Self::Format {
                expr: expr.into(),
                format: format.into(),
            },
            ast::Function::ToDate { expr, format } => Self::ToDate {
                expr: expr.into(),
                format: format.into(),
            },
            ast::Function::ToTimestamp { expr, format } => Self::ToTimestamp {
                expr: expr.into(),
                format: format.into(),
            },
            ast::Function::ToTime { expr, format } => Self::ToTime {
                expr: expr.into(),
                format: format.into(),
            },
            ast::Function::Position {
                from_expr,
                sub_expr,
            } => Self::Position {
                from_expr: from_expr.into(),
                sub_expr: sub_expr.into(),
            },
            ast::Function::FindIdx {
                from_expr,
                sub_expr,
                start,
            } => Self::FindIdx {
                from_expr: from_expr.into(),
                sub_expr: sub_expr.into(),
                start: start.map(Into::into),
            },
            ast::Function::Ascii(expr) => Self::Ascii(expr.into()),
            ast::Function::Chr(expr) => Self::Chr(expr.into()),
            ast::Function::Md5(expr) => Self::Md5(expr.into()),
            ast::Function::Hex(expr) => Self::Hex(expr.into()),
            ast::Function::Append { expr, value } => Self::Append {
                expr: expr.into(),
                value: value.into(),
            },
            ast::Function::Sort { expr, order } => Self::Sort {
                expr: expr.into(),
                order: order.map(Into::into),
            },
            ast::Function::Slice {
                expr,
                start,
                length,
            } => Self::Slice {
                expr: expr.into(),
                start: start.into(),
                length: length.into(),
            },
            ast::Function::Prepend { expr, value } => Self::Prepend {
                expr: expr.into(),
                value: value.into(),
            },
            ast::Function::Skip { expr, size } => Self::Skip {
                expr: expr.into(),
                size: size.into(),
            },
            ast::Function::Take { expr, size } => Self::Take {
                expr: expr.into(),
                size: size.into(),
            },
            ast::Function::GetX(expr) => Self::GetX(expr.into()),
            ast::Function::GetY(expr) => Self::GetY(expr.into()),
            ast::Function::Point { x, y } => Self::Point {
                x: x.into(),
                y: y.into(),
            },
            ast::Function::CalcDistance {
                geometry1,
                geometry2,
            } => Self::CalcDistance {
                geometry1: geometry1.into(),
                geometry2: geometry2.into(),
            },
            ast::Function::IsEmpty(expr) => Self::IsEmpty(expr.into()),
            ast::Function::Length(expr) => Self::Length(expr.into()),
            ast::Function::Entries(expr) => Self::Entries(expr.into()),
            ast::Function::Keys(expr) => Self::Keys(expr.into()),
            ast::Function::Values(expr) => Self::Values(expr.into()),
            ast::Function::Splice {
                list_data,
                begin_index,
                end_index,
                values,
            } => Self::Splice {
                list_data: list_data.into(),
                begin_index: begin_index.into(),
                end_index: end_index.into(),
                values: values.map(Into::into),
            },
            ast::Function::Dedup(expr) => Self::Dedup(expr.into()),
        }
    }
}

impl Explain for FunctionExprPlan {
    type Output = String;

    fn explain(&self, context: &mut ExplainContext) -> String {
        let mut output = String::new();
        match self {
            Self::Cast { expr, data_type } => {
                output.push_str("CAST(");
                fmt_expr(expr, context, &mut output);
                output.push_str(" AS ");
                output.push_str(&data_type.to_string());
                output.push(')');
            }
            Self::Extract { field, expr } => {
                output.push_str("EXTRACT(");
                output.push_str(&field.to_string());
                output.push_str(" FROM ");
                fmt_expr(expr, context, &mut output);
                output.push(')');
            }
            Self::Custom { name, exprs } => {
                output.push_str(name);
                output.push('(');
                fmt_expr_list(exprs, context, &mut output);
                output.push(')');
            }
            _ => {
                output.push_str(&self.to_string());
                output.push('(');
                for (index, expr) in self.as_exprs().enumerate() {
                    if index > 0 {
                        output.push_str(", ");
                    }
                    fmt_expr(expr, context, &mut output);
                }
                output.push(')');
            }
        }
        output
    }
}

#[cfg(test)]
mod tests {
    use {
        super::FunctionExprPlan,
        crate::{
            ast::{DataType, DateTimeField, Literal},
            plan::{
                ExprPlan,
                explain::{Explain, ExplainContext},
            },
        },
    };

    #[test]
    fn explain() {
        let actual = FunctionExprPlan::Cast {
            expr: ExprPlan::Identifier("id".to_owned()),
            data_type: DataType::Text,
        };
        let expected = "CAST(id AS TEXT)";
        assert_eq!(actual.explain(&mut ExplainContext::default()), expected);

        let actual = FunctionExprPlan::Extract {
            field: DateTimeField::Year,
            expr: ExprPlan::Identifier("created_at".to_owned()),
        };
        let expected = "EXTRACT(YEAR FROM created_at)";
        assert_eq!(actual.explain(&mut ExplainContext::default()), expected);

        let actual = FunctionExprPlan::Custom {
            name: "score".to_owned(),
            exprs: vec![
                ExprPlan::Identifier("id".to_owned()),
                ExprPlan::Literal(Literal::Number(1.into())),
            ],
        };
        let expected = "score(id, 1)";
        assert_eq!(actual.explain(&mut ExplainContext::default()), expected);

        let actual = FunctionExprPlan::Abs(ExprPlan::Identifier("score".to_owned()));
        let expected = "ABS(score)";
        assert_eq!(actual.explain(&mut ExplainContext::default()), expected);

        let actual = FunctionExprPlan::Now();
        let expected = "NOW()";
        assert_eq!(actual.explain(&mut ExplainContext::default()), expected);

        let actual = FunctionExprPlan::Concat(vec![
            ExprPlan::Identifier("first_name".to_owned()),
            ExprPlan::Identifier("last_name".to_owned()),
        ]);
        let expected = "CONCAT(first_name, last_name)";
        assert_eq!(actual.explain(&mut ExplainContext::default()), expected);
    }
}
