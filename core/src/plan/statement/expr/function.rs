use {
    super::{ExprPlan, fmt_expr},
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
            Self::Abs(expr) => fmt_call("ABS", [expr], context, &mut output),
            Self::AddMonth { expr, size } => {
                fmt_call("ADD_MONTH", [expr, size], context, &mut output);
            }
            Self::Lower(expr) => fmt_call("LOWER", [expr], context, &mut output),
            Self::Initcap(expr) => fmt_call("INITCAP", [expr], context, &mut output),
            Self::Upper(expr) => fmt_call("UPPER", [expr], context, &mut output),
            Self::Left { expr, size } => fmt_call("LEFT", [expr, size], context, &mut output),
            Self::Right { expr, size } => fmt_call("RIGHT", [expr, size], context, &mut output),
            Self::Asin(expr) => fmt_call("ASIN", [expr], context, &mut output),
            Self::Acos(expr) => fmt_call("ACOS", [expr], context, &mut output),
            Self::Atan(expr) => fmt_call("ATAN", [expr], context, &mut output),
            Self::Lpad { expr, size, fill } => match fill {
                Some(fill) => fmt_call("LPAD", [expr, size, fill], context, &mut output),
                None => fmt_call("LPAD", [expr, size], context, &mut output),
            },
            Self::Rpad { expr, size, fill } => match fill {
                Some(fill) => fmt_call("RPAD", [expr, size, fill], context, &mut output),
                None => fmt_call("RPAD", [expr, size], context, &mut output),
            },
            Self::Replace { expr, old, new } => {
                fmt_call("REPLACE", [expr, old, new], context, &mut output);
            }
            Self::Cast { expr, data_type } => {
                output.push_str("CAST(");
                fmt_expr(expr, context, &mut output);
                output.push_str(" AS ");
                output.push_str(&data_type.to_string());
                output.push(')');
            }
            Self::Ceil(expr) => fmt_call("CEIL", [expr], context, &mut output),
            Self::Coalesce(exprs) => fmt_call("COALESCE", exprs, context, &mut output),
            Self::Concat(exprs) => fmt_call("CONCAT", exprs, context, &mut output),
            Self::ConcatWs { separator, exprs } => fmt_call(
                "CONCAT_WS",
                std::iter::once(separator).chain(exprs.iter()),
                context,
                &mut output,
            ),
            Self::Custom { name, exprs } => {
                fmt_call(name, exprs, context, &mut output);
            }
            Self::IfNull { expr, then } => {
                fmt_call("IF_NULL", [expr, then], context, &mut output);
            }
            Self::NullIf { expr1, expr2 } => {
                fmt_call("NULL_IF", [expr1, expr2], context, &mut output);
            }
            Self::Rand(expr) => match expr {
                Some(expr) => fmt_call("RAND", [expr], context, &mut output),
                None => fmt_call("RAND", std::iter::empty(), context, &mut output),
            },
            Self::Round(expr) => fmt_call("ROUND", [expr], context, &mut output),
            Self::Trunc(expr) => fmt_call("TRUNC", [expr], context, &mut output),
            Self::Floor(expr) => fmt_call("FLOOR", [expr], context, &mut output),
            Self::Trim {
                expr,
                filter_chars,
                trim_where_field,
            } => fmt_trim(
                expr,
                filter_chars.as_ref(),
                trim_where_field.as_ref(),
                context,
                &mut output,
            ),
            Self::Exp(expr) => fmt_call("EXP", [expr], context, &mut output),
            Self::Extract { field, expr } => {
                output.push_str("EXTRACT(");
                output.push_str(&field.to_string());
                output.push_str(" FROM ");
                fmt_expr(expr, context, &mut output);
                output.push(')');
            }
            Self::Ln(expr) => fmt_call("LN", [expr], context, &mut output),
            Self::Log { antilog, base } => {
                fmt_call("LOG", [antilog, base], context, &mut output);
            }
            Self::Log2(expr) => fmt_call("LOG2", [expr], context, &mut output),
            Self::Log10(expr) => fmt_call("LOG10", [expr], context, &mut output),
            Self::Div { dividend, divisor } => {
                fmt_call("DIV", [dividend, divisor], context, &mut output);
            }
            Self::Mod { dividend, divisor } => {
                fmt_call("MOD", [dividend, divisor], context, &mut output);
            }
            Self::Gcd { left, right } => fmt_call("GCD", [left, right], context, &mut output),
            Self::Lcm { left, right } => fmt_call("LCM", [left, right], context, &mut output),
            Self::Sin(expr) => fmt_call("SIN", [expr], context, &mut output),
            Self::Cos(expr) => fmt_call("COS", [expr], context, &mut output),
            Self::Tan(expr) => fmt_call("TAN", [expr], context, &mut output),
            Self::Sqrt(expr) => fmt_call("SQRT", [expr], context, &mut output),
            Self::Power { expr, power } => {
                fmt_call("POWER", [expr, power], context, &mut output);
            }
            Self::Radians(expr) => fmt_call("RADIANS", [expr], context, &mut output),
            Self::Degrees(expr) => fmt_call("DEGREES", [expr], context, &mut output),
            Self::Now() => fmt_call("NOW", std::iter::empty(), context, &mut output),
            Self::CurrentDate() => {
                fmt_call("CURRENT_DATE", std::iter::empty(), context, &mut output);
            }
            Self::CurrentTime() => {
                fmt_call("CURRENT_TIME", std::iter::empty(), context, &mut output);
            }
            Self::CurrentTimestamp() => fmt_call(
                "CURRENT_TIMESTAMP",
                std::iter::empty(),
                context,
                &mut output,
            ),
            Self::Pi() => fmt_call("PI", std::iter::empty(), context, &mut output),
            Self::LastDay(expr) => fmt_call("LAST_DAY", [expr], context, &mut output),
            Self::Ltrim { expr, chars } => match chars {
                Some(chars) => fmt_call("LTRIM", [expr, chars], context, &mut output),
                None => fmt_call("LTRIM", [expr], context, &mut output),
            },
            Self::Rtrim { expr, chars } => match chars {
                Some(chars) => fmt_call("RTRIM", [expr, chars], context, &mut output),
                None => fmt_call("RTRIM", [expr], context, &mut output),
            },
            Self::Reverse(expr) => fmt_call("REVERSE", [expr], context, &mut output),
            Self::Repeat { expr, num } => {
                fmt_call("REPEAT", [expr, num], context, &mut output);
            }
            Self::Sign(expr) => fmt_call("SIGN", [expr], context, &mut output),
            Self::Substr { expr, start, count } => match count {
                Some(count) => fmt_call("SUBSTR", [expr, start, count], context, &mut output),
                None => fmt_call("SUBSTR", [expr, start], context, &mut output),
            },
            Self::Unwrap { expr, selector } => {
                fmt_call("UNWRAP", [expr, selector], context, &mut output);
            }
            Self::GenerateUuid() => {
                fmt_call("GENERATE_UUID", std::iter::empty(), context, &mut output);
            }
            Self::Greatest(exprs) => fmt_call("GREATEST", exprs, context, &mut output),
            Self::Format { expr, format } => {
                fmt_call("FORMAT", [expr, format], context, &mut output);
            }
            Self::ToDate { expr, format } => {
                fmt_call("TO_DATE", [expr, format], context, &mut output);
            }
            Self::ToTimestamp { expr, format } => {
                fmt_call("TO_TIMESTAMP", [expr, format], context, &mut output);
            }
            Self::ToTime { expr, format } => {
                fmt_call("TO_TIME", [expr, format], context, &mut output);
            }
            Self::Position {
                from_expr,
                sub_expr,
            } => fmt_call("POSITION", [sub_expr, from_expr], context, &mut output),
            Self::FindIdx {
                from_expr,
                sub_expr,
                start,
            } => match start {
                Some(start) => fmt_call(
                    "FIND_IDX",
                    [from_expr, sub_expr, start],
                    context,
                    &mut output,
                ),
                None => fmt_call("FIND_IDX", [from_expr, sub_expr], context, &mut output),
            },
            Self::Ascii(expr) => fmt_call("ASCII", [expr], context, &mut output),
            Self::Chr(expr) => fmt_call("CHR", [expr], context, &mut output),
            Self::Md5(expr) => fmt_call("MD5", [expr], context, &mut output),
            Self::Hex(expr) => fmt_call("HEX", [expr], context, &mut output),
            Self::Append { expr, value } => {
                fmt_call("APPEND", [expr, value], context, &mut output);
            }
            Self::Sort { expr, order } => match order {
                Some(order) => fmt_call("SORT", [expr, order], context, &mut output),
                None => fmt_call("SORT", [expr], context, &mut output),
            },
            Self::Slice {
                expr,
                start,
                length,
            } => fmt_call("SLICE", [expr, start, length], context, &mut output),
            Self::Prepend { expr, value } => {
                fmt_call("PREPEND", [expr, value], context, &mut output);
            }
            Self::Skip { expr, size } => {
                fmt_call("SKIP", [expr, size], context, &mut output);
            }
            Self::Take { expr, size } => {
                fmt_call("TAKE", [expr, size], context, &mut output);
            }
            Self::GetX(expr) => fmt_call("GET_X", [expr], context, &mut output),
            Self::GetY(expr) => fmt_call("GET_Y", [expr], context, &mut output),
            Self::Point { x, y } => fmt_call("POINT", [x, y], context, &mut output),
            Self::CalcDistance {
                geometry1,
                geometry2,
            } => fmt_call(
                "CALC_DISTANCE",
                [geometry1, geometry2],
                context,
                &mut output,
            ),
            Self::IsEmpty(expr) => fmt_call("IS_EMPTY", [expr], context, &mut output),
            Self::Length(expr) => fmt_call("LENGTH", [expr], context, &mut output),
            Self::Entries(expr) => fmt_call("ENTRIES", [expr], context, &mut output),
            Self::Keys(expr) => fmt_call("KEYS", [expr], context, &mut output),
            Self::Values(expr) => fmt_call("VALUES", [expr], context, &mut output),
            Self::Splice {
                list_data,
                begin_index,
                end_index,
                values,
            } => match values {
                Some(values) => fmt_call(
                    "SPLICE",
                    [list_data, begin_index, end_index, values],
                    context,
                    &mut output,
                ),
                None => fmt_call(
                    "SPLICE",
                    [list_data, begin_index, end_index],
                    context,
                    &mut output,
                ),
            },
            Self::Dedup(expr) => fmt_call("DEDUP", [expr], context, &mut output),
        }
        output
    }
}

fn fmt_call<'a>(
    name: &str,
    exprs: impl IntoIterator<Item = &'a ExprPlan>,
    context: &mut ExplainContext,
    output: &mut String,
) {
    output.push_str(name);
    output.push('(');
    for (index, expr) in exprs.into_iter().enumerate() {
        if index > 0 {
            output.push_str(", ");
        }
        fmt_expr(expr, context, output);
    }
    output.push(')');
}

fn fmt_trim(
    expr: &ExprPlan,
    filter_chars: Option<&ExprPlan>,
    trim_where_field: Option<&TrimWhereField>,
    context: &mut ExplainContext,
    output: &mut String,
) {
    output.push_str("TRIM(");
    if let Some(trim_where_field) = trim_where_field {
        output.push_str(&trim_where_field.to_string());
        output.push(' ');
    }
    if let Some(filter_chars) = filter_chars {
        fmt_expr(filter_chars, context, output);
        output.push_str(" FROM ");
    }
    fmt_expr(expr, context, output);
    output.push(')');
}

#[cfg(test)]
mod tests {
    use {
        super::FunctionExprPlan,
        crate::{
            ast::{DataType, DateTimeField, TrimWhereField},
            plan::{
                ExprPlan,
                explain::{Explain, ExplainContext},
            },
        },
    };

    fn test(actual: &FunctionExprPlan, expected: &str) {
        assert_eq!(actual.explain(&mut ExplainContext::default()), expected);
    }

    #[test]
    fn explain() {
        let actual = FunctionExprPlan::Abs(ExprPlan::Identifier("value".to_owned()));
        let expected = "ABS(value)";
        test(&actual, expected);

        let actual = FunctionExprPlan::AddMonth {
            expr: ExprPlan::Identifier("date".to_owned()),
            size: ExprPlan::Identifier("size".to_owned()),
        };
        let expected = "ADD_MONTH(date, size)";
        test(&actual, expected);

        let actual = FunctionExprPlan::Lower(ExprPlan::Identifier("value".to_owned()));
        let expected = "LOWER(value)";
        test(&actual, expected);

        let actual = FunctionExprPlan::Initcap(ExprPlan::Identifier("value".to_owned()));
        let expected = "INITCAP(value)";
        test(&actual, expected);

        let actual = FunctionExprPlan::Upper(ExprPlan::Identifier("value".to_owned()));
        let expected = "UPPER(value)";
        test(&actual, expected);

        let actual = FunctionExprPlan::Left {
            expr: ExprPlan::Identifier("value".to_owned()),
            size: ExprPlan::Identifier("size".to_owned()),
        };
        let expected = "LEFT(value, size)";
        test(&actual, expected);

        let actual = FunctionExprPlan::Right {
            expr: ExprPlan::Identifier("value".to_owned()),
            size: ExprPlan::Identifier("size".to_owned()),
        };
        let expected = "RIGHT(value, size)";
        test(&actual, expected);

        let actual = FunctionExprPlan::Asin(ExprPlan::Identifier("value".to_owned()));
        let expected = "ASIN(value)";
        test(&actual, expected);

        let actual = FunctionExprPlan::Acos(ExprPlan::Identifier("value".to_owned()));
        let expected = "ACOS(value)";
        test(&actual, expected);

        let actual = FunctionExprPlan::Atan(ExprPlan::Identifier("value".to_owned()));
        let expected = "ATAN(value)";
        test(&actual, expected);

        let actual = FunctionExprPlan::Lpad {
            expr: ExprPlan::Identifier("value".to_owned()),
            size: ExprPlan::Identifier("size".to_owned()),
            fill: None,
        };
        let expected = "LPAD(value, size)";
        test(&actual, expected);

        let actual = FunctionExprPlan::Lpad {
            expr: ExprPlan::Identifier("value".to_owned()),
            size: ExprPlan::Identifier("size".to_owned()),
            fill: Some(ExprPlan::Identifier("fill".to_owned())),
        };
        let expected = "LPAD(value, size, fill)";
        test(&actual, expected);

        let actual = FunctionExprPlan::Rpad {
            expr: ExprPlan::Identifier("value".to_owned()),
            size: ExprPlan::Identifier("size".to_owned()),
            fill: None,
        };
        let expected = "RPAD(value, size)";
        test(&actual, expected);

        let actual = FunctionExprPlan::Rpad {
            expr: ExprPlan::Identifier("value".to_owned()),
            size: ExprPlan::Identifier("size".to_owned()),
            fill: Some(ExprPlan::Identifier("fill".to_owned())),
        };
        let expected = "RPAD(value, size, fill)";
        test(&actual, expected);

        let actual = FunctionExprPlan::Replace {
            expr: ExprPlan::Identifier("value".to_owned()),
            old: ExprPlan::Identifier("old".to_owned()),
            new: ExprPlan::Identifier("new".to_owned()),
        };
        let expected = "REPLACE(value, old, new)";
        test(&actual, expected);

        let actual = FunctionExprPlan::Cast {
            expr: ExprPlan::Identifier("value".to_owned()),
            data_type: DataType::Text,
        };
        let expected = "CAST(value AS TEXT)";
        test(&actual, expected);

        let actual = FunctionExprPlan::Ceil(ExprPlan::Identifier("value".to_owned()));
        let expected = "CEIL(value)";
        test(&actual, expected);

        let actual = FunctionExprPlan::Coalesce(vec![
            ExprPlan::Identifier("left".to_owned()),
            ExprPlan::Identifier("right".to_owned()),
        ]);
        let expected = "COALESCE(left, right)";
        test(&actual, expected);

        let actual = FunctionExprPlan::Concat(vec![
            ExprPlan::Identifier("left".to_owned()),
            ExprPlan::Identifier("right".to_owned()),
        ]);
        let expected = "CONCAT(left, right)";
        test(&actual, expected);

        let actual = FunctionExprPlan::ConcatWs {
            separator: ExprPlan::Identifier("separator".to_owned()),
            exprs: vec![
                ExprPlan::Identifier("left".to_owned()),
                ExprPlan::Identifier("right".to_owned()),
            ],
        };
        let expected = "CONCAT_WS(separator, left, right)";
        test(&actual, expected);

        let actual = FunctionExprPlan::Custom {
            name: "custom_function".to_owned(),
            exprs: vec![
                ExprPlan::Identifier("left".to_owned()),
                ExprPlan::Identifier("right".to_owned()),
            ],
        };
        let expected = "custom_function(left, right)";
        test(&actual, expected);

        let actual = FunctionExprPlan::IfNull {
            expr: ExprPlan::Identifier("value".to_owned()),
            then: ExprPlan::Identifier("fallback".to_owned()),
        };
        let expected = "IF_NULL(value, fallback)";
        test(&actual, expected);

        let actual = FunctionExprPlan::NullIf {
            expr1: ExprPlan::Identifier("left".to_owned()),
            expr2: ExprPlan::Identifier("right".to_owned()),
        };
        let expected = "NULL_IF(left, right)";
        test(&actual, expected);

        let actual = FunctionExprPlan::Rand(None);
        let expected = "RAND()";
        test(&actual, expected);

        let actual = FunctionExprPlan::Rand(Some(ExprPlan::Identifier("seed".to_owned())));
        let expected = "RAND(seed)";
        test(&actual, expected);

        let actual = FunctionExprPlan::Round(ExprPlan::Identifier("value".to_owned()));
        let expected = "ROUND(value)";
        test(&actual, expected);

        let actual = FunctionExprPlan::Trunc(ExprPlan::Identifier("value".to_owned()));
        let expected = "TRUNC(value)";
        test(&actual, expected);

        let actual = FunctionExprPlan::Floor(ExprPlan::Identifier("value".to_owned()));
        let expected = "FLOOR(value)";
        test(&actual, expected);

        let actual = FunctionExprPlan::Trim {
            expr: ExprPlan::Identifier("value".to_owned()),
            filter_chars: None,
            trim_where_field: None,
        };
        let expected = "TRIM(value)";
        test(&actual, expected);

        let actual = FunctionExprPlan::Trim {
            expr: ExprPlan::Identifier("value".to_owned()),
            filter_chars: Some(ExprPlan::Identifier("chars".to_owned())),
            trim_where_field: None,
        };
        let expected = "TRIM(chars FROM value)";
        test(&actual, expected);

        let actual = FunctionExprPlan::Trim {
            expr: ExprPlan::Identifier("value".to_owned()),
            filter_chars: Some(ExprPlan::Identifier("chars".to_owned())),
            trim_where_field: Some(TrimWhereField::Both),
        };
        let expected = "TRIM(BOTH chars FROM value)";
        test(&actual, expected);

        let actual = FunctionExprPlan::Trim {
            expr: ExprPlan::Identifier("value".to_owned()),
            filter_chars: Some(ExprPlan::Identifier("chars".to_owned())),
            trim_where_field: Some(TrimWhereField::Leading),
        };
        let expected = "TRIM(LEADING chars FROM value)";
        test(&actual, expected);

        let actual = FunctionExprPlan::Trim {
            expr: ExprPlan::Identifier("value".to_owned()),
            filter_chars: Some(ExprPlan::Identifier("chars".to_owned())),
            trim_where_field: Some(TrimWhereField::Trailing),
        };
        let expected = "TRIM(TRAILING chars FROM value)";
        test(&actual, expected);

        let actual = FunctionExprPlan::Trim {
            expr: ExprPlan::Identifier("value".to_owned()),
            filter_chars: None,
            trim_where_field: Some(TrimWhereField::Leading),
        };
        let expected = "TRIM(LEADING value)";
        test(&actual, expected);

        let actual = FunctionExprPlan::Exp(ExprPlan::Identifier("value".to_owned()));
        let expected = "EXP(value)";
        test(&actual, expected);

        let actual = FunctionExprPlan::Extract {
            field: DateTimeField::Year,
            expr: ExprPlan::Identifier("created_at".to_owned()),
        };
        let expected = "EXTRACT(YEAR FROM created_at)";
        test(&actual, expected);

        let actual = FunctionExprPlan::Ln(ExprPlan::Identifier("value".to_owned()));
        let expected = "LN(value)";
        test(&actual, expected);

        let actual = FunctionExprPlan::Log {
            antilog: ExprPlan::Identifier("antilog".to_owned()),
            base: ExprPlan::Identifier("base".to_owned()),
        };
        let expected = "LOG(antilog, base)";
        test(&actual, expected);

        let actual = FunctionExprPlan::Log2(ExprPlan::Identifier("value".to_owned()));
        let expected = "LOG2(value)";
        test(&actual, expected);

        let actual = FunctionExprPlan::Log10(ExprPlan::Identifier("value".to_owned()));
        let expected = "LOG10(value)";
        test(&actual, expected);

        let actual = FunctionExprPlan::Div {
            dividend: ExprPlan::Identifier("dividend".to_owned()),
            divisor: ExprPlan::Identifier("divisor".to_owned()),
        };
        let expected = "DIV(dividend, divisor)";
        test(&actual, expected);

        let actual = FunctionExprPlan::Mod {
            dividend: ExprPlan::Identifier("dividend".to_owned()),
            divisor: ExprPlan::Identifier("divisor".to_owned()),
        };
        let expected = "MOD(dividend, divisor)";
        test(&actual, expected);

        let actual = FunctionExprPlan::Gcd {
            left: ExprPlan::Identifier("left".to_owned()),
            right: ExprPlan::Identifier("right".to_owned()),
        };
        let expected = "GCD(left, right)";
        test(&actual, expected);

        let actual = FunctionExprPlan::Lcm {
            left: ExprPlan::Identifier("left".to_owned()),
            right: ExprPlan::Identifier("right".to_owned()),
        };
        let expected = "LCM(left, right)";
        test(&actual, expected);

        let actual = FunctionExprPlan::Sin(ExprPlan::Identifier("value".to_owned()));
        let expected = "SIN(value)";
        test(&actual, expected);

        let actual = FunctionExprPlan::Cos(ExprPlan::Identifier("value".to_owned()));
        let expected = "COS(value)";
        test(&actual, expected);

        let actual = FunctionExprPlan::Tan(ExprPlan::Identifier("value".to_owned()));
        let expected = "TAN(value)";
        test(&actual, expected);

        let actual = FunctionExprPlan::Sqrt(ExprPlan::Identifier("value".to_owned()));
        let expected = "SQRT(value)";
        test(&actual, expected);

        let actual = FunctionExprPlan::Power {
            expr: ExprPlan::Identifier("value".to_owned()),
            power: ExprPlan::Identifier("power".to_owned()),
        };
        let expected = "POWER(value, power)";
        test(&actual, expected);

        let actual = FunctionExprPlan::Radians(ExprPlan::Identifier("value".to_owned()));
        let expected = "RADIANS(value)";
        test(&actual, expected);

        let actual = FunctionExprPlan::Degrees(ExprPlan::Identifier("value".to_owned()));
        let expected = "DEGREES(value)";
        test(&actual, expected);

        let actual = FunctionExprPlan::Now();
        let expected = "NOW()";
        test(&actual, expected);

        let actual = FunctionExprPlan::CurrentDate();
        let expected = "CURRENT_DATE()";
        test(&actual, expected);

        let actual = FunctionExprPlan::CurrentTime();
        let expected = "CURRENT_TIME()";
        test(&actual, expected);

        let actual = FunctionExprPlan::CurrentTimestamp();
        let expected = "CURRENT_TIMESTAMP()";
        test(&actual, expected);

        let actual = FunctionExprPlan::Pi();
        let expected = "PI()";
        test(&actual, expected);

        let actual = FunctionExprPlan::LastDay(ExprPlan::Identifier("date".to_owned()));
        let expected = "LAST_DAY(date)";
        test(&actual, expected);

        let actual = FunctionExprPlan::Ltrim {
            expr: ExprPlan::Identifier("value".to_owned()),
            chars: None,
        };
        let expected = "LTRIM(value)";
        test(&actual, expected);

        let actual = FunctionExprPlan::Ltrim {
            expr: ExprPlan::Identifier("value".to_owned()),
            chars: Some(ExprPlan::Identifier("chars".to_owned())),
        };
        let expected = "LTRIM(value, chars)";
        test(&actual, expected);

        let actual = FunctionExprPlan::Rtrim {
            expr: ExprPlan::Identifier("value".to_owned()),
            chars: None,
        };
        let expected = "RTRIM(value)";
        test(&actual, expected);

        let actual = FunctionExprPlan::Rtrim {
            expr: ExprPlan::Identifier("value".to_owned()),
            chars: Some(ExprPlan::Identifier("chars".to_owned())),
        };
        let expected = "RTRIM(value, chars)";
        test(&actual, expected);

        let actual = FunctionExprPlan::Reverse(ExprPlan::Identifier("value".to_owned()));
        let expected = "REVERSE(value)";
        test(&actual, expected);

        let actual = FunctionExprPlan::Repeat {
            expr: ExprPlan::Identifier("value".to_owned()),
            num: ExprPlan::Identifier("count".to_owned()),
        };
        let expected = "REPEAT(value, count)";
        test(&actual, expected);

        let actual = FunctionExprPlan::Sign(ExprPlan::Identifier("value".to_owned()));
        let expected = "SIGN(value)";
        test(&actual, expected);

        let actual = FunctionExprPlan::Substr {
            expr: ExprPlan::Identifier("value".to_owned()),
            start: ExprPlan::Identifier("start".to_owned()),
            count: None,
        };
        let expected = "SUBSTR(value, start)";
        test(&actual, expected);

        let actual = FunctionExprPlan::Substr {
            expr: ExprPlan::Identifier("value".to_owned()),
            start: ExprPlan::Identifier("start".to_owned()),
            count: Some(ExprPlan::Identifier("count".to_owned())),
        };
        let expected = "SUBSTR(value, start, count)";
        test(&actual, expected);

        let actual = FunctionExprPlan::Unwrap {
            expr: ExprPlan::Identifier("value".to_owned()),
            selector: ExprPlan::Identifier("selector".to_owned()),
        };
        let expected = "UNWRAP(value, selector)";
        test(&actual, expected);

        let actual = FunctionExprPlan::GenerateUuid();
        let expected = "GENERATE_UUID()";
        test(&actual, expected);

        let actual = FunctionExprPlan::Greatest(vec![
            ExprPlan::Identifier("left".to_owned()),
            ExprPlan::Identifier("right".to_owned()),
        ]);
        let expected = "GREATEST(left, right)";
        test(&actual, expected);

        let actual = FunctionExprPlan::Format {
            expr: ExprPlan::Identifier("value".to_owned()),
            format: ExprPlan::Identifier("format".to_owned()),
        };
        let expected = "FORMAT(value, format)";
        test(&actual, expected);

        let actual = FunctionExprPlan::ToDate {
            expr: ExprPlan::Identifier("value".to_owned()),
            format: ExprPlan::Identifier("format".to_owned()),
        };
        let expected = "TO_DATE(value, format)";
        test(&actual, expected);

        let actual = FunctionExprPlan::ToTimestamp {
            expr: ExprPlan::Identifier("value".to_owned()),
            format: ExprPlan::Identifier("format".to_owned()),
        };
        let expected = "TO_TIMESTAMP(value, format)";
        test(&actual, expected);

        let actual = FunctionExprPlan::ToTime {
            expr: ExprPlan::Identifier("value".to_owned()),
            format: ExprPlan::Identifier("format".to_owned()),
        };
        let expected = "TO_TIME(value, format)";
        test(&actual, expected);

        let actual = FunctionExprPlan::Position {
            from_expr: ExprPlan::Identifier("value".to_owned()),
            sub_expr: ExprPlan::Identifier("substring".to_owned()),
        };
        let expected = "POSITION(substring, value)";
        test(&actual, expected);

        let actual = FunctionExprPlan::FindIdx {
            from_expr: ExprPlan::Identifier("value".to_owned()),
            sub_expr: ExprPlan::Identifier("substring".to_owned()),
            start: None,
        };
        let expected = "FIND_IDX(value, substring)";
        test(&actual, expected);

        let actual = FunctionExprPlan::FindIdx {
            from_expr: ExprPlan::Identifier("value".to_owned()),
            sub_expr: ExprPlan::Identifier("substring".to_owned()),
            start: Some(ExprPlan::Identifier("start".to_owned())),
        };
        let expected = "FIND_IDX(value, substring, start)";
        test(&actual, expected);

        let actual = FunctionExprPlan::Ascii(ExprPlan::Identifier("value".to_owned()));
        let expected = "ASCII(value)";
        test(&actual, expected);

        let actual = FunctionExprPlan::Chr(ExprPlan::Identifier("value".to_owned()));
        let expected = "CHR(value)";
        test(&actual, expected);

        let actual = FunctionExprPlan::Md5(ExprPlan::Identifier("value".to_owned()));
        let expected = "MD5(value)";
        test(&actual, expected);

        let actual = FunctionExprPlan::Hex(ExprPlan::Identifier("value".to_owned()));
        let expected = "HEX(value)";
        test(&actual, expected);

        let actual = FunctionExprPlan::Append {
            expr: ExprPlan::Identifier("list".to_owned()),
            value: ExprPlan::Identifier("value".to_owned()),
        };
        let expected = "APPEND(list, value)";
        test(&actual, expected);

        let actual = FunctionExprPlan::Sort {
            expr: ExprPlan::Identifier("list".to_owned()),
            order: None,
        };
        let expected = "SORT(list)";
        test(&actual, expected);

        let actual = FunctionExprPlan::Sort {
            expr: ExprPlan::Identifier("list".to_owned()),
            order: Some(ExprPlan::Identifier("order".to_owned())),
        };
        let expected = "SORT(list, order)";
        test(&actual, expected);

        let actual = FunctionExprPlan::Slice {
            expr: ExprPlan::Identifier("list".to_owned()),
            start: ExprPlan::Identifier("start".to_owned()),
            length: ExprPlan::Identifier("length".to_owned()),
        };
        let expected = "SLICE(list, start, length)";
        test(&actual, expected);

        let actual = FunctionExprPlan::Prepend {
            expr: ExprPlan::Identifier("list".to_owned()),
            value: ExprPlan::Identifier("value".to_owned()),
        };
        let expected = "PREPEND(list, value)";
        test(&actual, expected);

        let actual = FunctionExprPlan::Skip {
            expr: ExprPlan::Identifier("list".to_owned()),
            size: ExprPlan::Identifier("size".to_owned()),
        };
        let expected = "SKIP(list, size)";
        test(&actual, expected);

        let actual = FunctionExprPlan::Take {
            expr: ExprPlan::Identifier("list".to_owned()),
            size: ExprPlan::Identifier("size".to_owned()),
        };
        let expected = "TAKE(list, size)";
        test(&actual, expected);

        let actual = FunctionExprPlan::GetX(ExprPlan::Identifier("point".to_owned()));
        let expected = "GET_X(point)";
        test(&actual, expected);

        let actual = FunctionExprPlan::GetY(ExprPlan::Identifier("point".to_owned()));
        let expected = "GET_Y(point)";
        test(&actual, expected);

        let actual = FunctionExprPlan::Point {
            x: ExprPlan::Identifier("x".to_owned()),
            y: ExprPlan::Identifier("y".to_owned()),
        };
        let expected = "POINT(x, y)";
        test(&actual, expected);

        let actual = FunctionExprPlan::CalcDistance {
            geometry1: ExprPlan::Identifier("left".to_owned()),
            geometry2: ExprPlan::Identifier("right".to_owned()),
        };
        let expected = "CALC_DISTANCE(left, right)";
        test(&actual, expected);

        let actual = FunctionExprPlan::IsEmpty(ExprPlan::Identifier("value".to_owned()));
        let expected = "IS_EMPTY(value)";
        test(&actual, expected);

        let actual = FunctionExprPlan::Length(ExprPlan::Identifier("value".to_owned()));
        let expected = "LENGTH(value)";
        test(&actual, expected);

        let actual = FunctionExprPlan::Entries(ExprPlan::Identifier("map".to_owned()));
        let expected = "ENTRIES(map)";
        test(&actual, expected);

        let actual = FunctionExprPlan::Keys(ExprPlan::Identifier("map".to_owned()));
        let expected = "KEYS(map)";
        test(&actual, expected);

        let actual = FunctionExprPlan::Values(ExprPlan::Identifier("map".to_owned()));
        let expected = "VALUES(map)";
        test(&actual, expected);

        let actual = FunctionExprPlan::Splice {
            list_data: ExprPlan::Identifier("list".to_owned()),
            begin_index: ExprPlan::Identifier("begin".to_owned()),
            end_index: ExprPlan::Identifier("end".to_owned()),
            values: None,
        };
        let expected = "SPLICE(list, begin, end)";
        test(&actual, expected);

        let actual = FunctionExprPlan::Splice {
            list_data: ExprPlan::Identifier("list".to_owned()),
            begin_index: ExprPlan::Identifier("begin".to_owned()),
            end_index: ExprPlan::Identifier("end".to_owned()),
            values: Some(ExprPlan::Identifier("values".to_owned())),
        };
        let expected = "SPLICE(list, begin, end, values)";
        test(&actual, expected);

        let actual = FunctionExprPlan::Dedup(ExprPlan::Identifier("list".to_owned()));
        let expected = "DEDUP(list)";
        test(&actual, expected);
    }
}
