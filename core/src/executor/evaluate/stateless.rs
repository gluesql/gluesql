use {
    super::{expr, function, EvaluateError, Evaluated},
    crate::{
        ast::{Expr, Function},
        data::{Interval, Literal, Row, Value},
        result::Result,
        store::DataRow,
    },
    chrono::prelude::Utc,
    std::{borrow::Cow, collections::HashMap},
};

#[derive(Clone)]
pub enum Context<'a> {
    Vec {
        columns: &'a [String],
        values: &'a [Value],
    },
    Map(&'a HashMap<String, Value>),
    None,
}

impl<'a> From<(&'a [String], &'a DataRow)> for Context<'a> {
    fn from((columns, data_row): (&'a [String], &'a DataRow)) -> Self {
        match data_row {
            DataRow::Vec(values) => Self::Vec { columns, values },
            DataRow::Map(values) => Self::Map(values),
        }
    }
}

impl<'a> From<Option<&'a Row>> for Context<'a> {
    fn from(row: Option<&'a Row>) -> Self {
        match row {
            Some(Row::Vec { columns, values }) => Context::Vec { columns, values },
            Some(Row::Map(values)) => Context::Map(values),
            None => Self::None,
        }
    }
}

impl<'a> Context<'a> {
    fn get_value(&'a self, target: &str) -> Option<&'a Value> {
        match self {
            Context::Vec { columns, values } => columns
                .as_ref()
                .iter()
                .position(|column| column == target)
                .and_then(|index| values.get(index)),
            Context::Map(values) => values.get(target),
            Context::None => None,
        }
    }
}

pub fn evaluate_stateless<'a, 'b, T: Into<Context<'b>>>(
    context: T,
    expr: &'a Expr,
) -> Result<Evaluated<'a>> {
    evaluate(&context.into(), expr)
}

fn evaluate<'a>(context: &Context<'_>, expr: &'a Expr) -> Result<Evaluated<'a>> {
    let eval = |expr| evaluate(context, expr);

    match expr {
        Expr::Literal(ast_literal) => expr::literal(ast_literal),
        Expr::TypedString { data_type, value } => {
            expr::typed_string(data_type, Cow::Borrowed(value))
        }
        Expr::Identifier(ident) => match context.get_value(ident) {
            Some(value) => Ok(value.clone()),
            None => Err(EvaluateError::ValueNotFound(ident.to_owned()).into()),
        }
        .map(Evaluated::from),
        Expr::Nested(expr) => eval(expr),
        Expr::BinaryOp { op, left, right } => {
            let left = eval(left)?;
            let right = eval(right)?;

            expr::binary_op(op, left, right)
        }
        Expr::UnaryOp { op, expr } => {
            let v = eval(expr)?;

            expr::unary_op(op, v)
        }
        Expr::InList {
            expr,
            list,
            negated,
        } => {
            let negated = *negated;
            let target = eval(expr)?;

            list.iter()
                .filter_map(|expr| {
                    let target = &target;

                    eval(expr).map_or_else(
                        |error| Some(Err(error)),
                        |evaluated| (target == &evaluated).then_some(Ok(!negated)),
                    )
                })
                .take(1)
                .collect::<Vec<_>>()
                .into_iter()
                .next()
                .unwrap_or(Ok(negated))
                .map(Value::Bool)
                .map(Evaluated::from)
        }
        Expr::Between {
            expr,
            negated,
            low,
            high,
        } => {
            let target = eval(expr)?;
            let low = eval(low)?;
            let high = eval(high)?;

            expr::between(target, *negated, low, high)
        }
        Expr::Like {
            expr,
            negated,
            pattern,
        } => {
            let target = eval(expr)?;
            let pattern = eval(pattern)?;
            let evaluated = target.like(pattern, true)?;

            Ok(match negated {
                true => Evaluated::from(Value::Bool(
                    evaluated == Evaluated::Literal(Literal::Boolean(false)),
                )),
                false => evaluated,
            })
        }
        Expr::ILike {
            expr,
            negated,
            pattern,
        } => {
            let target = eval(expr)?;
            let pattern = eval(pattern)?;
            let evaluated = target.like(pattern, false)?;

            Ok(match negated {
                true => Evaluated::from(Value::Bool(
                    evaluated == Evaluated::Literal(Literal::Boolean(false)),
                )),
                false => evaluated,
            })
        }
        Expr::IsNull(expr) => {
            let v = eval(expr)?.is_null();

            Ok(Evaluated::from(Value::Bool(v)))
        }
        Expr::IsNotNull(expr) => {
            let v = eval(expr)?.is_null();

            Ok(Evaluated::from(Value::Bool(!v)))
        }
        Expr::ArrayIndex { obj, indexes } => {
            let obj = eval(obj)?;
            let indexes = indexes.iter().map(eval).collect::<Result<Vec<_>>>()?;
            expr::array_index(obj, indexes)
        }
        Expr::Interval {
            expr,
            leading_field,
            last_field,
        } => {
            let value = eval(expr).and_then(Value::try_from).map(String::from)?;

            Interval::try_from_literal(&value, *leading_field, *last_field)
                .map(Value::Interval)
                .map(Evaluated::from)
        }

        Expr::Function(func) => evaluate_function(context, func),
        _ => Err(EvaluateError::UnsupportedStatelessExpr(expr.clone()).into()),
    }
}

fn evaluate_function<'a>(context: &Context<'_>, func: &'a Function) -> Result<Evaluated<'a>> {
    use function as f;

    let name = func.to_string();
    let eval = |expr| evaluate(context, expr);
    let eval_opt = |expr| -> Result<Option<_>> {
        match expr {
            Some(v) => Ok(Some(eval(v)?)),
            None => Ok(None),
        }
    };

    match func {
        // --- text ---
        #[cfg(feature = "function")]
        Function::Custom { name: _, exprs: _ } => {
            Err(EvaluateError::UnsupportedCustomFunction.into())
        }
        Function::Concat(exprs) => {
            let exprs = exprs.iter().map(eval).collect::<Result<_>>()?;

            f::concat(exprs)
        }
        Function::Lower(expr) => f::lower(name, eval(expr)?),
        Function::Upper(expr) => f::upper(name, eval(expr)?),
        Function::Left { expr, size } | Function::Right { expr, size } => {
            let expr = eval(expr)?;
            let size = eval(size)?;

            f::left_or_right(name, expr, size)
        }
        Function::Lpad { expr, size, fill } | Function::Rpad { expr, size, fill } => {
            let expr = eval(expr)?;
            let size = eval(size)?;
            let fill = eval_opt(fill.as_ref())?;

            f::lpad_or_rpad(name, expr, size, fill)
        }
        Function::Trim {
            expr,
            filter_chars,
            trim_where_field,
        } => {
            let expr = eval(expr)?;
            let filter_chars = eval_opt(filter_chars.as_ref())?;

            expr.trim(name, filter_chars, trim_where_field)
        }
        Function::Ltrim { expr, chars } => {
            let expr = eval(expr)?;
            let chars = eval_opt(chars.as_ref())?;

            expr.ltrim(name, chars)
        }
        Function::Rtrim { expr, chars } => {
            let expr = eval(expr)?;
            let chars = eval_opt(chars.as_ref())?;

            expr.rtrim(name, chars)
        }
        Function::Reverse(expr) => {
            let expr = eval(expr)?;

            f::reverse(name, expr)
        }
        Function::Repeat { expr, num } => {
            let expr = eval(expr)?;
            let num = eval(num)?;

            f::repeat(name, expr, num)
        }
        Function::Substr { expr, start, count } => {
            let expr = eval(expr)?;
            let start = eval(start)?;
            let count = eval_opt(count.as_ref())?;

            expr.substr(name, start, count)
        }
        Function::Ascii(expr) => f::ascii(name, eval(expr)?),
        Function::Chr(expr) => f::chr(name, eval(expr)?),

        // --- float ---
        Function::Sqrt(expr) => f::sqrt(eval(expr)?),
        Function::Power { expr, power } => {
            let expr = eval(expr)?;
            let power = eval(power)?;

            f::power(name, expr, power)
        }
        Function::Abs(expr) => f::abs(name, eval(expr)?),
        Function::IfNull { expr, then } => f::ifnull(eval(expr)?, eval(then)?),
        Function::Sign(expr) => f::sign(name, eval(expr)?),
        Function::Ceil(expr) => f::ceil(name, eval(expr)?),
        Function::Rand(expr) => f::rand(name, eval_opt(expr.as_ref())?),
        Function::Round(expr) => f::round(name, eval(expr)?),
        Function::Floor(expr) => f::floor(name, eval(expr)?),
        Function::Radians(expr) => f::radians(name, eval(expr)?),
        Function::Degrees(expr) => f::degrees(name, eval(expr)?),
        Function::Pi() => Ok(Evaluated::from(Value::F64(std::f64::consts::PI))),
        Function::Exp(expr) => f::exp(name, eval(expr)?),
        Function::Log { antilog, base } => {
            let antilog = eval(antilog)?;
            let base = eval(base)?;

            f::log(name, antilog, base)
        }
        Function::Ln(expr) => f::ln(name, eval(expr)?),
        Function::Log2(expr) => f::log2(name, eval(expr)?),
        Function::Log10(expr) => f::log10(name, eval(expr)?),
        Function::Sin(expr) => f::sin(name, eval(expr)?),
        Function::Cos(expr) => f::cos(name, eval(expr)?),
        Function::Tan(expr) => f::tan(name, eval(expr)?),
        Function::Asin(expr) => f::asin(name, eval(expr)?),
        Function::Acos(expr) => f::acos(name, eval(expr)?),
        Function::Atan(expr) => f::atan(name, eval(expr)?),

        // --- integer ---
        Function::Div { dividend, divisor } => {
            let dividend = eval(dividend)?;
            let divisor = eval(divisor)?;

            f::div(name, dividend, divisor)
        }
        Function::Mod { dividend, divisor } => {
            let dividend = eval(dividend)?;
            let divisor = eval(divisor)?;

            dividend.modulo(&divisor)
        }
        Function::Gcd { left, right } => {
            let left = eval(left)?;
            let right = eval(right)?;

            f::gcd(name, left, right)
        }
        Function::Lcm { left, right } => {
            let left = eval(left)?;
            let right = eval(right)?;

            f::lcm(name, left, right)
        }

        // --- etc ---
        Function::Unwrap { expr, selector } => {
            let expr = eval(expr)?;
            let selector = eval(selector)?;

            f::unwrap(name, expr, selector)
        }
        Function::GenerateUuid() => Ok(f::generate_uuid()),
        Function::Now() => Ok(Evaluated::from(Value::Timestamp(Utc::now().naive_utc()))),
        Function::Format { expr, format } => {
            let expr = eval(expr)?;
            let format = eval(format)?;

            f::format(name, expr, format)
        }

        Function::ToDate { expr, format } => {
            let expr = eval(expr)?;
            let format = eval(format)?;

            f::to_date(name, expr, format)
        }

        Function::ToTimestamp { expr, format } => {
            let expr = eval(expr)?;
            let format = eval(format)?;

            f::to_timestamp(name, expr, format)
        }
        Function::ToTime { expr, format } => {
            let expr = eval(expr)?;
            let format = eval(format)?;
            f::to_time(name, expr, format)
        }
        Function::Position {
            from_expr,
            sub_expr,
        } => {
            let from_expr = eval(from_expr)?;
            let sub_expr = eval(sub_expr)?;
            f::position(from_expr, sub_expr)
        }
        Function::Cast { expr, data_type } => {
            let expr = eval(expr)?;
            f::cast(expr, data_type)
        }
        Function::Extract { field, expr } => {
            let expr = eval(expr)?;
            f::extract(field, expr)
        }
        Function::ConcatWs { separator, exprs } => {
            let separator = eval(separator)?;
            let exprs = exprs.iter().map(eval).collect::<Result<Vec<_>>>()?;

            f::concat_ws(name, separator, exprs)
        }
    }
}
