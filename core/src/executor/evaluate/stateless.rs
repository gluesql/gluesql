use {
    super::{expr, function, EvaluateError, Evaluated},
    crate::{
        ast::{Expr, Function},
        data::{Row, Value},
        result::Result,
    },
    chrono::prelude::Utc,
    std::borrow::Cow,
};

type Columns<'a> = &'a [String];

pub fn evaluate_stateless<'a>(
    context: Option<(Columns, &'a Row)>,
    expr: &'a Expr,
) -> Result<Evaluated<'a>> {
    let eval = |expr| evaluate_stateless(context, expr);

    match expr {
        Expr::Literal(ast_literal) => expr::literal(ast_literal),
        Expr::TypedString { data_type, value } => {
            expr::typed_string(data_type, Cow::Borrowed(value))
        }
        Expr::Identifier(ident) => {
            let (columns, row) = match context {
                Some(context) => context,
                None => {
                    return Err(EvaluateError::ValueNotFound(ident.to_owned()).into());
                }
            };

            let value = columns
                .iter()
                .position(|column| column == ident)
                .and_then(|index| row.get_value(index));

            match value {
                Some(value) => Ok(value.clone()),
                None => Err(EvaluateError::ValueNotFound(ident.to_owned()).into()),
            }
            .map(Evaluated::from)
        }
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
        Expr::Cast { expr, data_type } => eval(expr)?.cast(data_type),
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
                        |evaluated| (target == &evaluated).then(|| Ok(!negated)),
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
        Expr::IsNull(expr) => {
            let v = eval(expr)?.is_null();

            Ok(Evaluated::from(Value::Bool(v)))
        }
        Expr::IsNotNull(expr) => {
            let v = eval(expr)?.is_null();

            Ok(Evaluated::from(Value::Bool(!v)))
        }
        Expr::Function(func) => evaluate_function(context, func),
        _ => Err(EvaluateError::UnsupportedStatelessExpr(expr.clone()).into()),
    }
}

fn evaluate_function<'a>(
    context: Option<(Columns, &'a Row)>,
    func: &'a Function,
) -> Result<Evaluated<'a>> {
    use function as f;

    let name = || func.to_string();
    let eval = |expr| evaluate_stateless(context, expr);
    let eval_opt = |expr| -> Result<Option<_>> {
        match expr {
            Some(v) => Ok(Some(eval(v)?)),
            None => Ok(None),
        }
    };

    match func {
        // --- text ---
        Function::Concat(exprs) => {
            let exprs = exprs.iter().map(eval).collect::<Result<_>>()?;
            f::concat(exprs)
        }
        Function::Lower(expr) => f::lower(name(), eval(expr)?),
        Function::Upper(expr) => f::upper(name(), eval(expr)?),
        Function::Left { expr, size } | Function::Right { expr, size } => {
            let expr = eval(expr)?;
            let size = eval(size)?;

            f::left_or_right(name(), expr, size)
        }
        Function::Lpad { expr, size, fill } | Function::Rpad { expr, size, fill } => {
            let expr = eval(expr)?;
            let size = eval(size)?;
            let fill = eval_opt(fill.as_ref())?;

            f::lpad_or_rpad(name(), expr, size, fill)
        }
        Function::Trim {
            expr,
            filter_chars,
            trim_where_field,
        } => {
            let expr = eval(expr)?;
            let filter_chars = eval_opt(filter_chars.as_ref())?;

            f::trim(name(), expr, filter_chars, trim_where_field)
        }
        Function::Ltrim { expr, chars } => {
            let expr = eval(expr)?;
            let chars = eval_opt(chars.as_ref())?;

            f::ltrim(name(), expr, chars)
        }
        Function::Rtrim { expr, chars } => {
            let expr = eval(expr)?;
            let chars = eval_opt(chars.as_ref())?;

            f::rtrim(name(), expr, chars)
        }
        Function::Rand(expr) => {
            f::rand(expr.as_ref().map(|expr| eval(expr)).transpose()?)
        }
        Function::Reverse(expr) => {
            let expr = eval(expr)?;

            f::reverse(name(), expr)
        }
        Function::Repeat { expr, num } => {
            let expr = eval(expr)?;
            let num = eval(num)?;

            f::repeat(name(), expr, num)
        }
        Function::Substr { expr, start, count } => {
            let expr = eval(expr)?;
            let start = eval(start)?;
            let count = eval_opt(count.as_ref())?;

            f::substr(name(), expr, start, count)
        }

        // --- float ---
        Function::Sqrt(expr) => f::sqrt(name(), eval(expr)?),
        Function::Power { expr, power } => {
            let expr = eval(expr)?;
            let power = eval(power)?;

            f::power(name(), expr, power)
        }
        Function::Abs(expr) => f::abs(name(), eval(expr)?),
        Function::IfNull { expr, then } => f::ifnull(eval(expr)?, eval(then)?),
        Function::Sign(expr) => f::sign(name(), eval(expr)?),
        Function::Ceil(expr) => f::ceil(name(), eval(expr)?),
        Function::Round(expr) => f::round(name(), eval(expr)?),
        Function::Floor(expr) => f::floor(name(), eval(expr)?),
        Function::Radians(expr) => f::radians(name(), eval(expr)?),
        Function::Degrees(expr) => f::degrees(name(), eval(expr)?),
        Function::Pi() => Ok(Value::F64(std::f64::consts::PI)),
        Function::Exp(expr) => f::exp(name(), eval(expr)?),
        Function::Log { antilog, base } => {
            let antilog = eval(antilog)?;
            let base = eval(base)?;

            f::log(name(), antilog, base)
        }
        Function::Ln(expr) => f::ln(name(), eval(expr)?),
        Function::Log2(expr) => f::log2(name(), eval(expr)?),
        Function::Log10(expr) => f::log10(name(), eval(expr)?),
        Function::Sin(expr) => f::sin(name(), eval(expr)?),
        Function::Cos(expr) => f::cos(name(), eval(expr)?),
        Function::Tan(expr) => f::tan(name(), eval(expr)?),
        Function::Asin(expr) => f::asin(name(), eval(expr)?),
        Function::Acos(expr) => f::acos(name(), eval(expr)?),
        Function::Atan(expr) => f::atan(name(), eval(expr)?),

        // --- integer ---
        Function::Div { dividend, divisor } => {
            let dividend = eval(dividend)?;
            let divisor = eval(divisor)?;

            f::div(name(), dividend, divisor)
        }
        Function::Mod { dividend, divisor } => {
            let dividend = eval(dividend)?;
            let divisor = eval(divisor)?;

            return dividend.modulo(&divisor);
        }
        Function::Gcd { left, right } => {
            let left = eval(left)?;
            let right = eval(right)?;

            f::gcd(name(), left, right)
        }
        Function::Lcm { left, right } => {
            let left = eval(left)?;
            let right = eval(right)?;

            f::lcm(name(), left, right)
        }

        // --- etc ---
        Function::Unwrap { expr, selector } => {
            let expr = eval(expr)?;
            let selector = eval(selector)?;

            f::unwrap(name(), expr, selector)
        }
        Function::GenerateUuid() => Ok(f::generate_uuid()),
        Function::Now() => Ok(Value::Timestamp(Utc::now().naive_utc())),
    }
    .map(Evaluated::from)
}
