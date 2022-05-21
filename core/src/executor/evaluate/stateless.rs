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
        Expr::Nested(expr) => evaluate_stateless(context, expr),
        Expr::BinaryOp { op, left, right } => {
            let left = evaluate_stateless(context, left)?; 
            let right = evaluate_stateless(context, right)?;

            expr::binary_op(op, left, right)
        }
        Expr::UnaryOp { op, expr } => {
            let v = evaluate_stateless(context, expr)?;

            expr::unary_op(op, v)
        }
        Expr::Cast { expr, data_type } => evaluate_stateless(context, expr)?.cast(data_type), 
        Expr::InList {
            expr,
            list,
            negated,
        } => {
            let negated = *negated;
            let target = evaluate_stateless(context, expr)?;

            list.iter()
                .filter_map(|expr| {
                    let target = &target;

                    evaluate_stateless(context, expr)
                        .map_or_else(
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
            let target = evaluate_stateless(context, expr)?;
            let low = evaluate_stateless(context, low)?;
            let high = evaluate_stateless(context, high)?;

            expr::between(target, *negated, low, high)
        }
        Expr::IsNull(expr) => {
            let v = evaluate_stateless(context, expr)?.is_null();

            Ok(Evaluated::from(Value::Bool(v)))
        }
        Expr::IsNotNull(expr) => {
            let v = evaluate_stateless(context, expr)?.is_null();

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

    let name = func.to_string();
 
    match func {
        // --- text ---
        Function::Concat(exprs) => {
            let exprs = exprs
                .iter()
                .map(|x| evaluate_stateless(context, x))
                .collect::<Result<_>>()?;

            f::concat(exprs)
        }
        Function::Lower(expr) => f::lower(name, evaluate_stateless(context, expr)?),
        Function::Upper(expr) => f::upper(name, evaluate_stateless(context, expr)?),
        Function::Left { expr, size } | Function::Right { expr, size } => {
            let expr = evaluate_stateless(context, expr)?;
            let size = evaluate_stateless(context, size)?;

            f::left_or_right(name, expr, size)
        }
        Function::Lpad { expr, size, fill } | Function::Rpad { expr, size, fill } => {
            let expr = evaluate_stateless(context, expr)?;
            let size = evaluate_stateless(context, size)?;
            let fill = match fill.as_ref() {
                Some(v) => Some(evaluate_stateless(context, v)?),
                None => None,
            };
    
            
            f::lpad_or_rpad(name, expr, size, fill)
        }
        Function::Trim {
            expr,
            filter_chars,
            trim_where_field,
        } => {
            let expr = evaluate_stateless(context, expr)?;
            let filter_chars = match filter_chars.as_ref() {
                Some(v) => Some(evaluate_stateless(context, v)?),
                None => None,
            };
    
            
            f::trim(name, expr, filter_chars, trim_where_field)
        }
        Function::Ltrim { expr, chars } => {
            let expr = evaluate_stateless(context, expr)?;
            let chars = match chars.as_ref() {
                Some(v) => Some(evaluate_stateless(context, v)?),
                None => None,
            };
    
            
            f::ltrim(name, expr, chars)
        }
        Function::Rtrim { expr, chars } => {
            let expr = evaluate_stateless(context, expr)?;
            let chars = match chars.as_ref() {
                Some(v) => Some(evaluate_stateless(context, v)?),
                None => None,
            };

            f::rtrim(name, expr, chars)
        }
        Function::Reverse(expr) => {
            let expr = evaluate_stateless(context, expr)?;

            f::reverse(name, expr)
        }
        Function::Repeat { expr, num } => {
            let expr = evaluate_stateless(context, expr)?;
            let num = evaluate_stateless(context, num)?;

            f::repeat(name, expr, num)
        }
        Function::Substr { expr, start, count } => {
            let expr = evaluate_stateless(context, expr)?;
            let start = evaluate_stateless(context, start)?;
            let count = match count.as_ref() {
                Some(v) => Some(evaluate_stateless(context, v)?),
                None => None,
            };

            f::substr(name, expr, start, count)
        }

        // --- float ---
        Function::Sqrt(expr) => f::sqrt(name, evaluate_stateless(context, expr)?),
        Function::Power { expr, power } => {
            let expr = evaluate_stateless(context, expr)?;
            let power = evaluate_stateless(context, power)?;

            f::power(name, expr, power)
        }
        Function::Abs(expr) => f::abs(name, evaluate_stateless(context, expr)?),
        Function::Sign(expr) => f::sign(name, evaluate_stateless(context, expr)?),
        Function::Ceil(expr) => f::ceil(name, evaluate_stateless(context, expr)?),
        Function::Round(expr) => f::round(name, evaluate_stateless(context, expr)?),
        Function::Floor(expr) => f::floor(name, evaluate_stateless(context, expr)?),
        Function::Radians(expr) => f::radians(name, evaluate_stateless(context, expr)?),
        Function::Degrees(expr) => f::degrees(name, evaluate_stateless(context, expr)?),
        Function::Pi() => Ok(Value::F64(std::f64::consts::PI)),
        Function::Exp(expr) => f::exp(name, evaluate_stateless(context, expr)?),
        Function::Log { antilog, base } => {
            let antilog = evaluate_stateless(context, antilog)?;
            let base = evaluate_stateless(context, base)?;

            f::log(name, antilog, base)
        }
        Function::Ln(expr) => f::ln(name, evaluate_stateless(context, expr)?),
        Function::Log2(expr) => f::log2(name, evaluate_stateless(context, expr)?),
        Function::Log10(expr) => f::log10(name, evaluate_stateless(context, expr)?),
        Function::Sin(expr) => f::sin(name, evaluate_stateless(context, expr)?),
        Function::Cos(expr) => f::cos(name, evaluate_stateless(context, expr)?),
        Function::Tan(expr) => f::tan(name, evaluate_stateless(context, expr)?),
        Function::Asin(expr) => f::asin(name, evaluate_stateless(context, expr)?),
        Function::Acos(expr) => f::acos(name, evaluate_stateless(context, expr)?),
        Function::Atan(expr) => f::atan(name, evaluate_stateless(context, expr)?),

        // --- integer ---
        Function::Div { dividend, divisor } => {
            let dividend = evaluate_stateless(context, dividend)?;
            let divisor = evaluate_stateless(context, divisor)?;

            f::div(name, dividend, divisor)
        }
        Function::Mod { dividend, divisor } => {
            let dividend = evaluate_stateless(context, dividend)?;
            let divisor = evaluate_stateless(context, divisor)?;

            return dividend.modulo(&divisor);
        }
        Function::Gcd { left, right } => {
            let left = evaluate_stateless(context, left)?;
            let right = evaluate_stateless(context, right)?;

            f::gcd(name, left, right)
        }
        Function::Lcm { left, right } => {
            let left = evaluate_stateless(context, left)?;
            let right = evaluate_stateless(context, right)?;

            f::lcm(name, left, right)
        }

        // --- etc ---
        Function::Unwrap { expr, selector } => {
            let expr = evaluate_stateless(context, expr)?;
            let selector = evaluate_stateless(context, selector)?;

            f::unwrap(name, expr, selector)
        }
        Function::GenerateUuid() => Ok(f::generate_uuid()),
        Function::Now() => Ok(Value::Timestamp(Utc::now().naive_utc())),
    }
    .map(Evaluated::from)
}
