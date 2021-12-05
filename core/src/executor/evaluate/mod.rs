mod error;
mod evaluated;
mod expr;
mod stateless;

use {
    super::{context::FilterContext, select::select},
    crate::{
        ast::{Aggregate, Expr, Function, TrimWhereField},
        data::Value,
        result::{Error, Result},
        store::GStore,
    },
    async_recursion::async_recursion,
    futures::{
        future::ready,
        stream::{self, StreamExt, TryStreamExt},
    },
    im_rc::HashMap,
    std::{
        borrow::Cow,
        cmp::{max, min},
        fmt::Debug,
        rc::Rc,
    },
    uuid::Uuid,
};

pub use {error::EvaluateError, evaluated::Evaluated, stateless::evaluate_stateless};

#[async_recursion(?Send)]
pub async fn evaluate<'a, T: Debug>(
    storage: &'a dyn GStore<T>,
    context: Option<Rc<FilterContext<'a>>>,
    aggregated: Option<Rc<HashMap<&'a Aggregate, Value>>>,
    expr: &'a Expr,
) -> Result<Evaluated<'a>> {
    let eval = |expr| {
        let context = context.as_ref().map(Rc::clone);
        let aggregated = aggregated.as_ref().map(Rc::clone);

        evaluate(storage, context, aggregated, expr)
    };

    match expr {
        Expr::Literal(ast_literal) => expr::literal(ast_literal),
        Expr::TypedString { data_type, value } => {
            expr::typed_string(data_type, Cow::Borrowed(value))
        }
        Expr::Identifier(ident) => {
            let context = context.ok_or(EvaluateError::UnreachableEmptyContext)?;

            match context.get_value(ident) {
                Some(value) => Ok(value.clone()),
                None => Err(EvaluateError::ValueNotFound(ident.to_string()).into()),
            }
            .map(Evaluated::from)
        }
        Expr::Nested(expr) => eval(expr).await,
        Expr::CompoundIdentifier(idents) => {
            if idents.len() != 2 {
                return Err(EvaluateError::UnsupportedCompoundIdentifier(expr.clone()).into());
            }

            let table_alias = &idents[0];
            let column = &idents[1];
            let context = context.ok_or(EvaluateError::UnreachableEmptyContext)?;

            match context.get_alias_value(table_alias, column) {
                Some(value) => Ok(value.clone()),
                None => Err(EvaluateError::ValueNotFound(column.to_string()).into()),
            }
            .map(Evaluated::from)
        }
        Expr::Subquery(query) => select(storage, query, context.as_ref().map(Rc::clone))
            .await?
            .map_ok(|row| row.take_first_value().map(Evaluated::from))
            .next()
            .await
            .unwrap_or_else(|| Err(EvaluateError::NestedSelectRowNotFound.into()))?,
        Expr::BinaryOp { op, left, right } => {
            let left = eval(left).await?;
            let right = eval(right).await?;

            expr::binary_op(op, left, right)
        }
        Expr::UnaryOp { op, expr } => {
            let v = eval(expr).await?;

            expr::unary_op(op, v)
        }
        Expr::Aggregate(aggr) => match aggregated
            .as_ref()
            .map(|aggregated| aggregated.get(aggr.as_ref()))
            .flatten()
        {
            Some(value) => Ok(Evaluated::from(value.clone())),
            None => Err(EvaluateError::UnreachableEmptyAggregateValue(*aggr.clone()).into()),
        },
        Expr::Function(func) => {
            let context = context.as_ref().map(Rc::clone);
            let aggregated = aggregated.as_ref().map(Rc::clone);

            evaluate_function(storage, context, aggregated, func).await
        }
        Expr::Cast { expr, data_type } => eval(expr).await?.cast(data_type),
        Expr::InList {
            expr,
            list,
            negated,
        } => {
            let negated = *negated;
            let target = eval(expr).await?;

            stream::iter(list)
                .then(eval)
                .try_filter(|evaluated| ready(evaluated == &target))
                .try_next()
                .await
                .map(|v| v.is_some() ^ negated)
                .map(Value::Bool)
                .map(Evaluated::from)
        }
        Expr::InSubquery {
            expr,
            subquery,
            negated,
        } => {
            let target = eval(expr).await?;

            select(storage, subquery, context)
                .await?
                .and_then(|row| ready(row.take_first_value().map(Evaluated::from)))
                .try_filter(|evaluated| ready(evaluated == &target))
                .try_next()
                .await
                .map(|v| v.is_some() ^ negated)
                .map(Value::Bool)
                .map(Evaluated::from)
        }
        Expr::Between {
            expr,
            negated,
            low,
            high,
        } => {
            let target = eval(expr).await?;
            let low = eval(low).await?;
            let high = eval(high).await?;

            expr::between(target, *negated, low, high)
        }
        Expr::Exists(query) => select(storage, query, context)
            .await?
            .try_next()
            .await
            .map(|v| v.is_some())
            .map(Value::Bool)
            .map(Evaluated::from),
        Expr::IsNull(expr) => {
            let v = eval(expr).await?.is_null();

            Ok(Evaluated::from(Value::Bool(v)))
        }
        Expr::IsNotNull(expr) => {
            let v = eval(expr).await?.is_null();

            Ok(Evaluated::from(Value::Bool(!v)))
        }
        Expr::Wildcard | Expr::QualifiedWildcard(_) => {
            Err(EvaluateError::UnreachableWildcardExpr.into())
        }
        Expr::Case {
            operand,
            when_then,
            else_result,
        } => {
            let operand = match operand {
                Some(op) => eval(op).await?,
                None => Evaluated::from(Value::Bool(true)),
            };

            for (when, then) in when_then.iter() {
                let when = eval(when).await?;

                if when.eq(&operand) {
                    return eval(then).await;
                }
            }

            match else_result {
                Some(er) => eval(er).await,
                None => Ok(Evaluated::from(Value::Null)),
            }
        }
    }
}

async fn evaluate_function<'a, T: Debug>(
    storage: &'a dyn GStore<T>,
    context: Option<Rc<FilterContext<'a>>>,
    aggregated: Option<Rc<HashMap<&'a Aggregate, Value>>>,
    func: &'a Function,
) -> Result<Evaluated<'a>> {
    let eval = |expr| {
        let context = context.as_ref().map(Rc::clone);
        let aggregated = aggregated.as_ref().map(Rc::clone);

        evaluate(storage, context, aggregated, expr)
    };

    enum Nullable<T> {
        Value(T),
        Null,
    }

    let eval_to_str = |expr| async move {
        match eval(expr).await?.try_into()? {
            Value::Str(s) => Ok(Nullable::Value(s)),
            Value::Null => Ok(Nullable::Null),
            _ => {
                Err::<_, Error>(EvaluateError::FunctionRequiresStringValue(func.to_string()).into())
            }
        }
    };

    let eval_to_float = |expr| async move {
        match eval(expr).await?.try_into()? {
            Value::I64(v) => Ok(Nullable::Value(v as f64)),
            Value::F64(v) => Ok(Nullable::Value(v)),
            Value::Null => Ok(Nullable::Null),
            _ => {
                Err::<_, Error>(EvaluateError::FunctionRequiresFloatValue(func.to_string()).into())
            }
        }
    };

    let eval_to_integer = |expr| async move {
        match eval(expr).await?.try_into()? {
            Value::I64(number) => Ok(Nullable::Value(number)),
            Value::Null => Ok(Nullable::Null),
            _ => Err::<_, Error>(
                EvaluateError::FunctionRequiresIntegerValue(func.to_string()).into(),
            ),
        }
    };

    macro_rules! eval_to_str {
        ($v : expr) => {
            match eval_to_str($v).await? {
                Nullable::Value(s) => s,
                Nullable::Null => return Ok(Evaluated::from(Value::Null)),
            }
        };
    }

    macro_rules! eval_to_float {
        ($v : expr) => {
            match eval_to_float($v).await? {
                Nullable::Value(f) => f,
                Nullable::Null => return Ok(Evaluated::from(Value::Null)),
            }
        };
    }

    macro_rules! eval_to_integer {
        ($v : expr) => {
            match eval_to_integer($v).await? {
                Nullable::Value(i) => i,
                Nullable::Null => return Ok(Evaluated::from(Value::Null)),
            }
        };
    }

    match func {
        Function::Lower(expr) => {
            let v = eval_to_str!(expr).to_lowercase();
            Ok(Evaluated::from(Value::Str(v)))
        }

        Function::Upper(expr) => {
            let v = eval_to_str!(expr).to_uppercase();
            Ok(Evaluated::from(Value::Str(v)))
        }

        Function::Sqrt(expr) => Ok(Value::F64(eval_to_float!(expr).sqrt())).map(Evaluated::from),

        Function::Power { expr, power } => {
            let number = eval_to_float!(expr);
            let power = eval_to_float!(power);

            Ok(Evaluated::from(Value::F64(number.powf(power) as f64)))
        }

        Function::Left { expr, size } | Function::Right { expr, size } => {
            let name = if matches!(func, Function::Left { .. }) {
                "LEFT"
            } else {
                "RIGHT"
            };

            let string = eval_to_str!(expr);

            let size = match eval(size).await?.try_into()? {
                Value::I64(number) => usize::try_from(number)
                    .map_err(|_| EvaluateError::FunctionRequiresUSizeValue(name.to_owned()))?,
                Value::Null => {
                    return Ok(Evaluated::from(Value::Null));
                }
                _ => {
                    return Err(EvaluateError::FunctionRequiresIntegerValue(name.to_owned()).into());
                }
            };

            let converted = if name == "LEFT" {
                string.get(..size).map(|v| v.to_string()).unwrap_or(string)
            } else {
                let start_pos = if size > string.len() {
                    0
                } else {
                    string.len() - size
                };

                string
                    .get(start_pos..)
                    .map(|value| value.to_string())
                    .unwrap_or(string)
            };

            Ok(Evaluated::from(Value::Str(converted)))
        }

        Function::ASin(expr) | Function::ACos(expr) | Function::ATan(expr) => {
            let float_number = eval_to_float!(expr);

            let trigonometric = |func, value| match func {
                Function::ASin(_) => f64::asin(value),
                Function::ACos(_) => f64::acos(value),
                _ => f64::atan(value),
            };

            Ok(Value::F64(trigonometric(func.to_owned(), float_number))).map(Evaluated::from)
        }

        Function::Lpad { expr, size, fill } | Function::Rpad { expr, size, fill } => {
            let name = if matches!(func, Function::Lpad { .. }) {
                "LPAD"
            } else {
                "RPAD"
            };

            let string = eval_to_str!(expr);

            let size = match eval(size).await?.try_into()? {
                Value::I64(number) => usize::try_from(number)
                    .map_err(|_| EvaluateError::FunctionRequiresUSizeValue(name.to_owned()))?,
                Value::Null => {
                    return Ok(Evaluated::from(Value::Null));
                }
                _ => {
                    return Err(EvaluateError::FunctionRequiresIntegerValue(name.to_owned()).into());
                }
            };

            let fill = match fill {
                Some(expr) => eval_to_str!(expr),
                None => " ".to_string(),
            };

            let result = if size > string.len() {
                let padding_size = size - string.len();
                let repeat_count = padding_size / fill.len();
                let plus_count = padding_size % fill.len();
                let fill = fill.repeat(repeat_count) + &fill[0..plus_count];

                if name == "LPAD" {
                    fill + &string
                } else {
                    string + &fill
                }
            } else {
                string[0..size].to_string()
            };

            Ok(Evaluated::from(Value::Str(result)))
        }

        Function::Ceil(expr) => Ok(eval_to_float!(expr).ceil())
            .map(Value::F64)
            .map(Evaluated::from),

        Function::Round(expr) => Ok(eval_to_float!(expr).round())
            .map(Value::F64)
            .map(Evaluated::from),

        Function::Floor(expr) => Ok(eval_to_float!(expr).floor())
            .map(Value::F64)
            .map(Evaluated::from),

        Function::Radians(expr) => Ok(eval_to_float!(expr).to_radians())
            .map(Value::F64)
            .map(Evaluated::from),

        Function::Degrees(expr) => Ok(eval_to_float!(expr).to_degrees())
            .map(Value::F64)
            .map(Evaluated::from),

        Function::Pi() => {
            { Ok(Evaluated::from(Value::F64(std::f64::consts::PI))) }.map(Evaluated::from)
        }
        Function::Trim {
            expr,
            filter_chars,
            trim_where_field,
        } => {
            let expr_str = eval_to_str!(expr);
            let expr_str = expr_str.as_str();

            let filter_chars = match filter_chars {
                Some(expr) => eval_to_str!(expr).chars().collect::<Vec<_>>(),
                None => vec![' '],
            };

            Ok(Value::Str(
                match trim_where_field {
                    Some(TrimWhereField::Both) => expr_str.trim_matches(&filter_chars[..]),
                    Some(TrimWhereField::Leading) => expr_str.trim_start_matches(&filter_chars[..]),
                    Some(TrimWhereField::Trailing) => expr_str.trim_end_matches(&filter_chars[..]),
                    None => return Ok(Evaluated::from(Value::Str(expr_str.trim().to_owned()))),
                }
                .to_owned(),
            ))
            .map(Evaluated::from)
        }
        Function::Exp(expr) => Ok(eval_to_float!(expr).exp())
            .map(Value::F64)
            .map(Evaluated::from),

        Function::Ln(expr) => Ok(eval_to_float!(expr).ln())
            .map(Value::F64)
            .map(Evaluated::from),

        Function::Log { antilog, base } => {
            let antilog = eval_to_float!(antilog);
            let base = eval_to_float!(base);

            Ok(Evaluated::from(Value::F64(antilog.log(base))))
        }

        Function::Log2(expr) => Ok(eval_to_float!(expr).log2())
            .map(Value::F64)
            .map(Evaluated::from),

        Function::Log10(expr) => Ok(eval_to_float!(expr).log10())
            .map(Value::F64)
            .map(Evaluated::from),

        Function::Sin(expr) | Function::Cos(expr) | Function::Tan(expr) => {
            let float_number = eval_to_float!(expr);

            let trigonometric = |func, value| match func {
                Function::Sin(_) => f64::sin(value),
                Function::Cos(_) => f64::cos(value),
                _ => f64::tan(value),
            };

            Ok(Value::F64(trigonometric(func.to_owned(), float_number))).map(Evaluated::from)
        }
        Function::Div { dividend, divisor } => {
            let name = "DIV";

            let dividend = match eval(dividend).await?.try_into()? {
                Value::F64(number) => number,
                Value::I64(number) => number as f64,
                Value::Null => {
                    return Ok(Evaluated::from(Value::Null));
                }
                _ => {
                    return Err(EvaluateError::FunctionRequiresFloatOrIntegerValue(
                        name.to_owned(),
                    )
                    .into());
                }
            };

            let divisor = match eval(divisor).await?.try_into()? {
                Value::F64(number) => match number {
                    x if x == 0.0 => return Err(EvaluateError::DivisorShouldNotBeZero.into()),
                    _ => number,
                },
                Value::I64(number) => match number {
                    0 => return Err(EvaluateError::DivisorShouldNotBeZero.into()),
                    _ => number as f64,
                },
                Value::Null => {
                    return Ok(Evaluated::from(Value::Null));
                }
                _ => {
                    return Err(EvaluateError::FunctionRequiresFloatOrIntegerValue(
                        name.to_owned(),
                    )
                    .into());
                }
            };

            Ok(Evaluated::from(Value::I64((dividend / divisor) as i64)))
        }
        Function::Mod { dividend, divisor } => {
            let dividend = eval(dividend).await?;
            let divisor = eval(divisor).await?;
            dividend.modulo(&divisor)
        }
        Function::Gcd { left, right } => {
            let left = eval_to_integer!(left);
            let right = eval_to_integer!(right);

            Ok(Evaluated::from(Value::I64(gcd(left, right))))
        }
        Function::Lcm { left, right } => {
            let left = eval_to_integer!(left);
            let right = eval_to_integer!(right);

            fn lcm(a: i64, b: i64) -> i64 {
                a * b / gcd(a, b)
            }

            Ok(Evaluated::from(Value::I64(lcm(left, right))))
        }
        Function::Ltrim { expr, chars } | Function::Rtrim { expr, chars } => {
            let name = if matches!(func, Function::Ltrim { .. }) {
                "LTRIM"
            } else {
                "RTRIM"
            };
            let pattern: Result<Vec<char>> = match chars {
                Some(chars) => Ok(eval_to_str!(chars).chars().collect::<Vec<char>>()),
                None => Ok(" ".chars().collect::<Vec<char>>()),
            };

            let string = eval_to_str!(expr);
            if name == "LTRIM" {
                Ok(Value::Str(
                    string.trim_start_matches(&pattern?[..]).to_string(),
                ))
            } else {
                Ok(Value::Str(
                    string.trim_end_matches(&pattern?[..]).to_string(),
                ))
            }
            .map(Evaluated::from)
        }
        Function::Reverse(expr) => Ok(eval_to_str!(expr).chars().rev().collect::<String>())
            .map(Value::Str)
            .map(Evaluated::from),

        Function::Substr { expr, start, count } => {
            let string = eval_to_str!(expr);
            let start = eval_to_integer!(start) - 1;

            let count = match count {
                Some(v) => eval_to_integer!(v),
                None => string.len() as i64,
            };

            let end = if count < 0 {
                return Err(EvaluateError::NegativeSubstrLenNotAllowed.into());
            } else {
                min(max(start + count, 0) as usize, string.len())
            };

            let start = min(max(start, 0) as usize, string.len());
            let string = String::from(&string[start..end]);
            Ok(Evaluated::from(Value::Str(string)))
        }
        Function::Unwrap { expr, selector } => {
            let evaluated = eval(expr).await?;

            if evaluated.is_null() {
                return Ok(Evaluated::from(Value::Null));
            }

            let value = match &evaluated {
                Evaluated::Value(value) => value.as_ref(),
                _ => {
                    return Err(EvaluateError::FunctionRequiresMapValue(func.to_string()).into());
                }
            };

            let selector = eval_to_str!(selector);
            value.selector(&selector).map(Evaluated::from)
        }
        .map(Evaluated::from),
        Function::GenerateUuid() => Ok(Evaluated::from(Value::Uuid(Uuid::new_v4().as_u128()))),
        Function::Repeat { expr, num } => {
            let expr = eval_to_str!(expr);
            let num = eval_to_integer!(num) as usize;
            Ok(Value::Str(expr.repeat(num)))
        }
        .map(Evaluated::from),
    }
}

fn gcd(a: i64, b: i64) -> i64 {
    if b == 0 {
        a
    } else {
        gcd(b, a % b)
    }
}
