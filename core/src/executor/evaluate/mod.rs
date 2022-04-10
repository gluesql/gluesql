mod error;
mod evaluated;
mod expr;
mod function;
mod stateless;

use {
    super::{context::FilterContext, select::select},
    crate::{
        ast::{Aggregate, Expr, Function},
        data::Value,
        result::Result,
        store::GStore,
    },
    async_recursion::async_recursion,
    chrono::prelude::Utc,
    futures::{
        future::ready,
        stream::{self, StreamExt, TryStreamExt},
    },
    im_rc::HashMap,
    std::{borrow::Cow, rc::Rc},
};

pub use {error::EvaluateError, evaluated::Evaluated, stateless::evaluate_stateless};

#[async_recursion(?Send)]
pub async fn evaluate<'a, T>(
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
            .and_then(|aggregated| aggregated.get(aggr.as_ref()))
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
        Expr::Extract { field, expr } => eval(expr).await?.extract(field),
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
        Expr::VariableArgs {
            args,
        } => {
            // todo ????o

        }
    }
}

async fn evaluate_function<'a, T>(
    storage: &'a dyn GStore<T>,
    context: Option<Rc<FilterContext<'a>>>,
    aggregated: Option<Rc<HashMap<&'a Aggregate, Value>>>,
    func: &'a Function,
) -> Result<Evaluated<'a>> {
    use function as f;

    let eval = |expr| {
        let context = context.as_ref().map(Rc::clone);
        let aggregated = aggregated.as_ref().map(Rc::clone);

        evaluate(storage, context, aggregated, expr)
    };

    let name = || func.to_string();

    match func {
        // --- text ---
        Function::Concat {expr} => f::concat(name(), eval(expr).await?),
        Function::Lower(expr) => f::lower(name(), eval(expr).await?),
        Function::Upper(expr) => f::upper(name(), eval(expr).await?),
        Function::Left { expr, size } | Function::Right { expr, size } => {
            let expr = eval(expr).await?;
            let size = eval(size).await?;

            f::left_or_right(name(), expr, size)
        }
        Function::Lpad { expr, size, fill } | Function::Rpad { expr, size, fill } => {
            let expr = eval(expr).await?;
            let size = eval(size).await?;
            let fill = match fill {
                Some(v) => Some(eval(v).await?),
                None => None,
            };

            f::lpad_or_rpad(name(), expr, size, fill)
        }
        Function::Trim {
            expr,
            filter_chars,
            trim_where_field,
        } => {
            let expr = eval(expr).await?;
            let filter_chars = match filter_chars {
                Some(v) => Some(eval(v).await?),
                None => None,
            };

            f::trim(name(), expr, filter_chars, trim_where_field)
        }
        Function::Ltrim { expr, chars } => {
            let expr = eval(expr).await?;
            let chars = match chars {
                Some(v) => Some(eval(v).await?),
                None => None,
            };

            f::ltrim(name(), expr, chars)
        }
        Function::Rtrim { expr, chars } => {
            let expr = eval(expr).await?;
            let chars = match chars {
                Some(v) => Some(eval(v).await?),
                None => None,
            };

            f::rtrim(name(), expr, chars)
        }
        Function::Reverse(expr) => {
            let expr = eval(expr).await?;

            f::reverse(name(), expr)
        }
        Function::Repeat { expr, num } => {
            let expr = eval(expr).await?;
            let num = eval(num).await?;

            f::repeat(name(), expr, num)
        }
        Function::Substr { expr, start, count } => {
            let expr = eval(expr).await?;
            let start = eval(start).await?;
            let count = match count {
                Some(v) => Some(eval(v).await?),
                None => None,
            };

            f::substr(name(), expr, start, count)
        }

        // --- float ---
        Function::Abs(expr) => f::abs(name(), eval(expr).await?),
        Function::Sign(expr) => f::sign(name(), eval(expr).await?),
        Function::Sqrt(expr) => f::sqrt(name(), eval(expr).await?),
        Function::Power { expr, power } => {
            let expr = eval(expr).await?;
            let power = eval(power).await?;

            f::power(name(), expr, power)
        }
        Function::Ceil(expr) => f::ceil(name(), eval(expr).await?),
        Function::Round(expr) => f::round(name(), eval(expr).await?),
        Function::Floor(expr) => f::floor(name(), eval(expr).await?),
        Function::Radians(expr) => f::radians(name(), eval(expr).await?),
        Function::Degrees(expr) => f::degrees(name(), eval(expr).await?),
        Function::Pi() => Ok(Value::F64(std::f64::consts::PI)),
        Function::Exp(expr) => f::exp(name(), eval(expr).await?),
        Function::Log { antilog, base } => {
            let antilog = eval(antilog).await?;
            let base = eval(base).await?;

            f::log(name(), antilog, base)
        }
        Function::Ln(expr) => f::ln(name(), eval(expr).await?),
        Function::Log2(expr) => f::log2(name(), eval(expr).await?),
        Function::Log10(expr) => f::log10(name(), eval(expr).await?),
        Function::Sin(expr) => f::sin(name(), eval(expr).await?),
        Function::Cos(expr) => f::cos(name(), eval(expr).await?),
        Function::Tan(expr) => f::tan(name(), eval(expr).await?),
        Function::ASin(expr) => f::asin(name(), eval(expr).await?),
        Function::ACos(expr) => f::acos(name(), eval(expr).await?),
        Function::ATan(expr) => f::atan(name(), eval(expr).await?),

        // --- integer ---
        Function::Div { dividend, divisor } => {
            let dividend = eval(dividend).await?;
            let divisor = eval(divisor).await?;

            f::div(name(), dividend, divisor)
        }
        Function::Mod { dividend, divisor } => {
            let dividend = eval(dividend).await?;
            let divisor = eval(divisor).await?;

            return dividend.modulo(&divisor);
        }
        Function::Gcd { left, right } => {
            let left = eval(left).await?;
            let right = eval(right).await?;

            f::gcd(name(), left, right)
        }
        Function::Lcm { left, right } => {
            let left = eval(left).await?;
            let right = eval(right).await?;

            f::lcm(name(), left, right)
        }

        // --- etc ---
        Function::Unwrap { expr, selector } => {
            let expr = eval(expr).await?;
            let selector = eval(selector).await?;

            f::unwrap(name(), expr, selector)
        }
        Function::GenerateUuid() => Ok(f::generate_uuid()),
        Function::Now() => Ok(Value::Timestamp(Utc::now().naive_utc())),
    }
    .map(Evaluated::from)
}
