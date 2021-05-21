use {
    super::{EvaluateError, Evaluated},
    crate::{
        ast::{BinaryOperator, Expr, UnaryOperator},
        data::{Literal, Row, Value},
        result::Result,
    },
    boolinator::Boolinator,
    std::{
        borrow::Cow,
        convert::{TryFrom, TryInto},
    },
};

type Columns<'a> = &'a [String];

pub fn evaluate_stateless<'a>(
    context: Option<(Columns, &'a Row)>, // columns, row
    expr: &'a Expr,
) -> Result<Evaluated<'a>> {
    let eval = |expr| evaluate_stateless(context, expr);

    match expr {
        Expr::Literal(ast_literal) => Literal::try_from(ast_literal).map(Evaluated::Literal),
        Expr::TypedString { data_type, value } => {
            let literal = Literal::Text(Cow::Borrowed(value));

            Value::try_from_literal(&data_type, &literal).map(Evaluated::from)
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
                .map(|index| row.get_value(index))
                .flatten();

            match value {
                Some(value) => Ok(value.clone()),
                None => Err(EvaluateError::ValueNotFound(ident.to_owned()).into()),
            }
            .map(Evaluated::from)
        }
        Expr::Nested(expr) => eval(&expr),
        Expr::BinaryOp { op, left, right } => {
            let l = eval(left)?;
            let r = eval(right)?;

            macro_rules! cmp {
                ($expr: expr) => {
                    Ok(Evaluated::from(Value::Bool($expr)))
                };
            }

            macro_rules! cond {
                (l $op: tt r) => {{
                    let l: bool = l.try_into()?;
                    let r: bool = r.try_into()?;
                    let v = l $op r;

                    Ok(Evaluated::from(Value::Bool(v)))
                }};
            }

            match op {
                BinaryOperator::Plus => l.add(&r),
                BinaryOperator::Minus => l.subtract(&r),
                BinaryOperator::Multiply => l.multiply(&r),
                BinaryOperator::Divide => l.divide(&r),
                BinaryOperator::StringConcat => l.concat(r),
                BinaryOperator::Eq => cmp!(l == r),
                BinaryOperator::NotEq => cmp!(l != r),
                BinaryOperator::Lt => cmp!(l < r),
                BinaryOperator::LtEq => cmp!(l <= r),
                BinaryOperator::Gt => cmp!(l > r),
                BinaryOperator::GtEq => cmp!(l >= r),
                BinaryOperator::And => cond!(l && r),
                BinaryOperator::Or => cond!(l || r),
            }
        }
        Expr::UnaryOp { op, expr } => {
            let v = eval(expr)?;

            match op {
                UnaryOperator::Plus => v.unary_plus(),
                UnaryOperator::Minus => v.unary_minus(),
                UnaryOperator::Not => v.try_into().map(|v: bool| Evaluated::from(Value::Bool(!v))),
            }
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
                        |evaluated| (target == &evaluated).as_some(Ok(!negated)),
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
            let negated = *negated;
            let target = eval(expr)?;

            let v = eval(low)? <= target && target <= eval(high)?;
            let v = negated ^ v;

            Ok(Evaluated::from(Value::Bool(v)))
        }
        Expr::IsNull(expr) => {
            let v = eval(expr)?.is_null();

            Ok(Evaluated::from(Value::Bool(v)))
        }
        Expr::IsNotNull(expr) => {
            let v = eval(expr)?.is_null();

            Ok(Evaluated::from(Value::Bool(!v)))
        }
        _ => {
            panic!();
        }
    }
}
