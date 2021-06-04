use {
    super::{expr, EvaluateError, Evaluated},
    crate::{
        ast::Expr,
        data::{Row, Value},
        result::Result,
    },
    boolinator::Boolinator,
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
            expr::typed_string(data_type, Cow::Borrowed(&value))
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
        Expr::Wildcard | Expr::QualifiedWildcard(_) => {
            Err(EvaluateError::UnreachableWildcardExpr.into())
        }
        _ => Err(EvaluateError::UnsupportedStatelessExpr(format!("{:#?}", expr)).into()),
    }
}
