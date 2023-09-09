use {
    super::{
        ast_literal::{translate_ast_literal, translate_datetime_field},
        data_type::translate_data_type,
        function::{
            translate_cast, translate_ceil, translate_extract, translate_floor, translate_function,
            translate_position,
        },
        operator::{translate_binary_operator, translate_unary_operator},
        translate_idents, translate_query, TranslateError,
    },
    crate::{
        ast::{Expr, OrderByExpr},
        result::Result,
        translate::function::translate_trim,
    },
    sqlparser::ast::{
        DateTimeField as SqlDateTimeField, Expr as SqlExpr, Interval as SqlInterval,
        OrderByExpr as SqlOrderByExpr,
    },
};

/// # Description
/// Returns [`Expr`] in the form required for `GlueSQL` from [`SqlExpr`] provided by `sqlparser-rs`. <br>
/// Among them, there are functions that are translated to a lower level of [`Expr`] rather than [`Expr::Function`]
/// - e.g) `cast`, `extract`
///
/// This is because it follows the parsed result of `sqlparser-rs` as it is. <br>
/// It is ambiguous whether the parsed tokens will be classified as a lower level of [`Expr`] or a lower level of [`Expr::Function`]. <br>
/// In `GlueSQL`, if an argument is received wrapped in `( )` in the sql statement, the standard is set to translate in the form of `Expr::Function(Box<Function::Cast>)` rather than `Expr::Cast`.
pub fn translate_expr(sql_expr: &SqlExpr) -> Result<Expr> {
    match sql_expr {
        SqlExpr::Identifier(ident) => Ok(Expr::Identifier(ident.value.clone())),
        SqlExpr::CompoundIdentifier(idents) => (idents.len() == 2)
            .then(|| Expr::CompoundIdentifier {
                alias: idents[0].value.clone(),
                ident: idents[1].value.clone(),
            })
            .ok_or_else(|| {
                TranslateError::UnsupportedExpr(translate_idents(idents).join(".")).into()
            }),
        SqlExpr::IsNull(expr) => translate_expr(expr).map(Box::new).map(Expr::IsNull),
        SqlExpr::IsNotNull(expr) => translate_expr(expr).map(Box::new).map(Expr::IsNotNull),
        SqlExpr::InList {
            expr,
            list,
            negated,
        } => Ok(Expr::InList {
            expr: translate_expr(expr).map(Box::new)?,
            list: list.iter().map(translate_expr).collect::<Result<_>>()?,
            negated: *negated,
        }),
        SqlExpr::InSubquery {
            expr,
            subquery,
            negated,
        } => Ok(Expr::InSubquery {
            expr: translate_expr(expr).map(Box::new)?,
            subquery: translate_query(subquery).map(Box::new)?,
            negated: *negated,
        }),
        SqlExpr::Between {
            expr,
            negated,
            low,
            high,
        } => Ok(Expr::Between {
            expr: translate_expr(expr).map(Box::new)?,
            negated: *negated,
            low: translate_expr(low).map(Box::new)?,
            high: translate_expr(high).map(Box::new)?,
        }),
        SqlExpr::Like {
            expr,
            negated,
            pattern,
            escape_char: None,
        } => Ok(Expr::Like {
            expr: translate_expr(expr).map(Box::new)?,
            negated: *negated,
            pattern: translate_expr(pattern).map(Box::new)?,
        }),
        SqlExpr::ILike {
            expr,
            negated,
            pattern,
            escape_char: None,
        } => Ok(Expr::ILike {
            expr: translate_expr(expr).map(Box::new)?,
            negated: *negated,
            pattern: translate_expr(pattern).map(Box::new)?,
        }),
        SqlExpr::BinaryOp { left, op, right } => Ok(Expr::BinaryOp {
            left: translate_expr(left).map(Box::new)?,
            op: translate_binary_operator(op)?,
            right: translate_expr(right).map(Box::new)?,
        }),
        SqlExpr::UnaryOp { op, expr } => Ok(Expr::UnaryOp {
            op: translate_unary_operator(op)?,
            expr: translate_expr(expr).map(Box::new)?,
        }),
        SqlExpr::Extract { field, expr } => translate_extract(field, expr),
        SqlExpr::Nested(expr) => translate_expr(expr).map(Box::new).map(Expr::Nested),
        SqlExpr::Value(value) => translate_ast_literal(value).map(Expr::Literal),
        SqlExpr::TypedString { data_type, value } => Ok(Expr::TypedString {
            data_type: translate_data_type(data_type)?,
            value: value.to_owned(),
        }),
        SqlExpr::Function(function) => translate_function(function),
        SqlExpr::Trim {
            expr,
            trim_where,
            trim_what,
        } => translate_trim(expr, trim_where, trim_what),
        SqlExpr::Floor { expr, field } => {
            if !matches!(field, SqlDateTimeField::NoDateTime) {
                return Err(TranslateError::UnsupportedExpr(sql_expr.to_string()).into());
            }

            translate_floor(expr)
        }
        SqlExpr::Ceil { expr, field } => {
            if !matches!(field, SqlDateTimeField::NoDateTime) {
                return Err(TranslateError::UnsupportedExpr(sql_expr.to_string()).into());
            }

            translate_ceil(expr)
        }
        SqlExpr::Exists { subquery, negated } => Ok(Expr::Exists {
            subquery: translate_query(subquery).map(Box::new)?,
            negated: *negated,
        }),
        SqlExpr::Subquery(query) => translate_query(query).map(Box::new).map(Expr::Subquery),
        SqlExpr::Case {
            operand,
            conditions,
            results,
            else_result,
        } => Ok(Expr::Case {
            operand: operand
                .as_ref()
                .map(|expr| translate_expr(expr.as_ref()).map(Box::new))
                .transpose()?,
            when_then: conditions
                .iter()
                .zip(results)
                .map(|(when, then)| {
                    let when = translate_expr(when)?;
                    let then = translate_expr(then)?;

                    Ok((when, then))
                })
                .collect::<Result<Vec<_>>>()?,
            else_result: else_result
                .as_ref()
                .map(|expr| translate_expr(expr.as_ref()).map(Box::new))
                .transpose()?,
        }),
        SqlExpr::ArrayIndex { obj, indexes } => Ok(Expr::ArrayIndex {
            obj: translate_expr(obj).map(Box::new)?,
            indexes: indexes.iter().map(translate_expr).collect::<Result<_>>()?,
        }),
        SqlExpr::Position { expr, r#in } => translate_position(expr, r#in),
        SqlExpr::Interval(SqlInterval {
            value,
            leading_field,
            last_field,
            ..
        }) => Ok(Expr::Interval {
            expr: translate_expr(value).map(Box::new)?,
            leading_field: leading_field
                .as_ref()
                .map(translate_datetime_field)
                .transpose()?,
            last_field: last_field
                .as_ref()
                .map(translate_datetime_field)
                .transpose()?,
        }),
        SqlExpr::Cast { expr, data_type } => translate_cast(expr, data_type),

        _ => Err(TranslateError::UnsupportedExpr(sql_expr.to_string()).into()),
    }
}

pub fn translate_order_by_expr(sql_order_by_expr: &SqlOrderByExpr) -> Result<OrderByExpr> {
    let SqlOrderByExpr {
        expr,
        asc,
        nulls_first,
    } = sql_order_by_expr;

    if nulls_first.is_some() {
        return Err(TranslateError::OrderByNullsFirstOrLastNotSupported.into());
    }

    Ok(OrderByExpr {
        expr: translate_expr(expr)?,
        asc: *asc,
    })
}
