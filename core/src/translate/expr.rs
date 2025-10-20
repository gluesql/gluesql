use {
    super::{
        ParamLiteral, TranslateError,
        ast_literal::{translate_ast_literal, translate_datetime_field},
        bind_placeholder,
        data_type::translate_data_type,
        function::{
            translate_cast_with_params, translate_ceil_with_params, translate_extract_with_params,
            translate_floor_with_params, translate_function_with_params,
            translate_position_with_params, translate_trim_with_params,
        },
        operator::{translate_binary_operator, translate_unary_operator},
        translate_idents, translate_query_with_params,
    },
    crate::{
        ast::{Expr, OrderByExpr},
        result::Result,
    },
    sqlparser::ast::{
        Array, CeilFloorKind as SqlCeilFloorKind, DateTimeField as SqlDateTimeField,
        Expr as SqlExpr, Interval as SqlInterval, OrderByExpr as SqlOrderByExpr,
        Subscript as SqlSubscript, Value as SqlValue,
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
pub(crate) fn translate_expr_with_params(
    sql_expr: &SqlExpr,
    params: &[ParamLiteral],
) -> Result<Expr> {
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
        SqlExpr::IsNull(expr) => translate_expr_with_params(expr, params)
            .map(Box::new)
            .map(Expr::IsNull),
        SqlExpr::IsNotNull(expr) => translate_expr_with_params(expr, params)
            .map(Box::new)
            .map(Expr::IsNotNull),
        SqlExpr::InList {
            expr,
            list,
            negated,
        } => Ok(Expr::InList {
            expr: translate_expr_with_params(expr, params).map(Box::new)?,
            list: list
                .iter()
                .map(|expr| translate_expr_with_params(expr, params))
                .collect::<Result<_>>()?,
            negated: *negated,
        }),
        SqlExpr::InSubquery {
            expr,
            subquery,
            negated,
        } => Ok(Expr::InSubquery {
            expr: translate_expr_with_params(expr, params).map(Box::new)?,
            subquery: translate_query_with_params(subquery, params).map(Box::new)?,
            negated: *negated,
        }),
        SqlExpr::Between {
            expr,
            negated,
            low,
            high,
        } => Ok(Expr::Between {
            expr: translate_expr_with_params(expr, params).map(Box::new)?,
            negated: *negated,
            low: translate_expr_with_params(low, params).map(Box::new)?,
            high: translate_expr_with_params(high, params).map(Box::new)?,
        }),
        SqlExpr::Like {
            expr,
            negated,
            pattern,
            escape_char: None,
            ..
        } => Ok(Expr::Like {
            expr: translate_expr_with_params(expr, params).map(Box::new)?,
            negated: *negated,
            pattern: translate_expr_with_params(pattern, params).map(Box::new)?,
        }),
        SqlExpr::ILike {
            expr,
            negated,
            pattern,
            escape_char: None,
            ..
        } => Ok(Expr::ILike {
            expr: translate_expr_with_params(expr, params).map(Box::new)?,
            negated: *negated,
            pattern: translate_expr_with_params(pattern, params).map(Box::new)?,
        }),
        SqlExpr::BinaryOp { left, op, right } => Ok(Expr::BinaryOp {
            left: translate_expr_with_params(left, params).map(Box::new)?,
            op: translate_binary_operator(op)?,
            right: translate_expr_with_params(right, params).map(Box::new)?,
        }),
        SqlExpr::UnaryOp { op, expr } => Ok(Expr::UnaryOp {
            op: translate_unary_operator(op)?,
            expr: translate_expr_with_params(expr, params).map(Box::new)?,
        }),
        SqlExpr::Extract { field, expr, .. } => translate_extract_with_params(params, field, expr),
        SqlExpr::Nested(expr) => translate_expr_with_params(expr, params)
            .map(Box::new)
            .map(Expr::Nested),
        SqlExpr::Value(value) => match value {
            SqlValue::Placeholder(placeholder) => bind_placeholder(params, placeholder),
            _ => translate_ast_literal(value).map(Expr::Literal),
        },
        SqlExpr::TypedString { data_type, value } => Ok(Expr::TypedString {
            data_type: translate_data_type(data_type)?,
            value: value.to_owned(),
        }),
        SqlExpr::Function(function) => translate_function_with_params(params, function),
        SqlExpr::Trim {
            expr,
            trim_where,
            trim_what,
            trim_characters,
        } => {
            if trim_characters.is_some() {
                return Err(TranslateError::UnsupportedTrimChars.into());
            }

            translate_trim_with_params(params, expr, trim_where, trim_what)
        }
        SqlExpr::Floor { expr, field } => {
            if !matches!(
                field,
                SqlCeilFloorKind::DateTimeField(SqlDateTimeField::NoDateTime)
            ) {
                return Err(TranslateError::UnsupportedExpr(sql_expr.to_string()).into());
            }

            translate_floor_with_params(params, expr)
        }
        SqlExpr::Ceil { expr, field } => {
            if !matches!(
                field,
                SqlCeilFloorKind::DateTimeField(SqlDateTimeField::NoDateTime)
            ) {
                return Err(TranslateError::UnsupportedExpr(sql_expr.to_string()).into());
            }

            translate_ceil_with_params(params, expr)
        }
        SqlExpr::Exists { subquery, negated } => Ok(Expr::Exists {
            subquery: translate_query_with_params(subquery, params).map(Box::new)?,
            negated: *negated,
        }),
        SqlExpr::Subquery(query) => translate_query_with_params(query, params)
            .map(Box::new)
            .map(Expr::Subquery),
        SqlExpr::Case {
            operand,
            conditions,
            results,
            else_result,
        } => Ok(Expr::Case {
            operand: operand
                .as_ref()
                .map(|expr| translate_expr_with_params(expr.as_ref(), params).map(Box::new))
                .transpose()?,
            when_then: conditions
                .iter()
                .zip(results)
                .map(|(when, then)| {
                    let when = translate_expr_with_params(when, params)?;
                    let then = translate_expr_with_params(then, params)?;

                    Ok((when, then))
                })
                .collect::<Result<Vec<_>>>()?,
            else_result: else_result
                .as_ref()
                .map(|expr| translate_expr_with_params(expr.as_ref(), params).map(Box::new))
                .transpose()?,
        }),
        SqlExpr::Subscript { expr, subscript } => match subscript.as_ref() {
            SqlSubscript::Index { index } => Ok(Expr::ArrayIndex {
                obj: translate_expr_with_params(expr, params).map(Box::new)?,
                indexes: vec![translate_expr_with_params(index, params)?],
            }),
            SqlSubscript::Slice { .. } => {
                Err(TranslateError::UnsupportedExpr(sql_expr.to_string()).into())
            }
        },
        SqlExpr::Array(Array { elem, .. }) => Ok(Expr::Array {
            elem: elem
                .iter()
                .map(|expr| translate_expr_with_params(expr, params))
                .collect::<Result<_>>()?,
        }),
        SqlExpr::Position { expr, r#in } => translate_position_with_params(params, expr, r#in),
        SqlExpr::Interval(SqlInterval {
            value,
            leading_field,
            last_field,
            ..
        }) => Ok(Expr::Interval {
            expr: translate_expr_with_params(value, params).map(Box::new)?,
            leading_field: leading_field
                .as_ref()
                .map(translate_datetime_field)
                .transpose()?,
            last_field: last_field
                .as_ref()
                .map(translate_datetime_field)
                .transpose()?,
        }),
        SqlExpr::Cast {
            expr,
            data_type,
            format,
            kind,
        } => translate_cast_with_params(params, kind, expr, data_type, format.as_ref()),

        _ => Err(TranslateError::UnsupportedExpr(sql_expr.to_string()).into()),
    }
}

/// Translates a [`SqlExpr`] into GlueSQL's [`Expr`] without external parameters.
///
/// # Errors
///
/// Returns an error when the SQL expression cannot be represented in GlueSQL,
/// such as when it contains unsupported syntax or literals.
pub fn translate_expr(sql_expr: &SqlExpr) -> Result<Expr> {
    const NO_PARAMS: [ParamLiteral; 0] = [];
    translate_expr_with_params(sql_expr, &NO_PARAMS)
}

/// Translates a [`SqlOrderByExpr`] into GlueSQL's [`OrderByExpr`] using the supplied parameters.
///
/// # Errors
///
/// Returns an error when the order-by expression uses syntax GlueSQL does not support
/// (for example `NULLS FIRST`/`NULLS LAST`) or when translating its sub-expressions fails.
pub(crate) fn translate_order_by_expr_with_params(
    sql_order_by_expr: &SqlOrderByExpr,
    params: &[ParamLiteral],
) -> Result<OrderByExpr> {
    let SqlOrderByExpr {
        expr,
        asc,
        nulls_first,
        ..
    } = sql_order_by_expr;

    if nulls_first.is_some() {
        return Err(TranslateError::OrderByNullsFirstOrLastNotSupported.into());
    }

    Ok(OrderByExpr {
        expr: translate_expr_with_params(expr, params)?,
        asc: *asc,
    })
}

/// Translates a [`SqlOrderByExpr`] into GlueSQL's [`OrderByExpr`] without parameters.
///
/// # Errors
///
/// Returns an error when the order-by expression uses unsupported syntax (such as
/// `NULLS FIRST`/`NULLS LAST`) or when translating inner expressions fails.
pub fn translate_order_by_expr(sql_order_by_expr: &SqlOrderByExpr) -> Result<OrderByExpr> {
    const NO_PARAMS: [ParamLiteral; 0] = [];
    translate_order_by_expr_with_params(sql_order_by_expr, &NO_PARAMS)
}
