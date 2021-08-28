use {
    super::{
        ast_literal::translate_ast_literal,
        data_type::translate_data_type,
        function::translate_function,
        operator::{translate_binary_operator, translate_unary_operator},
        translate_idents, translate_query, TranslateError,
    },
    crate::{
        ast::{AstLiteral, Expr, OrderByExpr},
        result::Result,
    },
    sqlparser::ast::{Expr as SqlExpr, OrderByExpr as SqlOrderByExpr},
};

pub fn translate_expr(sql_expr: &SqlExpr) -> Result<Expr> {
    match sql_expr {
        SqlExpr::Identifier(ident) => match ident.quote_style {
            Some(_) => Ok(Expr::Literal(AstLiteral::QuotedString(
                ident.value.to_owned(),
            ))),
            None => Ok(Expr::Identifier(ident.value.to_owned())),
        },
        SqlExpr::Wildcard => Ok(Expr::Wildcard),
        SqlExpr::QualifiedWildcard(idents) => Ok(Expr::QualifiedWildcard(translate_idents(idents))),
        SqlExpr::CompoundIdentifier(idents) => {
            Ok(Expr::CompoundIdentifier(translate_idents(idents)))
        }
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
        SqlExpr::BinaryOp { left, op, right } => Ok(Expr::BinaryOp {
            left: translate_expr(left).map(Box::new)?,
            op: translate_binary_operator(op)?,
            right: translate_expr(right).map(Box::new)?,
        }),
        SqlExpr::UnaryOp { op, expr } => Ok(Expr::UnaryOp {
            op: translate_unary_operator(op)?,
            expr: translate_expr(expr).map(Box::new)?,
        }),
        SqlExpr::Cast { expr, data_type } => Ok(Expr::Cast {
            expr: translate_expr(expr).map(Box::new)?,
            data_type: translate_data_type(data_type)?,
        }),
        SqlExpr::Nested(expr) => translate_expr(expr).map(Box::new).map(Expr::Nested),
        SqlExpr::Value(value) => translate_ast_literal(value).map(Expr::Literal),
        SqlExpr::TypedString { data_type, value } => Ok(Expr::TypedString {
            data_type: translate_data_type(data_type)?,
            value: value.to_owned(),
        }),
        SqlExpr::Function(function) => translate_function(function),
        SqlExpr::Exists(query) => translate_query(query).map(Box::new).map(Expr::Exists),
        SqlExpr::Subquery(query) => translate_query(query).map(Box::new).map(Expr::Subquery),
        SqlExpr::Case {
            operand,
            conditions,
            results,
            else_result,
        } => Ok(Expr::Case {
            operand: translate_option_expr(operand)?,
            when_then: translate_and_zip(conditions, results)?,
            else_result: translate_option_expr(else_result)?,
        }),
        _ => Err(TranslateError::UnsupportedExpr(sql_expr.to_string()).into()),
    }
}

pub fn translate_and_zip(first: &[SqlExpr], second: &[SqlExpr]) -> Result<Vec<(Expr, Expr)>> {
    let first = first
        .iter()
        .map(translate_expr)
        .collect::<Result<Vec<_>>>()?;
    let second = second
        .iter()
        .map(translate_expr)
        .collect::<Result<Vec<_>>>()?;
    let result = first
        .into_iter()
        .zip(second.into_iter())
        .collect::<Vec<_>>();
    Ok(result)
}

pub fn translate_option_expr(sql_option_expr: &Option<Box<SqlExpr>>) -> Result<Option<Box<Expr>>> {
    match sql_option_expr {
        Some(expr) => match translate_expr(expr).map(Box::new) {
            Ok(expr) => Ok(Some(expr)),
            Err(_) => Err(TranslateError::UnsupportedExpr(expr.to_string()).into()),
        },
        None => Ok(None),
    }
}

pub fn translate_order_by_expr(sql_order_by_expr: &SqlOrderByExpr) -> Result<OrderByExpr> {
    let SqlOrderByExpr {
        expr,
        asc,
        nulls_first,
    } = sql_order_by_expr;

    if matches!(nulls_first, Some(_)) {
        return Err(TranslateError::OrderByNullsFirstOrLastNotSupported.into());
    }

    Ok(OrderByExpr {
        expr: translate_expr(expr)?,
        asc: *asc,
    })
}
