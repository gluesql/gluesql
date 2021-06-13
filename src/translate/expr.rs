use {
    super::{
        ast_literal::translate_ast_literal,
        data_type::translate_data_type,
        function::translate_function,
        operator::{translate_binary_operator, translate_unary_operator},
        translate_idents, translate_query, TranslateError,
    },
    crate::{
        ast::{AstLiteral, Expr},
        result::Result,
    },
    sqlparser::ast::Expr as SqlExpr,
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
        _ => Err(TranslateError::UnsupportedExpr(sql_expr.to_string()).into()),
    }
}
