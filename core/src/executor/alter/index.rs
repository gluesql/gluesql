use {
    super::AlterError,
    crate::{
        ast::{ColumnDef, Expr, Function, OrderByExpr},
        data::Schema,
        result::Result,
        store::{GStore, GStoreMut},
    },
};

pub async fn create_index<T: GStore + GStoreMut>(
    storage: &mut T,
    table_name: &str,
    index_name: &str,
    column: &OrderByExpr,
) -> Result<()> {
    let expr = &column.expr;
    let Schema { column_defs, .. } = storage
        .fetch_schema(table_name)
        .await?
        .ok_or_else(|| AlterError::TableNotFound(table_name.to_owned()))?;
    let columns = column_defs
        .unwrap_or_default()
        .into_iter()
        .map(|ColumnDef { name, .. }| name)
        .collect::<Vec<_>>();

    let (valid, has_ident) = validate_index_expr(&columns, expr);
    if !valid {
        return Err(AlterError::UnsupportedIndexExpr(expr.clone()).into());
    } else if !has_ident {
        return Err(AlterError::IdentifierNotFound(expr.clone()).into());
    }

    storage.create_index(table_name, index_name, column).await
}

fn validate_index_expr(columns: &[String], expr: &Expr) -> (bool, bool) {
    let validate = |expr| validate_index_expr(columns, expr);

    match expr {
        Expr::Identifier(ident) => (columns.iter().any(|column| column == ident), true),
        Expr::Literal(_) | Expr::TypedString { .. } => (true, false),
        Expr::Nested(expr) => validate(expr),
        Expr::BinaryOp { left, right, .. } => {
            let (valid_l, has_ident_l) = validate(left);
            let (valid_r, has_ident_r) = validate(right);

            (valid_l && valid_r, has_ident_l || has_ident_r)
        }
        Expr::UnaryOp { expr, .. } => validate(expr),
        Expr::Function(func) => match func.as_ref() {
            Function::Cast { expr, .. } => validate(expr),
            _ => (false, false),
        },
        _ => (false, false),
    }
}
