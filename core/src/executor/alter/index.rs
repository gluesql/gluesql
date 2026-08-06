use {
    super::AlterError,
    crate::{
        ast::{ColumnDef, OrderByExpr},
        data::Schema,
        plan::{ExprPlan, FunctionExprPlan, plan_scalar_expr},
        planner::plan_scalar_references,
        result::Result,
        store::{GStore, GStoreMut},
    },
};

pub fn create_index<T: GStore + GStoreMut>(
    storage: &mut T,
    table_name: &str,
    index_name: &str,
    column: &OrderByExpr,
) -> Result<()> {
    let mut expr = plan_scalar_expr(column.expr.clone());
    plan_scalar_references(table_name, &mut expr);
    let Schema { column_defs, .. } = storage
        .fetch_schema(table_name)?
        .ok_or_else(|| AlterError::TableNotFound(table_name.to_owned()))?;
    let columns = column_defs
        .unwrap_or_default()
        .into_iter()
        .map(|ColumnDef { name, .. }| name)
        .collect::<Vec<_>>();

    let (valid, has_ident) = validate_index_expr(&columns, &expr);
    if !valid {
        return Err(AlterError::UnsupportedIndexExpr.into());
    } else if !has_ident {
        return Err(AlterError::IndexExprRequiresColumnReference.into());
    }

    storage.create_index(table_name, index_name, column)
}

fn validate_index_expr(columns: &[String], expr: &ExprPlan) -> (bool, bool) {
    let validate = |expr| validate_index_expr(columns, expr);

    match expr {
        ExprPlan::UnplannedReference {
            qualifier: None,
            name: ident,
        }
        | ExprPlan::ResolvedColumn { column: ident, .. } => {
            (columns.iter().any(|column| column == ident), true)
        }
        ExprPlan::Literal(_) | ExprPlan::TypedString { .. } => (true, false),
        ExprPlan::Nested(expr) | ExprPlan::UnaryOp { expr, .. } => validate(expr),
        ExprPlan::BinaryOp { left, right, .. } => {
            let (valid_l, has_ident_l) = validate(left);
            let (valid_r, has_ident_r) = validate(right);

            (valid_l && valid_r, has_ident_l || has_ident_r)
        }
        ExprPlan::Function(func) => match func.as_ref() {
            FunctionExprPlan::Cast { expr, .. } => validate(expr),
            _ => (false, false),
        },
        _ => (false, false),
    }
}

#[cfg(test)]
mod tests {
    use super::validate_index_expr;
    use crate::plan::ExprPlan;

    #[test]
    fn validates_unqualified_reference() {
        let expr = ExprPlan::UnplannedReference {
            qualifier: None,
            name: "id".to_owned(),
        };
        assert_eq!(validate_index_expr(&["id".to_owned()], &expr), (true, true));
    }
}
