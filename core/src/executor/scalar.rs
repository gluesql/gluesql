use crate::plan::{ExprPlan, visit_mut_expr};

pub fn bind_scalar_references(alias: &str, expr: &mut ExprPlan) {
    visit_mut_expr(expr, &mut |expr| {
        if let ExprPlan::UnplannedReference { qualifier, name } = expr
            && qualifier
                .as_deref()
                .is_none_or(|qualifier| qualifier == alias)
        {
            *expr = ExprPlan::ResolvedColumn {
                alias: alias.to_owned(),
                column: name.clone(),
            };
        }
    });
}

#[cfg(test)]
mod tests {
    use {
        super::bind_scalar_references,
        crate::{
            parse_sql::parse_expr,
            plan::{ExprPlan, plan_scalar_expr},
            translate::{NO_PARAMS, translate_expr},
        },
    };

    #[test]
    fn binds_scalar_references_to_the_runtime_alias() {
        let parsed = parse_expr("price + add_tax.tax + other.tax").unwrap();
        let mut expr = plan_scalar_expr(translate_expr(&parsed, NO_PARAMS).unwrap());

        bind_scalar_references("add_tax", &mut expr);

        assert!(matches!(
            expr,
            ExprPlan::BinaryOp { left, right, .. }
                if matches!(&*left, ExprPlan::BinaryOp { left, right, .. }
                    if matches!(&**left, ExprPlan::ResolvedColumn { alias, column } if alias == "add_tax" && column == "price")
                    && matches!(&**right, ExprPlan::ResolvedColumn { alias, column } if alias == "add_tax" && column == "tax"))
                && matches!(&*right, ExprPlan::UnplannedReference { qualifier: Some(alias), name } if alias == "other" && name == "tax")
        ));
    }
}
