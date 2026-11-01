use {
    super::try_visit_mut_query,
    crate::{
        ast::{Expr, OperateFunctionArg},
        executor::{AlterError, evaluate_stateless},
        plan::{CustomFunctionPlan, ExprPlan, FunctionArgPlan, SourcePlan},
        planner::{PlannerError, expr::try_visit_mut_expr},
        result::Result,
    },
};

pub fn validate(
    name: &str,
    args: &[OperateFunctionArg],
    body: &Expr,
) -> Result<CustomFunctionPlan> {
    for (index, arg) in args.iter().enumerate() {
        if args[index + 1..].iter().any(|other| arg.name == other.name) {
            return Err(AlterError::DuplicateArgName(arg.name.clone()).into());
        }
    }
    let args = args
        .iter()
        .cloned()
        .map(FunctionArgPlan::from)
        .collect::<Vec<_>>();
    for default in args.iter().filter_map(|arg| arg.default.as_ref()) {
        evaluate_stateless(None, default)?;
    }
    if !args
        .iter()
        .skip_while(|arg| arg.default.is_none())
        .all(|arg| arg.default.is_some())
    {
        return Err(AlterError::NonDefaultArgumentFollowsDefaultArgument.into());
    }

    let mut body = ExprPlan::from(body.clone());
    validate_body(&mut body)?;
    Ok(CustomFunctionPlan {
        name: name.to_owned(),
        args,
        body,
    })
}

fn validate_body(expr: &mut ExprPlan) -> Result<()> {
    try_visit_mut_expr(expr, &mut |expr| match expr {
        ExprPlan::Subquery(query)
        | ExprPlan::Exists {
            subquery: query, ..
        }
        | ExprPlan::InSubquery {
            subquery: query, ..
        } => try_visit_mut_query(query, &mut validate_body, &mut |source| match source {
            SourcePlan::Table(_) | SourcePlan::Dictionary(_) => {
                Err(PlannerError::StorageAccessInFunctionBody.into())
            }
            SourcePlan::Derived(_) | SourcePlan::Series(_) => Ok(()),
        }),
        _ => Ok(()),
    })
}
