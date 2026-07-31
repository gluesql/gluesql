use {
    super::{
        aggregate::{try_visit_aggregate, visit_mut_aggregate},
        try_visit_expr, visit_mut_expr,
    },
    crate::plan::{ExprPlan, PlanError, WindowFunctionPlan, WindowPlan},
};

pub fn visit_mut_window<F>(window: &mut WindowPlan, f: &mut F)
where
    F: FnMut(&mut ExprPlan),
{
    for expr in &mut window.over.partition_by {
        visit_mut_expr(expr, f);
    }

    for order_by in &mut window.over.order_by {
        visit_mut_expr(&mut order_by.expr, f);
    }

    match &mut window.func {
        WindowFunctionPlan::RowNumber
        | WindowFunctionPlan::Rank
        | WindowFunctionPlan::DenseRank => {}
        WindowFunctionPlan::Lag {
            expr,
            offset,
            default,
        }
        | WindowFunctionPlan::Lead {
            expr,
            offset,
            default,
        } => {
            visit_mut_expr(expr, f);
            visit_mut_expr(offset, f);

            if let Some(default) = default {
                visit_mut_expr(default, f);
            }
        }
        WindowFunctionPlan::Aggregate(aggregate) => visit_mut_aggregate(aggregate, f),
    }
}

pub fn try_visit_window<F>(window: &WindowPlan, f: &mut F) -> Result<(), PlanError>
where
    F: FnMut(&ExprPlan) -> Result<(), PlanError>,
{
    for expr in &window.over.partition_by {
        try_visit_expr(expr, f)?;
    }

    for order_by in &window.over.order_by {
        try_visit_expr(&order_by.expr, f)?;
    }

    match &window.func {
        WindowFunctionPlan::RowNumber
        | WindowFunctionPlan::Rank
        | WindowFunctionPlan::DenseRank => {}
        WindowFunctionPlan::Lag {
            expr,
            offset,
            default,
        }
        | WindowFunctionPlan::Lead {
            expr,
            offset,
            default,
        } => {
            try_visit_expr(expr, f)?;
            try_visit_expr(offset, f)?;

            if let Some(default) = default {
                try_visit_expr(default, f)?;
            }
        }
        WindowFunctionPlan::Aggregate(aggregate) => try_visit_aggregate(aggregate, f)?,
    }

    Ok(())
}
