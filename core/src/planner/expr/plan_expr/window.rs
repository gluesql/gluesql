use crate::plan::{ExprPlan, WindowExprPlan, WindowFunctionPlan};

impl WindowExprPlan {
    /// Every expression owned directly by this window's `OVER` clause
    /// (`PARTITION BY`/`ORDER BY`) and its own function arguments — `LAG`/
    /// `LEAD`'s `expr`/`offset`/`default`, or the argument of its aggregate.
    ///
    /// The single source of truth for window child-expression enumeration,
    /// shared by the read-only sites that need it (`PlanExpr::from`,
    /// schema-dependency scanning). Nesting checks and mutable rewrites walk
    /// the same children via `try_visit_window`/`visit_mut_window` instead,
    /// which additionally recurse into subqueries and nested expressions.
    pub fn as_exprs(&self) -> Vec<&ExprPlan> {
        let Self { func, over, .. } = self;

        let mut exprs: Vec<&ExprPlan> = over.partition_by.iter().collect();
        exprs.extend(over.order_by.iter().map(|order_by| &order_by.expr));

        match func {
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
                exprs.push(expr);
                exprs.push(offset);
                exprs.extend(default.iter());
            }
            WindowFunctionPlan::Aggregate(aggregate) => exprs.extend(aggregate.as_expr()),
        }

        exprs
    }
}
