use {
    super::{
        context::{AggregateValues, RowContext},
        evaluate::evaluate,
    },
    crate::{
        plan::{CustomFunctionPlan, ExprPlan},
        result::Result,
        store::GStore,
    },
    std::rc::Rc,
};

pub fn check_expr<'a, T: GStore>(
    storage: &'a T,
    functions: &'a [CustomFunctionPlan],
    context: Option<&Rc<RowContext<'a>>>,
    aggregated: Option<&Rc<AggregateValues>>,
    expr: &'a ExprPlan,
) -> Result<bool> {
    evaluate(storage, functions, context, aggregated, expr).map(|evaluated| {
        if evaluated.is_null() {
            Ok(false)
        } else {
            evaluated.try_into()
        }
    })?
}
