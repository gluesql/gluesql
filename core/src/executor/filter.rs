use {
    super::{
        context::{AggregateValues, RowContext},
        evaluate::evaluate,
    },
    crate::{plan::ExprPlan, result::Result, store::GStore},
    std::rc::Rc,
};

pub fn check_expr<'a, T: GStore>(
    storage: &'a T,
    context: Option<&Rc<RowContext<'a>>>,
    aggregated: Option<&Rc<AggregateValues>>,
    expr: &'a ExprPlan,
) -> Result<bool> {
    evaluate(storage, context, aggregated, expr).map(|evaluated| {
        if evaluated.is_null() {
            Ok(false)
        } else {
            evaluated.try_into()
        }
    })?
}
