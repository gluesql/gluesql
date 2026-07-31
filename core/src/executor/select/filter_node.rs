use {
    super::select_node::{self, SelectedRows},
    crate::{
        executor::{context::RowContext, filter::check_expr},
        plan::FilterPlan,
        result::Result,
        store::GStore,
    },
    std::rc::Rc,
};

pub(super) fn execute<'a, T>(
    storage: &'a T,
    plan: &'a FilterPlan,
    filter_context: Option<&Rc<RowContext<'a>>>,
) -> Result<SelectedRows<'a>>
where
    T: GStore,
{
    let FilterPlan { input, expr } = plan;
    let rows = select_node::execute(storage, input, filter_context)?;
    let filter_context = filter_context.cloned();
    let rows = rows.filter_map(move |context| {
        let context = match context {
            Ok(context) => context,
            Err(error) => return Some(Err(error)),
        };
        let evaluate_context = match &filter_context {
            Some(filter_context) => Some(Rc::new(RowContext::concat(
                Rc::clone(&context),
                Rc::clone(filter_context),
            ))),
            None => Some(Rc::clone(&context)),
        };

        match check_expr(storage, evaluate_context.as_ref(), None, expr) {
            Ok(true) => Some(Ok(context)),
            Ok(false) => None,
            Err(error) => Some(Err(error)),
        }
    });

    Ok(Box::new(rows))
}
