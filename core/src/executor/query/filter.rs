use {
    super::{
        SelectedRows,
        join::{inner, left_outer},
        source,
    },
    crate::{
        executor::{context::RowContext, filter::check_expr},
        plan::{FilterInputPlan, FilterPlan},
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
    let SelectedRows { sources, rows } = match input {
        FilterInputPlan::Source(source) => source::execute(storage, source)?
            .rows(None)?
            .into_selected(None),
        FilterInputPlan::InnerJoin(join) => inner::execute(storage, join, filter_context)?,
        FilterInputPlan::LeftOuterJoin(join) => left_outer::execute(storage, join, filter_context)?,
    };
    let filter_context = filter_context.map(Rc::clone);
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

    Ok(SelectedRows {
        sources,
        rows: Box::new(rows),
    })
}
