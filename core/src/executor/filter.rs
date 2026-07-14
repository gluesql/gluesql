use {
    super::{
        context::{AggregateValues, RowContext},
        evaluate::evaluate,
    },
    crate::{plan::ExprPlan, result::Result, store::GStore},
    std::rc::Rc,
};

pub struct Filter<'a, T: GStore> {
    storage: &'a T,
    where_clause: Option<&'a ExprPlan>,
    context: Option<Rc<RowContext<'a>>>,
}

impl<'a, T: GStore> Filter<'a, T> {
    pub fn new(
        storage: &'a T,
        where_clause: Option<&'a ExprPlan>,
        context: Option<Rc<RowContext<'a>>>,
    ) -> Self {
        Self {
            storage,
            where_clause,
            context,
        }
    }

    pub fn check(&self, project_context: Rc<RowContext<'a>>) -> Result<bool> {
        match self.where_clause {
            Some(expr) => {
                let context = match &self.context {
                    Some(context) => {
                        Rc::new(RowContext::concat(project_context, Rc::clone(context)))
                    }
                    None => project_context,
                };
                check_expr(self.storage, Some(&context), None, expr)
            }
            None => Ok(true),
        }
    }
}

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
