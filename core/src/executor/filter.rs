use {
    super::{
        context::{AggregateValues, RowContext},
        evaluate::evaluate,
    },
    crate::{plan::ExprPlan, result::Result, store::GStore},
    std::sync::Arc,
};

pub struct Filter<'a, T: GStore> {
    storage: &'a T,
    where_clause: Option<&'a ExprPlan>,
    context: Option<Arc<RowContext<'a>>>,
}

impl<'a, T: GStore> Filter<'a, T> {
    pub fn new(
        storage: &'a T,
        where_clause: Option<&'a ExprPlan>,
        context: Option<Arc<RowContext<'a>>>,
    ) -> Self {
        Self {
            storage,
            where_clause,
            context,
        }
    }

    pub fn check(&self, project_context: Arc<RowContext<'a>>) -> Result<bool> {
        match self.where_clause {
            Some(expr) => {
                let context = match &self.context {
                    Some(context) => {
                        Arc::new(RowContext::concat(project_context, Arc::clone(context)))
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
    context: Option<&Arc<RowContext<'a>>>,
    aggregated: Option<&Arc<AggregateValues>>,
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
