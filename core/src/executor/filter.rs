use {
    super::{context::RowContext, evaluate::evaluate},
    crate::{
        ast::{Aggregate, Expr},
        data::Value,
        result::Result,
        store::GStore,
    },
    im_rc::HashMap,
    std::rc::Rc,
};

pub struct Filter<'a, T: GStore> {
    storage: &'a T,
    where_clause: Option<&'a Expr>,
    context: Option<Rc<RowContext<'a>>>,
    aggregated: Option<Rc<HashMap<&'a Aggregate, Value>>>,
    /// Whether to consider as 'true' the evaluations that result in NULL.
    ///
    /// Taken directly from the PostgreSQL documentation:
    ///
    /// It should be noted that a check constraint is satisfied if
    /// the check expression evaluates to true or the null value.
    /// Since most expressions will evaluate to the null value if
    /// any operand is null, they will not prevent null values
    /// in the constrained columns.
    allow_null_evaluations: bool,
}

impl<'a, T: GStore> Filter<'a, T> {
    pub fn new(
        storage: &'a T,
        where_clause: Option<&'a Expr>,
        context: Option<Rc<RowContext<'a>>>,
        aggregated: Option<Rc<HashMap<&'a Aggregate, Value>>>,
        allow_null_evaluations: bool,
    ) -> Self {
        Self {
            storage,
            where_clause,
            context,
            aggregated,
            allow_null_evaluations,
        }
    }

    pub async fn check(&self, project_context: Rc<RowContext<'a>>) -> Result<bool> {
        match self.where_clause {
            Some(expr) => {
                let context = match &self.context {
                    Some(context) => {
                        Rc::new(RowContext::concat(project_context, Rc::clone(context)))
                    }
                    None => project_context,
                };
                let context = Some(context);
                let aggregated = self.aggregated.as_ref().map(Rc::clone);

                Ok(check_expr(self.storage, context, aggregated, expr)
                    .await?
                    .unwrap_or(self.allow_null_evaluations))
            }
            None => Ok(true),
        }
    }
}

pub async fn check_expr<'a, T: GStore>(
    storage: &'a T,
    context: Option<Rc<RowContext<'a>>>,
    aggregated: Option<Rc<HashMap<&'a Aggregate, Value>>>,
    expr: &'a Expr,
) -> Result<Option<bool>> {
    evaluate(storage, context, aggregated, expr)
        .await
        .map(|evaluated| evaluated.try_into())?
}
