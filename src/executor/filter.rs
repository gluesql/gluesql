use {
    super::{
        context::{BlendContext, FilterContext},
        evaluate::evaluate,
    },
    crate::{
        ast::{Aggregate, Expr},
        data::Value,
        result::Result,
        store::GStore,
    },
    im_rc::HashMap,
    std::{fmt::Debug, rc::Rc},
};

pub struct Filter<'a, T: 'static + Debug> {
    storage: &'a dyn GStore<T>,
    where_clause: Option<&'a Expr>,
    context: Option<Rc<FilterContext<'a>>>,
    aggregated: Option<Rc<HashMap<&'a Aggregate, Value>>>,
}

impl<'a, T: 'static + Debug> Filter<'a, T> {
    pub fn new(
        storage: &'a dyn GStore<T>,
        where_clause: Option<&'a Expr>,
        context: Option<Rc<FilterContext<'a>>>,
        aggregated: Option<Rc<HashMap<&'a Aggregate, Value>>>,
    ) -> Self {
        Self {
            storage,
            where_clause,
            context,
            aggregated,
        }
    }

    pub async fn check(&self, blend_context: Rc<BlendContext<'a>>) -> Result<bool> {
        match self.where_clause {
            Some(expr) => {
                let context = self.context.as_ref().map(Rc::clone);
                let context = FilterContext::concat(context, Some(blend_context));
                let context = Some(context).map(Rc::new);
                let aggregated = self.aggregated.as_ref().map(Rc::clone);

                check_expr(self.storage, context, aggregated, expr).await
            }
            None => Ok(true),
        }
    }
}

pub async fn check_expr<'a, T: 'static + Debug>(
    storage: &'a dyn GStore<T>,
    context: Option<Rc<FilterContext<'a>>>,
    aggregated: Option<Rc<HashMap<&'a Aggregate, Value>>>,
    expr: &'a Expr,
) -> Result<bool> {
    evaluate(storage, context, aggregated, expr)
        .await
        .map(|evaluated| evaluated.try_into())?
}
