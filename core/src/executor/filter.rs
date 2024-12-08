use {
    super::{context::RowContext, evaluate::evaluate},
    crate::{
        ast::{Aggregate, Expr},
        data::{Nullable, Value},
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
}

impl<'a, T: GStore> Filter<'a, T> {
    pub fn new(
        storage: &'a T,
        where_clause: Option<&'a Expr>,
        context: Option<Rc<RowContext<'a>>>,
        aggregated: Option<Rc<HashMap<&'a Aggregate, Value>>>,
    ) -> Self {
        Self {
            storage,
            where_clause,
            context,
            aggregated,
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

                Ok(
                    match check_expr(self.storage, context, aggregated, expr).await? {
                        Nullable::Entry(value) => value,
                        // In a where, NULL is treated as false
                        Nullable::Null => false,
                    },
                )
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
) -> Result<Nullable<bool>> {
    evaluate(storage, context, aggregated, expr)
        .await
        .map(|evaluated| {
            if evaluated.is_null() {
                Ok(Nullable::Null)
            } else {
                let value: bool = evaluated.try_into()?;
                Ok(Nullable::Entry(value))
            }
        })?
}
