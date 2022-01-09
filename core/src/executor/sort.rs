use {
    super::{
        context::{AggregateContext, BlendContext, FilterContext},
        evaluate::evaluate,
    },
    crate::{
        ast::{Aggregate, OrderByExpr},
        data::Value,
        result::Result,
        store::GStore,
    },
    futures::stream::{self, Stream, StreamExt, TryStreamExt},
    im_rc::HashMap,
    std::{cmp::Ordering, fmt::Debug, pin::Pin, rc::Rc},
    utils::Vector,
};

pub struct Sort<'a, T: Debug> {
    storage: &'a dyn GStore<T>,
    context: Option<Rc<FilterContext<'a>>>,
    order_by: &'a [OrderByExpr],
}

type Item<'a> = Result<(
    Option<Rc<HashMap<&'a Aggregate, Value>>>,
    Rc<BlendContext<'a>>,
)>;

impl<'a, T: Debug> Sort<'a, T> {
    pub fn new(
        storage: &'a dyn GStore<T>,
        context: Option<Rc<FilterContext<'a>>>,
        order_by: &'a [OrderByExpr],
    ) -> Self {
        Self {
            storage,
            context,
            order_by,
        }
    }

    pub async fn apply(
        &self,
        rows: impl Stream<Item = Result<AggregateContext<'a>>> + 'a,
    ) -> Result<Pin<Box<dyn Stream<Item = Item<'a>> + 'a>>> {
        if self.order_by.is_empty() {
            let rows = rows.map_ok(|aggregate_context| {
                let AggregateContext { aggregated, next } = aggregate_context;

                (aggregated.map(Rc::new), next)
            });

            return Ok(Box::pin(rows));
        }

        let rows = rows
            .and_then(move |AggregateContext { aggregated, next }| async move {
                let blend_context = Rc::clone(&next);
                let filter_context = Rc::new(FilterContext::concat(
                    self.context.as_ref().map(Rc::clone),
                    Some(Rc::clone(&next)),
                ));
                let aggregated = aggregated.map(Rc::new);

                let values = stream::iter(self.order_by.iter())
                    .then(|OrderByExpr { expr, asc }| {
                        let context = Some(Rc::clone(&filter_context));
                        let aggregated = aggregated.as_ref().map(Rc::clone);

                        async move {
                            evaluate(self.storage, context, aggregated, expr)
                                .await?
                                .try_into()
                                .map(|value| (value, *asc))
                        }
                    })
                    .try_collect::<Vec<_>>()
                    .await?;

                Ok((values, aggregated, blend_context))
            })
            .try_collect::<Vec<_>>()
            .await
            .map(Vector::from)?
            .sort_by(|(values_a, _, _), (values_b, _, _)| {
                let pairs = values_a
                    .iter()
                    .map(|(a, _)| a)
                    .zip(values_b.iter())
                    .map(|(a, (b, asc))| (a, b, asc.unwrap_or(true)));

                for (value_a, value_b, asc) in pairs {
                    let apply_asc = |ord: Ordering| if asc { ord } else { ord.reverse() };

                    match (value_a, value_b) {
                        (Value::Null, Value::Null) => {}
                        (Value::Null, _) => {
                            return apply_asc(Ordering::Greater);
                        }
                        (_, Value::Null) => {
                            return apply_asc(Ordering::Less);
                        }
                        _ => {}
                    };

                    match value_a.partial_cmp(value_b) {
                        Some(ord) if ord != Ordering::Equal => {
                            return apply_asc(ord);
                        }
                        _ => {}
                    }
                }

                Ordering::Equal
            })
            .into_iter()
            .map(|(_, aggregated, blend_context)| Ok((aggregated, blend_context)));

        Ok(Box::pin(stream::iter(rows)))
    }
}
