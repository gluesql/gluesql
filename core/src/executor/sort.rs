use bigdecimal::{BigDecimal, ToPrimitive};

use crate::{
    ast::{AstLiteral, Expr},
    executor::{context::BlendContext, evaluate_stateless},
    result::Error,
};

use {
    super::{
        context::{AggregateContext, FilterContext},
        evaluate::evaluate,
    },
    crate::{
        ast::OrderByExpr,
        data::{Row, Value},
        result::Result,
        store::GStore,
    },
    futures::stream::{self, Stream, StreamExt, TryStreamExt},
    std::{cmp::Ordering, rc::Rc},
    utils::Vector,
};

pub struct Sort<'a> {
    storage: &'a dyn GStore,
    context: Option<Rc<FilterContext<'a>>>,
    order_by: &'a [OrderByExpr],
}

impl<'a> Sort<'a> {
    pub fn new(
        storage: &'a dyn GStore,
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
        rows: impl Stream<Item = Result<(AggregateContext<'a>, Row)>> + 'a,
        labels: Vec<String>,
        table_alias: &'a String,
    ) -> Result<impl Stream<Item = Result<Row>> + 'a> {
        #[derive(futures_enum::Stream)]
        enum Rows<I1, I2> {
            NonOrderBy(I1),
            OrderBy(I2),
        }
        if self.order_by.is_empty() {
            let rows = rows.map_ok(|(_, row)| row);

            return Ok(Rows::NonOrderBy(Box::pin(rows)));
        }
        let labels: Rc<[String]> = Rc::from(labels);
        let rows = rows
            .and_then(|(AggregateContext { aggregated, next }, row)| {
                // let table_alias = next.get_table_alias();
                let labels = Rc::clone(&labels);
                let filter_context = Rc::new(FilterContext::concat(
                    self.context.as_ref().map(Rc::clone),
                    Some(Rc::clone(&next)),
                ));
                let label_context = BlendContext::new(table_alias, labels, Some(row.clone()), None);
                let filter_context = Rc::new(FilterContext::concat(
                    Some(filter_context),
                    Some(Rc::from(label_context)),
                ));
                let aggregated = aggregated.map(Rc::new);
                async move {
                    let values = stream::iter(self.order_by.iter())
                        .then(|OrderByExpr { expr, asc }| {
                            let context = Some(Rc::clone(&filter_context));
                            let aggregated = aggregated.as_ref().map(Rc::clone);
                            let row = row.clone();
                            async move {
                                if let Expr::Literal(AstLiteral::Number(big_decimal)) = expr {
                                    let value =
                                        row.get_value(big_decimal.to_usize().unwrap() - 1).unwrap();

                                    return Ok((value.clone(), *asc));
                                }
                                let value: Value =
                                    evaluate(self.storage, context, aggregated, expr)
                                        .await?
                                        .try_into()?;

                                // if let Value::I64(index) = value {
                                //     let index: usize = index.try_into().unwrap();
                                //     let index = index - 1;
                                //     println!("index: {index}");
                                //     let value = row.get_value(index).unwrap();

                                //     return Ok::<_, Error>((value.clone(), *asc));
                                // }

                                Ok::<_, Error>((value, *asc))
                            }
                        })
                        .try_collect::<Vec<_>>()
                        .await?;

                    Ok((values, row))
                }
            })
            .try_collect::<Vec<_>>()
            .await
            .map(Vector::from)?
            .sort_by(|(values_a, ..), (values_b, ..)| {
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
            .map(|(.., row)| Ok(row));

        Ok(Rows::OrderBy(stream::iter(rows)))
    }
}
