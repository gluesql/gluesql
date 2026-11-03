use {
    super::{
        super::{
            LabeledRows,
            values::{self, MaterializedRows},
        },
        sort_by,
    },
    crate::{
        data::{Key, Row, Value},
        executor::{context::RowContext, evaluate::evaluate},
        plan::{CustomFunctionPlan, OrderByExprPlan, ValuesOrderByPlan},
        result::Result,
        store::GStore,
    },
    std::rc::Rc,
};

pub(crate) fn execute<'a, T: GStore>(
    storage: &'a T,
    functions: &'a [CustomFunctionPlan],
    ValuesOrderByPlan { input, exprs }: &'a ValuesOrderByPlan,
    filter_context: Option<&Rc<RowContext<'a>>>,
) -> Result<LabeledRows<'a>> {
    let MaterializedRows { labels, rows } =
        values::materialize(storage, functions, input, filter_context)?;
    let rows = sort(storage, functions, rows, exprs, filter_context)?;

    Ok(LabeledRows {
        labels,
        rows: Box::new(rows.into_iter().map(Ok)),
    })
}

fn sort<T: GStore>(
    storage: &T,
    functions: &[CustomFunctionPlan],
    rows: Vec<Row>,
    order_by: &[OrderByExprPlan],
    filter_context: Option<&Rc<RowContext<'_>>>,
) -> Result<Vec<Row>> {
    let mut keyed_rows = Vec::with_capacity(rows.len());
    for row in rows {
        let context = Rc::new(row.as_context());
        let context = match filter_context {
            Some(filter_context) => Rc::new(RowContext::concat(context, Rc::clone(filter_context))),
            None => context,
        };
        let keys = order_by
            .iter()
            .map(|OrderByExprPlan { expr, asc }| {
                evaluate(storage, functions, Some(&context), None, expr)
                    .and_then(Value::try_from)
                    .and_then(Key::try_from)
                    .map(|key| (key, *asc))
            })
            .collect::<Result<Vec<_>>>()?;

        keyed_rows.push((keys, row));
    }

    keyed_rows.sort_by(|(keys_a, _), (keys_b, _)| sort_by(keys_a, keys_b));

    let sorted = keyed_rows
        .into_iter()
        .map(|(_, row)| row)
        .collect::<Vec<_>>();

    Ok(sorted)
}
