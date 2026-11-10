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
        executor::{
            context::{RowContext, StatementContext},
            evaluate::evaluate,
        },
        plan::{OrderByExprPlan, ValuesOrderByPlan},
        result::Result,
        store::GStore,
    },
    std::rc::Rc,
};

pub(crate) fn execute<'a, T: GStore>(
    storage: &'a T,
    stmt_context: StatementContext<'a>,
    filter_context: Option<&Rc<RowContext<'a>>>,
    ValuesOrderByPlan { input, exprs }: &'a ValuesOrderByPlan,
) -> Result<LabeledRows<'a>> {
    let MaterializedRows { labels, rows } =
        values::materialize(storage, stmt_context, filter_context, input)?;
    let rows = sort(storage, stmt_context, rows, filter_context, exprs)?;

    Ok(LabeledRows {
        labels,
        rows: Box::new(rows.into_iter().map(Ok)),
    })
}

fn sort<T: GStore>(
    storage: &T,
    stmt_context: StatementContext<'_>,
    rows: Vec<Row>,
    filter_context: Option<&Rc<RowContext<'_>>>,
    order_by: &[OrderByExprPlan],
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
                evaluate(storage, stmt_context, Some(&context), None, expr)
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
