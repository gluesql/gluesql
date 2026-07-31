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
        executor::evaluate::evaluate_stateless,
        plan::{OrderByExprPlan, ValuesOrderByPlan},
        result::Result,
    },
};

pub(crate) fn execute<'a>(
    ValuesOrderByPlan { input, exprs }: &ValuesOrderByPlan,
) -> Result<LabeledRows<'a>> {
    let MaterializedRows { labels, rows } = values::materialize(input)?;
    let rows = sort(rows, exprs)?;

    Ok(LabeledRows {
        labels,
        rows: Box::new(rows.into_iter().map(Ok)),
    })
}

fn sort(rows: Vec<Row>, order_by: &[OrderByExprPlan]) -> Result<Vec<Row>> {
    let mut keyed_rows = Vec::with_capacity(rows.len());
    for row in rows {
        let keys = order_by
            .iter()
            .map(|OrderByExprPlan { expr, asc }| {
                evaluate_stateless(Some(row.as_context()), expr)
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
