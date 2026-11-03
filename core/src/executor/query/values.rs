use {
    super::{LabeledRows, QueryError},
    crate::{
        data::{Row, Value},
        executor::{context::RowContext, evaluate::evaluate},
        plan::{CustomFunctionPlan, ValuesPlan},
        result::Result,
        store::GStore,
    },
    std::rc::Rc,
};

pub(super) struct MaterializedRows {
    pub(super) labels: Vec<String>,
    pub(super) rows: Vec<Row>,
}

pub(super) fn execute<'a, T: GStore>(
    storage: &'a T,
    functions: &'a [CustomFunctionPlan],
    plan: &'a ValuesPlan,
    filter_context: Option<&Rc<RowContext<'a>>>,
) -> Result<LabeledRows<'a>> {
    let MaterializedRows { labels, rows } = materialize(storage, functions, plan, filter_context)?;

    Ok(LabeledRows {
        labels,
        rows: Box::new(rows.into_iter().map(Ok)),
    })
}

pub(super) fn materialize<'a, T: GStore>(
    storage: &'a T,
    functions: &'a [CustomFunctionPlan],
    ValuesPlan(exprs_list): &'a ValuesPlan,
    context: Option<&Rc<RowContext<'a>>>,
) -> Result<MaterializedRows> {
    let first_len = exprs_list.first().map_or(0, Vec::len);
    let labels = labels_from_len(first_len);
    let columns = Rc::from(labels.clone());

    let mut column_types = vec![None; first_len];
    let mut rows = Vec::with_capacity(exprs_list.len());

    for exprs in exprs_list {
        if exprs.len() != first_len {
            return Err(QueryError::ValuesLengthMismatch.into());
        }

        let mut values = Vec::with_capacity(exprs.len());

        for (expr, column_type) in exprs.iter().zip(column_types.iter_mut()) {
            let evaluated = evaluate(storage, functions, context, None, expr)?;

            let value = if let Some(data_type) = column_type.as_ref() {
                evaluated.try_into_value(data_type, true)?
            } else {
                let value: Value = evaluated.try_into()?;
                *column_type = value.get_type();
                value
            };

            values.push(value);
        }

        rows.push(Row {
            columns: Rc::clone(&columns),
            values,
        });
    }

    Ok(MaterializedRows { labels, rows })
}

pub(super) fn labels(ValuesPlan(exprs_list): &ValuesPlan) -> Vec<String> {
    labels_from_len(exprs_list.first().map_or(0, Vec::len))
}

fn labels_from_len(len: usize) -> Vec<String> {
    (1..=len).map(|i| format!("column{i}")).collect()
}
