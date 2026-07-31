use {
    super::SourceRows,
    crate::{
        data::{Row, Value},
        executor::{evaluate::evaluate_stateless, fetch::FetchError},
        plan::SeriesSourcePlan,
        result::Result,
    },
    std::rc::Rc,
};

pub(super) fn execute(series: &SeriesSourcePlan) -> Result<SourceRows<'_>> {
    let columns = columns(series);
    let value: Value = evaluate_stateless(None, &series.size)?.try_into()?;
    let size: i64 = value.try_into()?;
    if size < 0 {
        return Err(FetchError::SeriesSizeWrong(size).into());
    }
    let rows = (1..=size).map({
        let columns = Rc::clone(&columns);

        move |value| {
            Ok(Row {
                columns: Rc::clone(&columns),
                values: vec![Value::I64(value)],
            })
        }
    });

    Ok(SourceRows {
        alias: &series.alias.name,
        columns,
        rows: Box::new(rows),
    })
}

pub(super) fn columns(_series: &SeriesSourcePlan) -> Rc<[String]> {
    Rc::from(vec!["N".to_owned()])
}
