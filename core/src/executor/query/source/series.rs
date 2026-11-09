use {
    super::{
        super::{QueryError, SourceColumns},
        PreparedSource, SourceRows,
    },
    crate::{
        data::{Row, Value},
        executor::{
            context::{RowContext, StatementContext},
            evaluate::evaluate,
        },
        plan::SeriesSourcePlan,
        result::Result,
        store::GStore,
    },
    std::rc::Rc,
};

pub(super) fn execute<'a, T: GStore>(
    storage: &'a T,
    stmt_context: StatementContext<'a>,
    series: &'a SeriesSourcePlan,
) -> PreparedSource<'a> {
    let output = SourceColumns {
        alias: &series.alias.name,
        names: Rc::from(vec!["N".to_owned()]),
    };
    let source = SourceColumns {
        alias: output.alias,
        names: Rc::clone(&output.names),
    };
    let rows = Box::new(move |evaluation_context: Option<Rc<RowContext<'a>>>| {
        rows(
            storage,
            stmt_context,
            SourceColumns {
                alias: source.alias,
                names: Rc::clone(&source.names),
            },
            evaluation_context.as_ref(),
            series,
        )
    });

    PreparedSource { output, rows }
}

fn rows<'a, T: GStore>(
    storage: &'a T,
    stmt_context: StatementContext<'a>,
    source: SourceColumns<'a>,
    context: Option<&Rc<RowContext<'a>>>,
    series: &'a SeriesSourcePlan,
) -> Result<SourceRows<'a>> {
    let columns = Rc::clone(&source.names);
    let value: Value = evaluate(storage, stmt_context, context, None, &series.size)?.try_into()?;
    let size: i64 = value.try_into()?;
    if size < 0 {
        return Err(QueryError::InvalidSeriesSize(size).into());
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
        source,
        rows: Box::new(rows),
    })
}
