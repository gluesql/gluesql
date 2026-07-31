mod output_labels;

use {
    super::{super::SourceColumns, PreparedSource, SourceRows},
    crate::{
        data::Row,
        executor::{context::RowContext, fetch::FetchError, query},
        plan::DerivedSourcePlan,
        result::Result,
        store::GStore,
    },
    std::rc::Rc,
};

pub(super) fn execute<'a, T: GStore>(
    storage: &'a T,
    derived: &'a DerivedSourcePlan,
) -> Result<PreparedSource<'a>> {
    let labels = output_labels::query(storage, &derived.query)?;
    let alias_columns = &derived.alias.columns;
    if alias_columns.len() > labels.len() {
        return Err(FetchError::TooManyColumnAliases(
            derived.alias.name.clone(),
            labels.len(),
            alias_columns.len(),
        )
        .into());
    }
    let names = alias_columns
        .iter()
        .cloned()
        .chain(labels[alias_columns.len()..].iter().cloned())
        .collect::<Vec<_>>();

    let output = SourceColumns {
        alias: &derived.alias.name,
        names: Rc::from(names),
    };
    let source = SourceColumns {
        alias: output.alias,
        names: Rc::clone(&output.names),
    };
    let rows = Box::new(move |evaluation_context: Option<Rc<RowContext<'a>>>| {
        rows(
            storage,
            derived,
            SourceColumns {
                alias: source.alias,
                names: Rc::clone(&source.names),
            },
            evaluation_context.as_ref(),
        )
    });

    Ok(PreparedSource { output, rows })
}

fn rows<'a, T: GStore>(
    storage: &'a T,
    derived: &'a DerivedSourcePlan,
    source: SourceColumns<'a>,
    evaluation_context: Option<&Rc<RowContext<'a>>>,
) -> Result<SourceRows<'a>> {
    let columns = Rc::clone(&source.names);
    let rows = query::execute(storage, &derived.query, evaluation_context.map(Rc::clone))?.map({
        let columns = Rc::clone(&columns);

        move |row| {
            let row = row?;
            Ok(Row {
                columns: Rc::clone(&columns),
                values: row.values,
            })
        }
    });

    Ok(SourceRows {
        source,
        rows: Box::new(rows),
    })
}
