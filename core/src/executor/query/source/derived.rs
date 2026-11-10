mod output_labels;

use {
    super::{
        super::{QueryError, SourceColumns},
        PreparedSource, SourceRows,
    },
    crate::{
        data::Row,
        executor::{
            context::{RowContext, StatementContext},
            query,
        },
        plan::DerivedSourcePlan,
        result::Result,
        store::GStore,
    },
    std::rc::Rc,
};

pub(super) fn execute<'a, T: GStore>(
    storage: &'a T,
    stmt_context: StatementContext<'a>,
    derived: &'a DerivedSourcePlan,
) -> Result<PreparedSource<'a>> {
    let labels = output_labels::query(storage, stmt_context, &derived.query)?;
    let alias_columns = &derived.alias.columns;
    if alias_columns.len() > labels.len() {
        return Err(QueryError::TooManyColumnAliases(
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
            stmt_context,
            SourceColumns {
                alias: source.alias,
                names: Rc::clone(&source.names),
            },
            evaluation_context.as_ref(),
            derived,
        )
    });

    Ok(PreparedSource { output, rows })
}

fn rows<'a, T: GStore>(
    storage: &'a T,
    stmt_context: StatementContext<'a>,
    source: SourceColumns<'a>,
    evaluation_context: Option<&Rc<RowContext<'a>>>,
    derived: &'a DerivedSourcePlan,
) -> Result<SourceRows<'a>> {
    let columns = Rc::clone(&source.names);
    let rows = query::execute(
        storage,
        stmt_context,
        evaluation_context.map(Rc::clone),
        &derived.query,
    )?
    .map({
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
