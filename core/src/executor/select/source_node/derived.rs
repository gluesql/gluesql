use {
    super::SourceRows,
    crate::{
        data::Row,
        executor::{
            context::RowContext,
            fetch::FetchError,
            select::{labels, select},
        },
        plan::DerivedSourcePlan,
        result::Result,
        store::GStore,
    },
    std::rc::Rc,
};

pub(super) fn execute<'a, T: GStore>(
    storage: &'a T,
    derived: &'a DerivedSourcePlan,
    evaluation_context: Option<&Rc<RowContext<'a>>>,
) -> Result<SourceRows<'a>> {
    let columns = columns(storage, derived)?;
    let rows = select(storage, &derived.query, evaluation_context.cloned())?.map({
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
        alias: &derived.alias.name,
        columns,
        rows: Box::new(rows),
    })
}

pub(super) fn columns<T: GStore>(storage: &T, derived: &DerivedSourcePlan) -> Result<Rc<[String]>> {
    let labels = labels(storage, &derived.query)?;
    let alias_columns = &derived.alias.columns;
    if alias_columns.len() > labels.len() {
        return Err(FetchError::TooManyColumnAliases(
            derived.alias.name.clone(),
            labels.len(),
            alias_columns.len(),
        )
        .into());
    }
    let columns = alias_columns
        .iter()
        .cloned()
        .chain(labels[alias_columns.len()..].iter().cloned())
        .collect::<Vec<_>>();

    Ok(Rc::from(columns))
}
