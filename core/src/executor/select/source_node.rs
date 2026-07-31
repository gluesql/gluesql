mod derived;
mod dictionary;
mod series;
mod table;

use {
    super::SelectedRows,
    crate::{
        data::Row, executor::context::RowContext, plan::SourcePlan, result::Result, store::GStore,
    },
    std::{borrow::Cow, rc::Rc},
};

pub(super) struct SourceRows<'a> {
    pub(super) alias: &'a str,
    pub(super) columns: Rc<[String]>,
    pub(super) rows: Box<dyn Iterator<Item = Result<Row>> + 'a>,
}

impl<'a> SourceRows<'a> {
    pub(super) fn into_selected(
        self,
        next_context: Option<Rc<RowContext<'a>>>,
    ) -> SelectedRows<'a> {
        let Self {
            alias,
            columns,
            rows,
        } = self;
        let rows = rows.map(move |row| {
            row.map(|mut row| {
                row.columns = Rc::clone(&columns);
                Rc::new(RowContext::new(
                    alias,
                    Cow::Owned(row),
                    next_context.clone(),
                ))
            })
        });

        Box::new(rows)
    }
}

pub(super) fn execute<'a, T: GStore>(
    storage: &'a T,
    source: &'a SourcePlan,
    evaluation_context: Option<&Rc<RowContext<'a>>>,
) -> Result<SourceRows<'a>> {
    match source {
        SourcePlan::Table(table) => table::execute(storage, table, evaluation_context),
        SourcePlan::Derived(derived) => derived::execute(storage, derived, evaluation_context),
        SourcePlan::Series(series) => series::execute(series),
        SourcePlan::Dictionary(dictionary) => dictionary::execute(storage, dictionary),
    }
}

pub(super) fn columns<T: GStore>(storage: &T, source: &SourcePlan) -> Result<Rc<[String]>> {
    match source {
        SourcePlan::Table(table) => table::columns(storage, table),
        SourcePlan::Derived(derived) => derived::columns(storage, derived),
        SourcePlan::Series(series) => Ok(series::columns(series)),
        SourcePlan::Dictionary(dictionary) => Ok(dictionary::columns(dictionary)),
    }
}
