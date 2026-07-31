mod derived;
mod dictionary;
mod series;
mod table;

use {
    super::{SelectedRows, SelectedSources, SourceColumns},
    crate::{
        data::Row, executor::context::RowContext, plan::SourcePlan, result::Result, store::GStore,
    },
    std::{borrow::Cow, rc::Rc},
};

pub(super) struct PreparedSource<'a> {
    pub(super) output: SourceColumns<'a>,
    rows: Box<dyn Fn(Option<Rc<RowContext<'a>>>) -> Result<SourceRows<'a>> + 'a>,
}

pub(super) struct SourceRows<'a> {
    source: SourceColumns<'a>,
    pub(super) rows: Box<dyn Iterator<Item = Result<Row>> + 'a>,
}

impl<'a> SourceRows<'a> {
    pub(super) fn into_selected(
        self,
        next_context: Option<Rc<RowContext<'a>>>,
    ) -> SelectedRows<'a> {
        let Self { source, rows } = self;
        let SourceColumns { alias, names } = source.clone();
        let rows = rows.map(move |row| {
            row.map(|mut row| {
                row.columns = Rc::clone(&names);
                Rc::new(RowContext::new(
                    alias,
                    Cow::Owned(row),
                    next_context.clone(),
                ))
            })
        });

        SelectedRows {
            sources: SelectedSources {
                base: source,
                joined: Vec::new(),
            },
            rows: Box::new(rows),
        }
    }
}

impl<'a> PreparedSource<'a> {
    pub(super) fn rows(
        &self,
        evaluation_context: Option<Rc<RowContext<'a>>>,
    ) -> Result<SourceRows<'a>> {
        (self.rows)(evaluation_context)
    }
}

pub(super) fn execute<'a, T: GStore>(
    storage: &'a T,
    source: &'a SourcePlan,
) -> Result<PreparedSource<'a>> {
    match source {
        SourcePlan::Table(table) => table::execute(storage, table),
        SourcePlan::Derived(derived) => derived::execute(storage, derived),
        SourcePlan::Series(series) => Ok(series::execute(series)),
        SourcePlan::Dictionary(dictionary) => Ok(dictionary::execute(storage, dictionary)),
    }
}
