use {
    super::SelectedRows,
    crate::{
        executor::{context::RowContext, fetch::fetch_relation_rows},
        plan::TableFactorPlan,
        result::Result,
        store::GStore,
    },
    std::{borrow::Cow, rc::Rc},
};

pub(super) fn execute<'a, T>(
    storage: &'a T,
    relation: &'a TableFactorPlan,
) -> Result<SelectedRows<'a>>
where
    T: GStore,
{
    let rows = fetch_relation_rows(storage, relation, None)?.map(move |row| {
        let row = row?;
        let alias = relation.alias_name();

        Ok(Rc::new(RowContext::new(alias, Cow::Owned(row), None)))
    });

    Ok(Box::new(rows))
}
