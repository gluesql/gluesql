use {
    crate::{
        executor::{context::RowContext, fetch::fetch_relation_rows, join::Join},
        plan::{SelectPlan, TableWithJoinsPlan},
        result::Result,
        store::GStore,
    },
    std::{borrow::Cow, rc::Rc},
};

pub(super) type SelectedRows<'a> = Box<dyn Iterator<Item = Result<Rc<RowContext<'a>>>> + 'a>;

pub(super) fn execute<'a, T>(
    storage: &'a T,
    plan: &'a SelectPlan,
    filter_context: Option<&Rc<RowContext<'a>>>,
) -> Result<SelectedRows<'a>>
where
    T: GStore,
{
    let SelectPlan {
        from: table_with_joins,
    } = plan;

    let TableWithJoinsPlan { relation, joins } = table_with_joins;
    let rows = fetch_relation_rows(storage, relation, None)?.map(move |row| {
        let row = row?;
        let alias = relation.alias_name();

        Ok(RowContext::new(alias, Cow::Owned(row), None))
    });

    let join = Join::new(storage, joins, filter_context.cloned());
    join.apply(Box::new(rows))
}
