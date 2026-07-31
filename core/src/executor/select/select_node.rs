use {
    crate::{
        executor::{
            aggregate::{self, AggregateIter},
            context::RowContext,
            fetch::fetch_relation_rows,
            filter::Filter,
            join::Join,
        },
        plan::{SelectPlan, TableWithJoinsPlan},
        result::Result,
        store::GStore,
    },
    std::{borrow::Cow, rc::Rc},
};

pub(super) fn execute<'a, T>(
    storage: &'a T,
    plan: &'a SelectPlan,
    filter_context: Option<&Rc<RowContext<'a>>>,
) -> Result<AggregateIter<'a>>
where
    T: GStore,
{
    let SelectPlan {
        from: table_with_joins,
        selection: where_clause,
        group_by,
        having,
        aggregate_slots,
    } = plan;

    let TableWithJoinsPlan { relation, joins } = table_with_joins;
    let rows = fetch_relation_rows(storage, relation, None)?.map(move |row| {
        let row = row?;
        let alias = relation.alias_name();

        Ok(RowContext::new(alias, Cow::Owned(row), None))
    });

    let join = Join::new(storage, joins, filter_context.cloned());
    let filter = Rc::new(Filter::new(
        storage,
        where_clause.as_ref(),
        filter_context.cloned(),
    ));
    let rows = join.apply(Box::new(rows))?;
    let rows = rows.filter_map(move |project_context| {
        let project_context = match project_context {
            Ok(project_context) => project_context,
            Err(error) => return Some(Err(error)),
        };

        match filter.check(Rc::clone(&project_context)) {
            Ok(true) => Some(Ok(project_context)),
            Ok(false) => None,
            Err(error) => Some(Err(error)),
        }
    });

    aggregate::apply(
        storage,
        aggregate_slots.as_deref(),
        group_by,
        having.as_ref(),
        filter_context,
        Box::new(rows),
    )
}
