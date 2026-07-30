use {
    super::{LabeledRows, project::Project},
    crate::{
        data::Row,
        executor::{
            aggregate,
            context::{AggregateContext, AggregateValues, RowContext},
            fetch::{fetch_labels, fetch_relation_rows},
            filter::Filter,
            join::Join,
        },
        plan::{SelectPlan, TableWithJoinsPlan},
        result::Result,
        store::GStore,
    },
    std::{borrow::Cow, rc::Rc},
};

type ProjectedRow<'a> = (Option<Rc<AggregateValues>>, Option<Rc<RowContext<'a>>>, Row);
type ProjectedIter<'a> = Box<dyn Iterator<Item = Result<ProjectedRow<'a>>> + 'a>;

pub(super) struct ProjectedRows<'a> {
    pub(super) labels: Vec<String>,
    pub(super) rows: ProjectedIter<'a>,
    pub(super) table_alias: &'a str,
}

pub(super) fn execute<'a, T>(
    storage: &'a T,
    plan: &'a SelectPlan,
    filter_context: Option<Rc<RowContext<'a>>>,
) -> Result<LabeledRows<'a>>
where
    T: GStore,
{
    let ProjectedRows { labels, rows, .. } = project(storage, plan, filter_context)?;
    let rows = rows.map(|row| row.map(|(.., row)| row));

    Ok(LabeledRows {
        labels,
        rows: Box::new(rows),
    })
}

pub(super) fn project<'a, T>(
    storage: &'a T,
    plan: &'a SelectPlan,
    filter_context: Option<Rc<RowContext<'a>>>,
) -> Result<ProjectedRows<'a>>
where
    T: GStore,
{
    let SelectPlan {
        from: table_with_joins,
        selection: where_clause,
        projection,
        group_by,
        having,
        aggregate_slots,
    } = plan;

    let TableWithJoinsPlan { relation, joins } = &table_with_joins;
    let rows = fetch_relation_rows(storage, relation, None)?.map(move |row| {
        let row = row?;
        let alias = relation.alias_name();

        Ok(RowContext::new(alias, Cow::Owned(row), None))
    });

    let join = Join::new(storage, joins, filter_context.as_ref().map(Rc::clone));
    let filter = Rc::new(Filter::new(
        storage,
        where_clause.as_ref(),
        filter_context.as_ref().map(Rc::clone),
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

    let rows = aggregate::apply(
        storage,
        aggregate_slots.as_deref(),
        group_by,
        having.as_ref(),
        filter_context.as_ref(),
        Box::new(rows),
    )?;

    let labels = fetch_labels(storage, relation, joins, projection)?;
    let labels = Rc::from(labels);
    let project = Rc::new(Project::new(storage, filter_context, projection));
    let project_labels = Rc::clone(&labels);
    let rows = rows.map(move |aggregate_context| {
        let aggregate_context = aggregate_context?;
        let project = Rc::clone(&project);
        let AggregateContext { aggregated, next } = aggregate_context;

        let row = project.apply(aggregated.as_ref(), &project_labels, next.as_ref())?;

        Ok((aggregated, next, row))
    });

    let labels = labels.iter().cloned().collect();

    Ok(ProjectedRows {
        labels,
        rows: Box::new(rows),
        table_alias: relation.alias_name(),
    })
}
