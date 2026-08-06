use {
    super::super::{SelectedRows, SourceColumns, source},
    super::{JoinCandidateGroup, JoinCandidates, inner, left_outer},
    crate::{
        data::Row,
        executor::context::RowContext,
        plan::{NestedLoopJoinInputPlan, NestedLoopJoinPlan},
        result::Result,
        store::GStore,
    },
    std::{borrow::Cow, rc::Rc},
};

pub(super) fn execute<'a, T: GStore>(
    storage: &'a T,
    plan: &'a NestedLoopJoinPlan,
    filter_context: Option<&Rc<RowContext<'a>>>,
) -> Result<JoinCandidates<'a>> {
    let NestedLoopJoinPlan { input, right } = plan;
    let SelectedRows { mut sources, rows } = match input {
        NestedLoopJoinInputPlan::Source(source_plan) => source::execute(storage, source_plan)?
            .rows(None)?
            .into_selected(None),
        NestedLoopJoinInputPlan::InnerJoin(join) => inner::execute(storage, join, filter_context)?,
        NestedLoopJoinInputPlan::LeftOuterJoin(join) => {
            left_outer::execute(storage, join, filter_context)?
        }
    };
    let right = source::execute(storage, right)?;
    let output = SourceColumns {
        alias: right.output.alias,
        names: Rc::clone(&right.output.names),
    };
    sources.joined.push(SourceColumns {
        alias: right.output.alias,
        names: Rc::clone(&right.output.names),
    });
    let right_alias = right.output.alias;
    let filter_context = filter_context.map(Rc::clone);
    let groups = rows.map(move |left| {
        let left = left?;
        let evaluation_context = match &filter_context {
            Some(filter_context) => Some(Rc::new(RowContext::concat(
                Rc::clone(&left),
                Rc::clone(filter_context),
            ))),
            None => Some(Rc::clone(&left)),
        };
        let rows = right.rows(evaluation_context)?.rows;

        Ok(candidate_group(left, right_alias, rows))
    });

    Ok(JoinCandidates {
        sources,
        right: output,
        groups: Box::new(groups),
    })
}

fn candidate_group<'a>(
    left: Rc<RowContext<'a>>,
    right_alias: &'a str,
    rows: impl Iterator<Item = Result<Row>> + 'a,
) -> JoinCandidateGroup<'a> {
    let left_context = Rc::clone(&left);
    let rows = rows.map(move |row| {
        row.map(|row| {
            Rc::new(RowContext::new(
                right_alias,
                Cow::Owned(row),
                Some(Rc::clone(&left_context)),
            ))
        })
    });

    JoinCandidateGroup {
        left,
        rows: Box::new(rows),
    }
}
