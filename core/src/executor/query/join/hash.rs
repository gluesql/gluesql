use {
    super::super::{SelectedRows, SourceColumns, source},
    super::{JoinCandidateGroup, JoinCandidates, inner, left_outer},
    crate::{
        data::{Key, Row},
        executor::{context::RowContext, evaluate::evaluate, filter::check_expr},
        plan::{HashJoinInputPlan, HashJoinPlan},
        result::Result,
        store::GStore,
    },
    itertools::Itertools,
    std::{borrow::Cow, collections::HashMap, rc::Rc},
};

pub(super) fn execute<'a, T: GStore>(
    storage: &'a T,
    plan: &'a HashJoinPlan,
    filter_context: Option<&Rc<RowContext<'a>>>,
) -> Result<JoinCandidates<'a>> {
    let HashJoinPlan {
        input,
        right,
        input_key,
        right_key,
        right_filter,
    } = plan;
    let SelectedRows { mut sources, rows } = match input {
        HashJoinInputPlan::Source(source_plan) => source::execute(storage, source_plan)?
            .rows(None)?
            .into_selected(None),
        HashJoinInputPlan::InnerJoin(join) => inner::execute(storage, join, filter_context)?,
        HashJoinInputPlan::LeftOuterJoin(join) => {
            left_outer::execute(storage, join, filter_context)?
        }
    };
    let right = source::execute(storage, right)?;
    let (rows_map, right_source) = build_rows(
        storage,
        right,
        filter_context,
        right_key,
        right_filter.as_ref(),
    )?;
    sources.joined.push(SourceColumns {
        alias: right_source.alias,
        names: Rc::clone(&right_source.names),
    });
    let right_alias = right_source.alias;
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
        let rows = evaluate(storage, evaluation_context.as_ref(), None, input_key)
            .and_then(|evaluated| Key::try_from(evaluated).map(|key| rows_map.get(&key).cloned()));
        let rows: Box<dyn Iterator<Item = Result<Row>> + 'a> = match rows {
            Ok(Some(rows)) => Box::new(rows.into_iter().map(Ok)),
            Ok(None) => Box::new(std::iter::empty()),
            Err(error) => Box::new(std::iter::once(Err(error))),
        };

        Ok(candidate_group(left, right_alias, rows))
    });

    Ok(JoinCandidates {
        sources,
        right: right_source,
        groups: Box::new(groups),
    })
}

fn build_rows<'a, T: GStore>(
    storage: &'a T,
    source: source::PreparedSource<'a>,
    filter_context: Option<&Rc<RowContext<'a>>>,
    right_key: &'a crate::plan::ExprPlan,
    right_filter: Option<&'a crate::plan::ExprPlan>,
) -> Result<(HashMap<Key, Vec<Row>>, SourceColumns<'a>)> {
    let mut rows = Vec::new();
    for row in source.rows(filter_context.map(Rc::clone))?.rows {
        let row = row?;
        let context = Rc::new(RowContext::new(
            source.output.alias,
            Cow::Borrowed(&row),
            filter_context.map(Rc::clone),
        ));
        let key: Key = evaluate(storage, Some(&context), None, right_key)?.try_into()?;
        if matches!(key, Key::None) {
            continue;
        }
        let pass = match right_filter {
            Some(expr) => check_expr(storage, Some(&context), None, expr)?,
            None => true,
        };
        if pass {
            rows.push((key, row));
        }
    }

    Ok((rows.into_iter().into_group_map(), source.output))
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
