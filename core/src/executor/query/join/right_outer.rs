use {
    super::super::{
        QueryError, SelectedIter, SelectedRows, SelectedSources, SourceColumns, source,
    },
    super::{inner, left_outer, reject_unplanned_right_outer},
    crate::{
        data::{Key, Row, Value},
        executor::{context::RowContext, evaluate::evaluate, filter::check_expr},
        plan::{
            ExprPlan, HashJoinInputPlan, HashJoinPlan, JoinConditionInputPlan,
            NestedLoopJoinInputPlan, NestedLoopJoinPlan, RightOuterJoinInputPlan,
            RightOuterJoinPlan, SourcePlan,
        },
        result::Result,
        store::GStore,
    },
    std::{
        borrow::Cow,
        cell::RefCell,
        collections::{HashMap, HashSet},
        iter,
        rc::Rc,
    },
};

/// The left-driven [`super::JoinCandidates`] pipeline cannot answer "was this right row matched by
/// *any* left row?" — its groups are keyed by left row, and the right rows inside a group carry no
/// stable identity. So this executor drives the join itself, buffering the right relation to use
/// each row's position as its identity.
pub(crate) fn execute<'a, T: GStore>(
    storage: &'a T,
    plan: &'a RightOuterJoinPlan,
    filter_context: Option<&Rc<RowContext<'a>>>,
) -> Result<SelectedRows<'a>> {
    let (mechanism, condition) = split_input(&plan.input);
    let SelectedRows {
        mut sources,
        rows: left_rows,
    } = execute_left(storage, mechanism.input(), filter_context)?;

    let right = source::execute(storage, mechanism.right())?;
    let right_alias = right.output.alias;
    let right_names = Rc::clone(&right.output.names);

    // The *outer* query's context, not a per-left-row one, matching how `hash::build_rows` prepares
    // its side: a right-side subquery correlating to the left relation fails to resolve identifiers.
    let all_rows: Rc<[Row]> = right
        .rows(filter_context.map(Rc::clone))?
        .rows
        .map(|row| {
            row.map(|mut row| {
                row.columns = Rc::clone(&right_names);
                row
            })
        })
        .collect::<Result<Vec<_>>>()?
        .into();

    let lookup = Lookup::new(storage, mechanism, filter_context, right_alias, &all_rows)?;

    // Must run before the right source joins `sources`: the plan's relations describe the left side
    // alone, and `null_left_context` pairs them with `sources` by position.
    let null_left = null_left_context(&sources, &plan.null_extend.relations)?;
    sources.joined.push(SourceColumns {
        alias: right_alias,
        names: Rc::clone(&right_names),
    });

    let seen: Rc<RefCell<HashSet<usize>>> = Rc::new(RefCell::new(HashSet::new()));
    let outer_context = filter_context.map(Rc::clone);

    let matched = {
        let (seen, all_rows) = (Rc::clone(&seen), Rc::clone(&all_rows));

        left_rows.flat_map(move |left| {
            let left = match left {
                Ok(left) => left,
                Err(error) => return Box::new(iter::once(Err(error))) as SelectedIter<'a>,
            };
            let evaluation_context = match &outer_context {
                Some(outer) => Rc::new(RowContext::concat(Rc::clone(&left), Rc::clone(outer))),
                None => Rc::clone(&left),
            };

            let candidates = match lookup.candidates(storage, &evaluation_context, all_rows.len()) {
                Ok(candidates) => candidates,
                Err(error) => return Box::new(iter::once(Err(error))) as SelectedIter<'a>,
            };

            let (seen, all_rows) = (Rc::clone(&seen), Rc::clone(&all_rows));
            let outer_context = outer_context.as_ref().map(Rc::clone);

            Box::new(candidates.into_iter().filter_map(move |index| {
                let row = all_rows[index].clone();
                let row_context = Rc::new(RowContext::new(
                    right_alias,
                    Cow::Owned(row),
                    Some(Rc::clone(&left)),
                ));

                if let Some(expr) = condition {
                    let context = match &outer_context {
                        Some(outer) => Rc::new(RowContext::concat(
                            Rc::clone(&row_context),
                            Rc::clone(outer),
                        )),
                        None => Rc::clone(&row_context),
                    };

                    match check_expr(storage, Some(&context), None, expr) {
                        Ok(true) => {}
                        Ok(false) => return None,
                        Err(error) => return Some(Err(error)),
                    }
                }

                seen.borrow_mut().insert(index);

                Some(Ok(row_context))
            })) as SelectedIter<'a>
        })
    };

    let unmatched = (0..all_rows.len())
        .filter(move |index| !seen.borrow().contains(index))
        .map(move |index| {
            Ok(Rc::new(RowContext::new(
                right_alias,
                Cow::Owned(all_rows[index].clone()),
                null_left.as_ref().map(Rc::clone),
            )))
        });

    // `chain` does not touch `unmatched` until `matched` is exhausted, so `seen` is complete
    // by the time the unmatched pass reads it.
    Ok(SelectedRows {
        sources,
        rows: Box::new(matched.chain(unmatched)),
    })
}

#[derive(Clone, Copy)]
enum Mechanism<'a> {
    NestedLoop(&'a NestedLoopJoinPlan),
    Hash(&'a HashJoinPlan),
}

impl<'a> Mechanism<'a> {
    fn right(&self) -> &'a SourcePlan {
        match self {
            Self::NestedLoop(plan) => &plan.right,
            Self::Hash(plan) => &plan.right,
        }
    }

    fn input(&self) -> LeftInput<'a> {
        match self {
            Self::NestedLoop(plan) => LeftInput::NestedLoop(&plan.input),
            Self::Hash(plan) => LeftInput::Hash(&plan.input),
        }
    }
}

#[derive(Clone, Copy)]
enum LeftInput<'a> {
    NestedLoop(&'a NestedLoopJoinInputPlan),
    Hash(&'a HashJoinInputPlan),
}

fn split_input(input: &RightOuterJoinInputPlan) -> (Mechanism<'_>, Option<&ExprPlan>) {
    match input {
        RightOuterJoinInputPlan::NestedLoop(plan) => (Mechanism::NestedLoop(plan), None),
        RightOuterJoinInputPlan::Hash(plan) => (Mechanism::Hash(plan), None),
        RightOuterJoinInputPlan::Condition(condition) => {
            let mechanism = match &condition.input {
                JoinConditionInputPlan::NestedLoop(plan) => Mechanism::NestedLoop(plan),
                JoinConditionInputPlan::Hash(plan) => Mechanism::Hash(plan),
            };

            (mechanism, Some(&condition.expr))
        }
    }
}

fn execute_left<'a, T: GStore>(
    storage: &'a T,
    input: LeftInput<'a>,
    filter_context: Option<&Rc<RowContext<'a>>>,
) -> Result<SelectedRows<'a>> {
    match input {
        LeftInput::NestedLoop(NestedLoopJoinInputPlan::Source(source_plan))
        | LeftInput::Hash(HashJoinInputPlan::Source(source_plan)) => {
            Ok(source::execute(storage, source_plan)?
                .rows(None)?
                .into_selected(None))
        }
        LeftInput::NestedLoop(NestedLoopJoinInputPlan::InnerJoin(join))
        | LeftInput::Hash(HashJoinInputPlan::InnerJoin(join)) => {
            inner::execute(storage, join, filter_context)
        }
        LeftInput::NestedLoop(NestedLoopJoinInputPlan::LeftOuterJoin(join))
        | LeftInput::Hash(HashJoinInputPlan::LeftOuterJoin(join)) => {
            left_outer::execute(storage, join, filter_context)
        }
        LeftInput::NestedLoop(NestedLoopJoinInputPlan::RightOuterJoin(join))
        | LeftInput::Hash(HashJoinInputPlan::RightOuterJoin(join)) => {
            execute(storage, join, filter_context)
        }
        LeftInput::NestedLoop(NestedLoopJoinInputPlan::UnplannedRightOuterJoin(_)) => {
            reject_unplanned_right_outer()
        }
    }
}

enum Lookup<'a> {
    All,
    ByKey {
        index_map: HashMap<Key, Vec<usize>>,
        input_key: &'a ExprPlan,
    },
}

impl<'a> Lookup<'a> {
    fn new<T: GStore>(
        storage: &'a T,
        mechanism: Mechanism<'a>,
        filter_context: Option<&Rc<RowContext<'a>>>,
        right_alias: &'a str,
        all_rows: &[Row],
    ) -> Result<Self> {
        let plan = match mechanism {
            Mechanism::NestedLoop(_) => return Ok(Self::All),
            Mechanism::Hash(plan) => plan,
        };

        let mut index_map: HashMap<Key, Vec<usize>> = HashMap::new();
        for (index, row) in all_rows.iter().enumerate() {
            let context = Rc::new(RowContext::new(
                right_alias,
                Cow::Borrowed(row),
                filter_context.map(Rc::clone),
            ));

            let key: Key = evaluate(storage, Some(&context), None, &plan.right_key)?.try_into()?;
            if matches!(key, Key::None) {
                continue;
            }
            if let Some(expr) = &plan.right_filter
                && !check_expr(storage, Some(&context), None, expr)?
            {
                continue;
            }

            index_map.entry(key).or_default().push(index);
        }

        // Rows skipped above stay in `all_rows`, so the unmatched pass still surfaces them.
        Ok(Self::ByKey {
            index_map,
            input_key: &plan.input_key,
        })
    }

    fn candidates<T: GStore>(
        &self,
        storage: &T,
        evaluation_context: &Rc<RowContext<'_>>,
        total: usize,
    ) -> Result<Vec<usize>> {
        match self {
            Self::All => Ok((0..total).collect()),
            Self::ByKey {
                index_map,
                input_key,
            } => {
                let key: Key =
                    evaluate(storage, Some(evaluation_context), None, input_key)?.try_into()?;

                Ok(index_map.get(&key).cloned().unwrap_or_default())
            }
        }
    }
}

fn null_left_context<'a>(
    sources: &SelectedSources<'a>,
    relations: &[String],
) -> Result<Option<Rc<RowContext<'a>>>> {
    // `sources` still holds the left side alone, so it lines up positionally with the plan's
    // relations. Pairing by position keeps duplicate aliases — `A JOIN B AS A` — distinct.
    let left = iter::once(&sources.base)
        .chain(sources.joined.iter())
        .collect::<Vec<_>>();
    let mismatch = left.len() != relations.len()
        || left
            .iter()
            .zip(relations)
            .any(|(source, relation)| source.alias != relation);
    if mismatch {
        return Err(QueryError::UnreachableNullExtendRelationMismatch(relations.join(", ")).into());
    }

    Ok(left.into_iter().fold(None, |next, source| {
        let row = Row {
            columns: Rc::clone(&source.names),
            values: source.names.iter().map(|_| Value::Null).collect(),
        };

        Some(Rc::new(RowContext::new(
            source.alias,
            Cow::Owned(row),
            next,
        )))
    }))
}
