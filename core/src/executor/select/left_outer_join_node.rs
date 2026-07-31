use {
    super::{
        JoinCandidates, SelectedIter, SelectedRows, hash_join_node, join_condition_node,
        nested_loop_join_node,
    },
    crate::{
        data::{Row, Value},
        executor::context::RowContext,
        plan::{LeftOuterJoinInputPlan, LeftOuterJoinPlan},
        result::Result,
        store::GStore,
    },
    std::{borrow::Cow, rc::Rc},
};

pub(super) fn execute<'a, T: GStore>(
    storage: &'a T,
    plan: &'a LeftOuterJoinPlan,
    filter_context: Option<&Rc<RowContext<'a>>>,
) -> Result<SelectedRows<'a>> {
    let JoinCandidates {
        sources,
        right,
        groups,
    } = match &plan.input {
        LeftOuterJoinInputPlan::NestedLoop(join) => {
            nested_loop_join_node::execute(storage, join, filter_context)?
        }
        LeftOuterJoinInputPlan::Hash(join) => {
            hash_join_node::execute(storage, join, filter_context)?
        }
        LeftOuterJoinInputPlan::Condition(condition) => {
            join_condition_node::execute(storage, condition, filter_context)?
        }
    };
    let rows = groups.flat_map(move |group| {
        let group = match group {
            Ok(group) => group,
            Err(error) => return Box::new(std::iter::once(Err(error))) as SelectedIter<'a>,
        };
        let row = Row {
            columns: Rc::clone(&right.names),
            values: right.names.iter().map(|_| Value::Null).collect(),
        };
        let fallback = Rc::new(RowContext::new(
            right.alias,
            Cow::Owned(row),
            Some(group.left),
        ));

        Box::new(LeftOuter {
            rows: group.rows,
            fallback: Some(fallback),
            yielded: false,
        })
    });

    Ok(SelectedRows {
        sources,
        rows: Box::new(rows),
    })
}

struct LeftOuter<'a> {
    rows: SelectedIter<'a>,
    fallback: Option<Rc<RowContext<'a>>>,
    yielded: bool,
}

impl<'a> Iterator for LeftOuter<'a> {
    type Item = Result<Rc<RowContext<'a>>>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.rows.next() {
            Some(row) => {
                self.yielded = true;
                Some(row)
            }
            None if !self.yielded => self.fallback.take().map(Ok),
            None => None,
        }
    }
}
