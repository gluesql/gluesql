use {
    super::super::{SelectedIter, SelectedRows},
    super::{JoinCandidates, condition, hash, nested_loop},
    crate::{
        data::{Row, Value},
        executor::context::{RowContext, StatementContext},
        plan::{LeftOuterJoinInputPlan, LeftOuterJoinPlan},
        result::Result,
        store::GStore,
    },
    std::{borrow::Cow, iter, rc::Rc},
};

pub(crate) fn execute<'a, T: GStore>(
    storage: &'a T,
    stmt_context: StatementContext<'a>,
    filter_context: Option<&Rc<RowContext<'a>>>,
    plan: &'a LeftOuterJoinPlan,
) -> Result<SelectedRows<'a>> {
    let JoinCandidates {
        sources,
        right,
        groups,
    } = match &plan.input {
        LeftOuterJoinInputPlan::NestedLoop(join) => {
            nested_loop::execute(storage, stmt_context, filter_context, join)?
        }
        LeftOuterJoinInputPlan::Hash(join) => {
            hash::execute(storage, stmt_context, filter_context, join)?
        }
        LeftOuterJoinInputPlan::Condition(condition) => {
            condition::execute(storage, stmt_context, filter_context, condition)?
        }
    };
    let rows = groups.flat_map(move |group| {
        let group = match group {
            Ok(group) => group,
            Err(error) => return Box::new(iter::once(Err(error))) as SelectedIter<'a>,
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
