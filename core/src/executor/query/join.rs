pub(super) mod condition;
pub(super) mod hash;
pub(super) mod inner;
pub(super) mod left_outer;
pub(super) mod nested_loop;
pub(super) mod right_outer;

use {
    super::{QueryError, SelectedIter, SelectedSources, SourceColumns},
    crate::{executor::context::RowContext, result::Result},
    std::rc::Rc,
};

struct JoinCandidateGroup<'a> {
    left: Rc<RowContext<'a>>,
    rows: SelectedIter<'a>,
}

type JoinCandidateGroupIter<'a> = Box<dyn Iterator<Item = Result<JoinCandidateGroup<'a>>> + 'a>;

struct JoinCandidates<'a> {
    sources: SelectedSources<'a>,
    right: SourceColumns<'a>,
    groups: JoinCandidateGroupIter<'a>,
}

/// Reaching here means a custom [`crate::store::Planner`] left
/// [`crate::planner::plan_right_outer_join`] out of its pipeline.
pub(super) fn reject_unplanned_right_outer<T>() -> Result<T> {
    Err(QueryError::UnreachableUnplannedRightOuterJoin.into())
}
