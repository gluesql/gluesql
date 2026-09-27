pub(super) mod condition;
pub(super) mod hash;
pub(super) mod inner;
pub(super) mod left_outer;
pub(super) mod nested_loop;

use {
    super::{SelectedIter, SelectedSources, SourceColumns},
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
