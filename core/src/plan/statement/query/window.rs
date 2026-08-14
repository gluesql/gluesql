use {
    super::FilterPlan,
    crate::plan::{InnerJoinPlan, LeftOuterJoinPlan, SourcePlan, WindowExprPlan},
    serde::{Deserialize, Serialize},
};

/// The stages that may sit directly below a [`WindowPlan`].
///
/// `GROUP BY`, `HAVING`, `SELECT DISTINCT`, and a plain (non-window)
/// aggregate anywhere in the same projection are all rejected outright when
/// a window function is present (see `planner::window`), so `Aggregation`
/// and `Having` can never reach this stage.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum WindowInputPlan {
    Source(SourcePlan),
    InnerJoin(Box<InnerJoinPlan>),
    LeftOuterJoin(Box<LeftOuterJoinPlan>),
    Filter(FilterPlan),
}

impl WindowInputPlan {
    pub(crate) fn base_source(&self) -> &SourcePlan {
        match self {
            Self::Source(source) => source,
            Self::InnerJoin(join) => join.base_source(),
            Self::LeftOuterJoin(join) => join.base_source(),
            Self::Filter(filter) => filter.input.base_source(),
        }
    }

    pub(crate) fn joined_sources(&self) -> Vec<&SourcePlan> {
        match self {
            Self::Source(_) => Vec::new(),
            Self::InnerJoin(join) => join.joined_sources(),
            Self::LeftOuterJoin(join) => join.joined_sources(),
            Self::Filter(filter) => filter.input.joined_sources(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct WindowPlan {
    pub input: WindowInputPlan,
    pub window_slots: Vec<WindowExprPlan>,
}
