mod index_predicate;

pub use index_predicate::IndexPredicatePlan;

use {
    crate::plan::ExprPlan,
    serde::{Deserialize, Serialize},
};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum TableAccessPlan {
    FullScan,
    /// A full scan no planner may narrow: a RIGHT JOIN's unmatched pass needs the complete left
    /// input. Executes exactly like [`Self::FullScan`].
    FullScanRequired,
    PrimaryKey {
        expr: ExprPlan,
    },
    Index {
        name: String,
        asc: Option<bool>,
        predicate: Option<IndexPredicatePlan>,
    },
}
