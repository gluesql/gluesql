mod index_predicate;

pub use index_predicate::IndexPredicatePlan;

use {
    crate::plan::ExprPlan,
    serde::{Deserialize, Serialize},
};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum TableAccessPlan {
    FullScan {
        /// Original column names in schema order. `None` requests all columns;
        /// an empty list preserves rows without returning column values.
        columns: Option<Vec<String>>,
    },
    PrimaryKey {
        expr: ExprPlan,
    },
    Index {
        name: String,
        asc: Option<bool>,
        predicate: Option<IndexPredicatePlan>,
    },
}
