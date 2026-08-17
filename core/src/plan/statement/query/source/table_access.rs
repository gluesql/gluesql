mod index_predicate;

pub use index_predicate::IndexPredicatePlan;

use {
    crate::plan::{
        ExprPlan,
        explain::{Explain, ExplainContext, ExplainNode},
    },
    serde::{Deserialize, Serialize},
};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum TableAccessPlan {
    FullScan,
    PrimaryKey {
        expr: ExprPlan,
    },
    Index {
        name: String,
        asc: Option<bool>,
        predicate: Option<IndexPredicatePlan>,
    },
}

impl TableAccessPlan {
    pub(super) fn explain(&self, node: ExplainNode, context: &mut ExplainContext) -> ExplainNode {
        match self {
            Self::FullScan => node.with_property("access", "full scan"),
            Self::PrimaryKey { expr } => node
                .with_property("access", "primary key")
                .with_property("key", expr.explain(context)),
            Self::Index {
                name,
                asc,
                predicate,
            } => node
                .with_property("access", format!("index {name}"))
                .with_optional_property(
                    "order",
                    asc.map(|asc| if asc { "ascending" } else { "descending" }),
                )
                .with_optional_property(
                    "predicate",
                    predicate
                        .as_ref()
                        .map(|predicate| predicate.explain(context)),
                ),
        }
    }
}
