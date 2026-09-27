use {
    super::TableAliasPlan,
    crate::plan::QueryPlan,
    serde::{Deserialize, Serialize},
};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct DerivedSourcePlan {
    pub query: Box<QueryPlan>,
    pub alias: TableAliasPlan,
}
