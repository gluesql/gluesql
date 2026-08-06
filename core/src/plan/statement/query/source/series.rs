use {
    super::TableAliasPlan,
    crate::plan::ExprPlan,
    serde::{Deserialize, Serialize},
};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SeriesSourcePlan {
    pub alias: TableAliasPlan,
    pub size: ExprPlan,
}
