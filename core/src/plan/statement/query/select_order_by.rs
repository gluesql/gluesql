use {
    super::ProjectPlan,
    crate::plan::OrderByExprPlan,
    serde::{Deserialize, Serialize},
};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SelectOrderByPlan {
    pub input: ProjectPlan,
    pub exprs: Vec<OrderByExprPlan>,
}
