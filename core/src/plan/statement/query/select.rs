use {
    super::ProjectPlan,
    crate::plan::{ExprPlan, OrderByExprPlan, TableWithJoinsPlan},
    serde::{Deserialize, Serialize},
};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SelectPlan {
    pub from: TableWithJoinsPlan,
    pub selection: Option<ExprPlan>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SelectOrderByPlan {
    pub input: ProjectPlan,
    pub exprs: Vec<OrderByExprPlan>,
}
