use {
    super::ProjectPlan,
    crate::plan::{AggregatePlan, ExprPlan, OrderByExprPlan, TableWithJoinsPlan},
    serde::{Deserialize, Serialize},
};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SelectPlan {
    pub from: TableWithJoinsPlan,
    pub selection: Option<ExprPlan>,
    pub group_by: Vec<ExprPlan>,
    pub having: Option<ExprPlan>,
    pub aggregate_slots: Option<Vec<AggregatePlan>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SelectOrderByPlan {
    pub input: ProjectPlan,
    pub exprs: Vec<OrderByExprPlan>,
}
