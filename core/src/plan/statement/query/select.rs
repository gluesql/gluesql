use {
    crate::plan::{AggregatePlan, ExprPlan, OrderByExprPlan, ProjectionPlan, TableWithJoinsPlan},
    serde::{Deserialize, Serialize},
};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SelectPlan {
    pub projection: ProjectionPlan,
    pub from: TableWithJoinsPlan,
    pub selection: Option<ExprPlan>,
    pub group_by: Vec<ExprPlan>,
    pub having: Option<ExprPlan>,
    pub aggregate_slots: Option<Vec<AggregatePlan>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SelectOrderByPlan {
    pub input: Box<SelectPlan>,
    pub exprs: Vec<OrderByExprPlan>,
}
