use {
    crate::{ast, plan::ExprPlan},
    serde::{Deserialize, Serialize},
};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct IndexPredicatePlan {
    pub operator: ast::IndexOperator,
    pub expr: ExprPlan,
}
