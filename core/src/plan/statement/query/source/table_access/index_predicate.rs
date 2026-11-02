use {
    crate::{
        ast::IndexOperator,
        plan::{
            ExprPlan,
            explain::{Explain, ExplainContext},
        },
    },
    serde::{Deserialize, Serialize},
};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct IndexPredicatePlan {
    pub operator: IndexOperator,
    pub expr: ExprPlan,
}

impl Explain for IndexPredicatePlan {
    type Output = String;

    fn explain(&self, context: &mut ExplainContext) -> String {
        let operator = match &self.operator {
            IndexOperator::Gt => ">",
            IndexOperator::Lt => "<",
            IndexOperator::GtEq => ">=",
            IndexOperator::LtEq => "<=",
            IndexOperator::Eq => "=",
        };

        format!("{operator} {}", self.expr.explain(context))
    }
}
