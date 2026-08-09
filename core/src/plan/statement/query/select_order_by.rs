use {
    super::ProjectPlan,
    crate::plan::{
        OrderByExprPlan,
        explain::{Explain, ExplainContext, ExplainNode},
    },
    serde::{Deserialize, Serialize},
};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SelectOrderByPlan {
    pub input: ProjectPlan,
    pub exprs: Vec<OrderByExprPlan>,
}

impl Explain for SelectOrderByPlan {
    type Output = ExplainNode;

    fn explain(&self, context: &mut ExplainContext) -> ExplainNode {
        ExplainNode::new("sort")
            .with_property("order", self.exprs.as_slice().explain(context))
            .with_child(self.input.explain(context))
    }
}
