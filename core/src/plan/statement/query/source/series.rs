use {
    super::TableAliasPlan,
    crate::plan::{
        ExprPlan,
        explain::{Explain, ExplainContext, ExplainNode},
    },
    serde::{Deserialize, Serialize},
};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SeriesSourcePlan {
    pub alias: TableAliasPlan,
    pub size: ExprPlan,
}

impl Explain for SeriesSourcePlan {
    type Output = ExplainNode;

    fn explain(&self, context: &mut ExplainContext) -> ExplainNode {
        ExplainNode::new(format!("series {}", self.alias.name))
            .with_property("size", self.size.explain(context))
    }
}
