use {
    super::TableAliasPlan,
    crate::plan::{
        QueryPlan,
        explain::{Explain, ExplainContext, ExplainNode},
    },
    serde::{Deserialize, Serialize},
};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct DerivedSourcePlan {
    pub query: Box<QueryPlan>,
    pub alias: TableAliasPlan,
}

impl Explain for DerivedSourcePlan {
    type Output = ExplainNode;

    fn explain(&self, context: &mut ExplainContext) -> ExplainNode {
        ExplainNode::new(format!("derived {}", self.alias.name))
            .with_optional_property(
                "columns",
                (!self.alias.columns.is_empty()).then(|| self.alias.columns.join(", ")),
            )
            .with_child(self.query.explain(context))
    }
}
