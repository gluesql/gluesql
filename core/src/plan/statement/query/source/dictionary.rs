use {
    super::TableAliasPlan,
    crate::{
        ast,
        plan::explain::{Explain, ExplainContext, ExplainNode},
    },
    serde::{Deserialize, Serialize},
};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct DictionarySourcePlan {
    pub dictionary: ast::Dictionary,
    pub alias: TableAliasPlan,
}

impl Explain for DictionarySourcePlan {
    type Output = ExplainNode;

    fn explain(&self, _context: &mut ExplainContext) -> ExplainNode {
        ExplainNode::new(format!("dictionary {}", self.alias.name))
            .with_property("source", &self.dictionary)
    }
}
