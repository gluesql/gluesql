use {
    super::TableAliasPlan,
    crate::ast,
    serde::{Deserialize, Serialize},
};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct DictionarySourcePlan {
    pub dictionary: ast::Dictionary,
    pub alias: TableAliasPlan,
}
