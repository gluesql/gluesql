use {
    super::TableAliasPlan,
    crate::{ast, plan::QueryPlan},
    serde::{Deserialize, Serialize},
};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct DerivedSourcePlan {
    /// Temporary query AST, consumed by CTE planning before schema collection.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub unplanned: Option<Box<ast::Query>>,
    pub query: Box<QueryPlan>,
    pub alias: TableAliasPlan,
}
