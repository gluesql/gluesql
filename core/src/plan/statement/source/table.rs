use {
    super::{TableAccessPlan, TableAliasPlan},
    serde::{Deserialize, Serialize},
};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TableSourcePlan {
    pub name: String,
    pub alias: Option<TableAliasPlan>,
    pub access: TableAccessPlan,
}
