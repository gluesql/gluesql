use {
    super::TableAliasPlan,
    crate::{ast, plan::ExprPlan},
    serde::{Deserialize, Serialize},
};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TableSourcePlan {
    pub name: String,
    pub alias: Option<TableAliasPlan>,
    pub access: TableAccessPlan,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum TableAccessPlan {
    FullScan,
    PrimaryKey {
        expr: ExprPlan,
    },
    Index {
        name: String,
        asc: Option<bool>,
        predicate: Option<IndexPredicatePlan>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct IndexPredicatePlan {
    pub operator: ast::IndexOperator,
    pub expr: ExprPlan,
}
