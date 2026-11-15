use {
    crate::{ast::DataType, data::Value},
    serde::{Deserialize, Serialize},
};

/// A storage query that supplies rows to the common executor.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct NativeSourcePlan {
    pub query: String,
    pub params: Vec<Value>,
    pub alias: String,
    pub columns: Vec<NativeColumnPlan>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct NativeColumnPlan {
    pub name: String,
    pub data_type: DataType,
}
