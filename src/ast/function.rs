use {
    super::{Expr, ObjectName},
    serde::{Deserialize, Serialize},
};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum FunctionArg {
    Named { name: String, arg: Expr },
    Unnamed(Expr),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Function {
    pub name: ObjectName,
    pub args: Vec<FunctionArg>,
}
