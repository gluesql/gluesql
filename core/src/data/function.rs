use serde::{Deserialize, Serialize};
use crate::ast::{Expr, DataType, OperateFunctionArg};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CustomFunction {
    pub func_name: String,
    pub args: Option<Vec<OperateFunctionArg>>,
    pub body: String
}
