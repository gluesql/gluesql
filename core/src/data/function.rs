use crate::ast::{DataType, Expr, OperateFunctionArg};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CustomFunction {
    pub func_name: String,
    pub args: Option<Vec<OperateFunctionArg>>,
    pub body: String,
}
