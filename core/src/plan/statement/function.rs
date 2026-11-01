use {
    super::ExprPlan,
    crate::ast::{self, DataType},
    serde::{Deserialize, Serialize},
};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct CustomFunctionPlan {
    pub name: String,
    pub args: Vec<FunctionArgPlan>,
    pub body: ExprPlan,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct FunctionArgPlan {
    pub name: String,
    pub data_type: DataType,
    pub default: Option<ExprPlan>,
}

impl From<ast::OperateFunctionArg> for FunctionArgPlan {
    fn from(arg: ast::OperateFunctionArg) -> Self {
        let ast::OperateFunctionArg {
            name,
            data_type,
            default,
        } = arg;

        Self {
            name,
            data_type,
            default: default.map(ExprPlan::from),
        }
    }
}
