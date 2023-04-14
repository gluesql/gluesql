use {
    crate::ast::{Expr, OperateFunctionArg},
    serde::{Deserialize, Serialize},
};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CustomFunction {
    pub func_name: String,
    pub args: Vec<OperateFunctionArg>,
    pub body: Expr,
}

impl CustomFunction {
    pub fn to_str(&self) -> String {
        let name = &self.func_name;
        let args = self
            .args
            .iter()
            .map(|arg| format!("{}: {}", arg.name, arg.data_type))
            .collect::<Vec<String>>()
            .join(", ");
        format!("{name}({args})")
    }
}
