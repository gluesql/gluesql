use {
    crate::ast::{Expr, OperateFunctionArg},
    serde::{Deserialize, Serialize},
};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CustomFunction {
    pub func_name: String,
    pub args: Option<Vec<OperateFunctionArg>>,
    pub return_: Option<Expr>,
}

impl CustomFunction {
    pub fn to_str(&self) -> String {
        let name = &self.func_name;
        let args = self
            .args
            .as_ref()
            .map(|args| {
                args.iter()
                    .map(|arg| format!("{}:{}", arg.name, arg.data_type))
                    .collect::<Vec<String>>()
                    .join(", ")
            })
            .unwrap_or_else(|| "".to_owned());
        format!("{name}({args})")
    }
}
