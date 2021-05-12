use {
    super::{expr::translate_expr, translate_object_name},
    crate::{
        ast::{Function, FunctionArg},
        result::Result,
    },
    sqlparser::ast::{Function as SqlFunction, FunctionArg as SqlFunctionArg},
};

pub fn translate_function(sql_function: &SqlFunction) -> Result<Function> {
    let SqlFunction { name, args, .. } = sql_function;

    let name = translate_object_name(name);
    let args = args
        .iter()
        .map(|arg| match arg {
            SqlFunctionArg::Named { name, arg } => Ok(FunctionArg::Named {
                name: name.value.to_owned(),
                arg: translate_expr(arg)?,
            }),
            SqlFunctionArg::Unnamed(expr) => translate_expr(expr).map(FunctionArg::Unnamed),
        })
        .collect::<Result<_>>()?;

    Ok(Function { name, args })
}
