use {
    super::{validate_arg, validate_arg_names, AlterError},
    crate::{
        ast::{Expr, OperateFunctionArg},
        data::CustomFunction,
        result::Result,
        store::{GStore, GStoreMut},
    },
};

pub async fn insert_function<T: GStore + GStoreMut>(
    storage: &mut T,
    func_name: &str,
    args: &Vec<OperateFunctionArg>,
    or_replace: bool,
    body: &Expr,
) -> Result<()> {
    validate_arg_names(args)?;
    args.iter().try_for_each(validate_arg)?;

    if storage.fetch_function(func_name).await?.is_none() || or_replace {
        storage.delete_function(func_name).await?;
        storage
            .insert_function(CustomFunction {
                func_name: func_name.to_owned(),
                args: args.to_owned(),
                body: body.to_owned(),
            })
            .await?;
        Ok(())
    } else {
        Err(AlterError::FunctionAlreadyExists(func_name.to_owned()).into())
    }
}

pub async fn delete_function<T: GStore + GStoreMut>(
    storage: &mut T,
    func_names: &[String],
    if_exists: bool,
) -> Result<()> {
    for func_name in func_names {
        let function = storage.fetch_function(func_name).await?;

        if !if_exists {
            function.ok_or_else(|| AlterError::FunctionNotFound(func_name.to_owned()))?;
        }

        storage.delete_function(func_name).await?;
    }
    Ok(())
}
