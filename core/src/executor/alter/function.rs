use {
    super::{validate_arg, validate_arg_names, AlterError},
    crate::{
        ast::{ColumnDef, OperateFunctionArg, Query, SetExpr, TableFactor, Values},
        data::{CustomFunction, Schema, TableError},
        executor::{evaluate_stateless, select::select},
        prelude::{DataType, Value},
        result::{Error, IntoControlFlow, Result},
        store::{GStore, GStoreMut},
    },
    chrono::Utc,
    futures::stream::TryStreamExt,
    std::{
        iter,
        ops::ControlFlow::{Break, Continue},
    },
};

pub async fn create_function<T: GStore + GStoreMut>(
    storage: &mut T,
    func_name: &str,
    args: Option<Vec<OperateFunctionArg>>,
    or_replace: bool,
    body: &str,
) -> Result<()> {
    if let Some(args) = &args {
        validate_arg_names(args)?;

        for arg in args {
            validate_arg(arg)?;
        }
    }
    let func = CustomFunction {
        func_name: func_name.to_owned(),
        args: args,
        body: body.to_owned(),
    };

    if storage.fetch_function(func_name).await?.is_none() || or_replace {
        storage.drop_function(&func_name).await?;
        storage.create_function(func).await?;
        return Ok(());
    } else {
        return Err(AlterError::FunctionAlreadyExists(func_name.to_owned()).into());
    }
}

pub async fn drop_function<T: GStore + GStoreMut>(
    storage: &mut T,
    func_names: &[&str],
    if_exists: bool,
) -> Result<()> {
    for func_name in func_names {
        storage.drop_function(&func_name).await?;
    }
    Ok(())
}
