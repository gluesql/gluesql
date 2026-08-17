use {
    super::{AlterError, validate_arg_names, validate_default_args},
    crate::{
        ast::{Expr, OperateFunctionArg},
        data::CustomFunction,
        plan::{ExprPlan, plan_scalar_expr, try_visit_expr},
        result::Result,
        store::{GStore, GStoreMut},
    },
};

pub fn insert_function<T: GStore + GStoreMut>(
    storage: &mut T,
    func_name: &str,
    args: &[OperateFunctionArg],
    or_replace: bool,
    body: &Expr,
) -> Result<()> {
    validate_arg_names(args)?;
    validate_default_args(args)?;
    validate_scalar_body(func_name, args, body)?;

    if storage.fetch_function(func_name)?.is_none() || or_replace {
        storage.delete_function(func_name)?;
        storage.insert_function(CustomFunction {
            func_name: func_name.to_owned(),
            args: args.to_owned(),
            body: body.to_owned(),
        })?;
        Ok(())
    } else {
        Err(AlterError::FunctionAlreadyExists(func_name.to_owned()).into())
    }
}

fn validate_scalar_body(func_name: &str, args: &[OperateFunctionArg], body: &Expr) -> Result<()> {
    let body = plan_scalar_expr(body.clone());

    try_visit_expr(&body, &mut |expr| -> std::result::Result<(), AlterError> {
        match expr {
            ExprPlan::Aggregate(_)
            | ExprPlan::Exists { .. }
            | ExprPlan::InSubquery { .. }
            | ExprPlan::Subquery(_) => Err(AlterError::FunctionBodyMustBeScalar),
            ExprPlan::UnplannedReference { qualifier, name }
                if qualifier
                    .as_deref()
                    .is_none_or(|qualifier| qualifier == func_name)
                    && args.iter().any(|arg| arg.name == *name) =>
            {
                Ok(())
            }
            ExprPlan::UnplannedReference { name, .. } => {
                Err(AlterError::FunctionArgumentNotFound(name.clone()))
            }
            _ => Ok(()),
        }
    })?;

    Ok(())
}

pub fn delete_function<T: GStore + GStoreMut>(
    storage: &mut T,
    func_names: &[String],
    if_exists: bool,
) -> Result<()> {
    for func_name in func_names {
        let function = storage.fetch_function(func_name)?;

        if !if_exists {
            function.ok_or_else(|| AlterError::FunctionNotFound(func_name.to_owned()))?;
        }

        storage.delete_function(func_name)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use {
        super::validate_scalar_body,
        crate::{
            ast::OperateFunctionArg,
            executor::alter::AlterError,
            parse_sql::parse_expr,
            prelude::DataType,
            translate::{NO_PARAMS, translate_expr},
        },
    };

    fn body(sql: &str) -> crate::ast::Expr {
        let parsed = parse_expr(sql).expect(sql);
        translate_expr(&parsed, NO_PARAMS).expect(sql)
    }

    fn args(names: &[&str]) -> Vec<OperateFunctionArg> {
        names
            .iter()
            .map(|name| OperateFunctionArg {
                name: (*name).to_owned(),
                data_type: DataType::Text,
                default: None,
            })
            .collect()
    }

    #[test]
    fn scalar_body_uses_declared_function_arguments() {
        assert_eq!(
            validate_scalar_body("add_tax", &args(&["price", "tax"]), &body("price + tax")),
            Ok(())
        );
        assert_eq!(
            validate_scalar_body("add_tax", &args(&["price"]), &body("other + 1")),
            Err(AlterError::FunctionArgumentNotFound("other".to_owned()).into())
        );
    }

    #[test]
    fn scalar_body_rejects_aggregate_and_subquery() {
        assert_eq!(
            validate_scalar_body("total", &args(&["price"]), &body("SUM(price)")),
            Err(AlterError::FunctionBodyMustBeScalar.into())
        );
        assert_eq!(
            validate_scalar_body("total", &args(&["price"]), &body("(SELECT 1)")),
            Err(AlterError::FunctionBodyMustBeScalar.into())
        );
    }
}
