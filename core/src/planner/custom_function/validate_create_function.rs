use {
    super::shared::validate_function_definition,
    crate::{
        plan::{StatementPlan, StatementPlanKind},
        result::Result,
    },
};

pub fn validate(statement: &StatementPlan) -> Result<()> {
    if let StatementPlanKind::CreateFunction {
        name,
        args,
        return_,
        ..
    } = &statement.kind
    {
        validate_function_definition(name, args, return_)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use {
        super::validate,
        crate::{parse_sql::parse, planner::PlannerError, result::Error, translate::translate},
    };

    #[test]
    fn validates_definition_without_executing_the_body() {
        for body in [
            "(SELECT id FROM Item)",
            "(SELECT COUNT(*) FROM GLUE_TABLES)",
            "CASE WHEN true THEN 1 ELSE (SELECT id FROM Item) END",
            "(SELECT 1 FROM SERIES((SELECT id FROM Item)))",
            "(SELECT column1 FROM (VALUES ((SELECT id FROM Item))) v)",
        ] {
            let sql = format!("CREATE FUNCTION leak() RETURN {body}");
            let statement = translate(&parse(&sql).unwrap()[0]).unwrap().into();
            assert_eq!(
                validate(&statement),
                Err(Error::Planner(PlannerError::StorageAccessInFunctionBody)),
                "{sql}"
            );
        }
        // CREATE validates the definition without resolving or executing its calls.
        let statement =
            translate(&parse("CREATE FUNCTION forward_call() RETURN future_call()").unwrap()[0])
                .unwrap()
                .into();
        assert_eq!(validate(&statement), Ok(()));
    }
}
