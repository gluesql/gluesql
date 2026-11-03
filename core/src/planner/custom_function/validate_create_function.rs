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
    fn create_function() {
        let cases = [
            (
                "CREATE FUNCTION leak() RETURN (SELECT id FROM Item)",
                Err(Error::Planner(PlannerError::StorageAccessInFunctionBody)),
            ),
            (
                "CREATE FUNCTION forward_call() RETURN future_call()",
                Ok(()),
            ),
            ("SELECT future_call()", Ok(())),
        ];

        for (sql, expected) in cases {
            let parsed = parse(sql).expect(sql).into_iter().next().expect(sql);
            let statement = translate(&parsed).expect(sql).into();
            let actual = validate(&statement);
            assert_eq!(actual, expected, "validate without resolving calls: {sql}");
        }
    }
}
