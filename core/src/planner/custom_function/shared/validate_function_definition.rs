use {
    super::try_visit_mut_query,
    crate::{
        ast::{Expr, OperateFunctionArg},
        executor::{AlterError, evaluate_stateless},
        plan::{CustomFunctionPlan, ExprPlan, FunctionArgPlan, SourcePlan},
        planner::{PlannerError, expr::try_visit_mut_expr},
        result::Result,
    },
};

pub fn validate(
    name: &str,
    args: &[OperateFunctionArg],
    body: &Expr,
) -> Result<CustomFunctionPlan> {
    for (index, arg) in args.iter().enumerate() {
        if args[index + 1..].iter().any(|other| arg.name == other.name) {
            return Err(AlterError::DuplicateArgName(arg.name.clone()).into());
        }
    }
    let args = args
        .iter()
        .cloned()
        .map(FunctionArgPlan::from)
        .collect::<Vec<_>>();
    for default in args.iter().filter_map(|arg| arg.default.as_ref()) {
        evaluate_stateless(None, default)?;
    }
    if !args
        .iter()
        .skip_while(|arg| arg.default.is_none())
        .all(|arg| arg.default.is_some())
    {
        return Err(AlterError::NonDefaultArgumentFollowsDefaultArgument.into());
    }

    let mut body = ExprPlan::from(body.clone());
    validate_body(&mut body)?;
    Ok(CustomFunctionPlan {
        name: name.to_owned(),
        args,
        body,
    })
}

fn validate_body(expr: &mut ExprPlan) -> Result<()> {
    try_visit_mut_expr(expr, &mut |expr| match expr {
        ExprPlan::Subquery(query)
        | ExprPlan::Exists {
            subquery: query, ..
        }
        | ExprPlan::InSubquery {
            subquery: query, ..
        } => try_visit_mut_query(query, &mut validate_body, &mut |source| match source {
            SourcePlan::Table(_) | SourcePlan::Dictionary(_) => {
                Err(PlannerError::StorageAccessInFunctionBody.into())
            }
            SourcePlan::Derived(_) | SourcePlan::Series(_) => Ok(()),
        }),
        _ => Ok(()),
    })
}

#[cfg(test)]
mod tests {
    use {
        super::validate,
        crate::{
            ast::{DataType, Statement},
            executor::{AlterError, EvaluateError},
            parse_sql::{parse, parse_expr},
            plan::{CustomFunctionPlan, ExprPlan, FunctionArgPlan},
            planner::PlannerError,
            result::{Error, Result},
            translate::{NO_PARAMS, translate, translate_expr},
        },
    };

    fn definition(sql: &str) -> Result<CustomFunctionPlan> {
        let parsed = parse(sql).expect(sql).into_iter().next().expect(sql);
        let Statement::CreateFunction {
            name,
            args,
            return_,
            ..
        } = translate(&parsed).expect(sql)
        else {
            panic!("expected CREATE FUNCTION: {sql}");
        };
        validate(&name, &args, &return_)
    }

    fn expr(sql: &str) -> ExprPlan {
        translate_expr(&parse_expr(sql).expect(sql), NO_PARAMS)
            .expect(sql)
            .into()
    }

    #[test]
    fn arguments() {
        let sql = "CREATE FUNCTION add(x INT, y INT DEFAULT 1 + 2) RETURN x + y";
        let expected = CustomFunctionPlan {
            name: "add".to_owned(),
            args: vec![
                FunctionArgPlan {
                    name: "x".to_owned(),
                    data_type: DataType::Int,
                    default: None,
                },
                FunctionArgPlan {
                    name: "y".to_owned(),
                    data_type: DataType::Int,
                    default: Some(expr("1 + 2")),
                },
            ],
            body: expr("x + y"),
        };
        assert_eq!(definition(sql), Ok(expected), "{sql}");

        let cases = [
            (
                "CREATE FUNCTION f(x INT, y INT, x INT) RETURN x",
                AlterError::DuplicateArgName("x".to_owned()).into(),
            ),
            (
                "CREATE FUNCTION f(x INT DEFAULT 1, y INT) RETURN x",
                AlterError::NonDefaultArgumentFollowsDefaultArgument.into(),
            ),
            (
                "CREATE FUNCTION f(x INT DEFAULT missing) RETURN x",
                Error::Evaluate(EvaluateError::IdentifierRequiresRowContext(
                    "missing".to_owned(),
                )),
            ),
        ];
        for (sql, expected) in cases {
            assert_eq!(definition(sql), Err(expected), "{sql}");
        }
    }

    #[test]
    fn body() {
        for body in [
            "1",
            "future_call()",
            "(SELECT 1)",
            "EXISTS (SELECT 1 FROM SERIES(2))",
            "1 IN (VALUES (1), (2))",
            "(SELECT column1 FROM (VALUES (1)) v)",
        ] {
            let sql = format!("CREATE FUNCTION f() RETURN {body}");
            let expected = CustomFunctionPlan {
                name: "f".to_owned(),
                args: Vec::new(),
                body: expr(body),
            };
            assert_eq!(definition(&sql), Ok(expected), "{sql}");
        }

        let cases = [
            "CREATE FUNCTION leak() RETURN (SELECT id FROM Item)",
            "CREATE FUNCTION leak() RETURN (SELECT COUNT(*) FROM GLUE_TABLES)",
            "CREATE FUNCTION leak() RETURN EXISTS (SELECT id FROM Item)",
            "CREATE FUNCTION leak() RETURN 1 IN (SELECT id FROM Item)",
            "CREATE FUNCTION leak() RETURN CASE WHEN true THEN 1 ELSE (SELECT id FROM Item) END",
            "CREATE FUNCTION leak() RETURN (SELECT 1 FROM SERIES((SELECT id FROM Item)))",
            "CREATE FUNCTION leak() RETURN (SELECT column1 FROM (VALUES ((SELECT id FROM Item))) v)",
        ];

        for sql in cases {
            let actual = definition(sql);
            let expected = Err(Error::Planner(PlannerError::StorageAccessInFunctionBody));
            assert_eq!(actual, expected, "body must not access storage: {sql}");
        }
    }
}
