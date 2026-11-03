use crate::{
    ast::{Expr, OperateFunctionArg},
    executor::evaluate_stateless,
    plan::{CustomFunctionPlan, ExprPlan, FunctionPlan, OperateFunctionArgPlan, SourcePlan},
    planner::{
        PlannerError,
        visit::{Visitor, walk_expr, walk_source},
    },
    result::Result,
};

pub fn validate(
    name: &str,
    args: &[OperateFunctionArg],
    body: &Expr,
) -> Result<CustomFunctionPlan> {
    for (index, arg) in args.iter().enumerate() {
        if args[index + 1..].iter().any(|other| arg.name == other.name) {
            return Err(PlannerError::DuplicateArgName(arg.name.clone()).into());
        }
    }
    let args = args
        .iter()
        .cloned()
        .map(OperateFunctionArgPlan::from)
        .collect::<Vec<_>>();
    for default in args.iter().filter_map(|arg| arg.default.as_ref()) {
        FunctionDefaultValidator.visit_expr(default)?;
        evaluate_stateless(None, default)?;
    }
    if !args
        .iter()
        .skip_while(|arg| arg.default.is_none())
        .all(|arg| arg.default.is_some())
    {
        return Err(PlannerError::NonDefaultArgumentFollowsDefaultArgument.into());
    }

    let body = ExprPlan::from(body.clone());
    FunctionBodyValidator.visit_expr(&body)?;
    Ok(CustomFunctionPlan {
        name: name.to_owned(),
        args,
        body,
    })
}

struct FunctionDefaultValidator;

impl Visitor for FunctionDefaultValidator {
    type Error = PlannerError;

    fn visit_expr(&mut self, expr: &ExprPlan) -> std::result::Result<(), PlannerError> {
        if let ExprPlan::Function(function) = expr
            && let FunctionPlan::UnplannedCustom { name, .. } = function.as_ref()
        {
            return Err(PlannerError::CustomFunctionInArgumentDefault(name.clone()));
        }
        walk_expr(self, expr)
    }
}

struct FunctionBodyValidator;

impl Visitor for FunctionBodyValidator {
    type Error = PlannerError;

    fn visit_source(&mut self, source: &SourcePlan) -> std::result::Result<(), PlannerError> {
        match source {
            SourcePlan::Table(_) | SourcePlan::Dictionary(_) => {
                Err(PlannerError::StorageAccessInFunctionBody)
            }
            SourcePlan::Derived(_) | SourcePlan::Series(_) => walk_source(self, source),
        }
    }
}

#[cfg(test)]
mod tests {
    use {
        super::validate,
        crate::{
            ast::{DataType, Statement},
            executor::EvaluateError,
            parse_sql::{parse, parse_expr},
            plan::{CustomFunctionPlan, ExprPlan, OperateFunctionArgPlan},
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
                OperateFunctionArgPlan {
                    name: "x".to_owned(),
                    data_type: DataType::Int,
                    default: None,
                },
                OperateFunctionArgPlan {
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
                PlannerError::DuplicateArgName("x".to_owned()).into(),
            ),
            (
                "CREATE FUNCTION f(x INT DEFAULT 1, y INT) RETURN x",
                PlannerError::NonDefaultArgumentFollowsDefaultArgument.into(),
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
    fn argument_defaults() {
        let test = |default: &str, name: &str| {
            let sql = format!("CREATE FUNCTION f(x INT DEFAULT {default}) RETURN x");
            let actual = definition(&sql);
            let expected = Err(Error::Planner(
                PlannerError::CustomFunctionInArgumentDefault("HELPER".to_owned()),
            ));
            assert_eq!(actual, expected, "[{name}] {sql}");
        };

        test("CASE WHEN true THEN 1 ELSE helper() END", "unselected ELSE");
        test(
            "CASE WHEN false THEN helper() ELSE 1 END",
            "unselected THEN",
        );
        test(
            "CASE WHEN true THEN 1 WHEN helper() = 0 THEN 2 ELSE 3 END",
            "unselected WHEN",
        );
        test(
            "ABS(CASE WHEN true THEN 1 ELSE helper() END)",
            "builtin argument",
        );
        test(
            "CASE WHEN true THEN 1 ELSE (SELECT helper()) END",
            "subquery in an unselected branch",
        );
        test("helper()", "direct custom call");
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
