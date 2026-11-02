use {
    super::shared::validate_function_definition,
    crate::{
        data::Value,
        plan::{CustomFunctionPlan, ExprPlan, FunctionPlan, StatementPlan, StatementPlanKind},
        planner::{
            PlannerError, aggregate,
            visit_mut::{VisitorMut, walk_expr_mut},
        },
        result::{Error, Result},
        store::CustomFunction,
    },
    std::{collections::HashMap, mem},
};

pub fn plan<T: CustomFunction + ?Sized>(
    storage: &T,
    mut statement: StatementPlan,
) -> Result<StatementPlan> {
    let first_new = statement.functions.len();
    let mut planner = CustomFunctionPlanner {
        storage,
        names: statement
            .functions
            .iter()
            .enumerate()
            .map(|(index, function)| (function.name.to_uppercase(), index))
            .collect(),
        functions: &mut statement.functions,
    };
    match &mut statement.kind {
        StatementPlanKind::Query(query) | StatementPlanKind::Insert { source: query, .. } => {
            planner.visit_query(query)?;
        }
        StatementPlanKind::CreateTable {
            source: Some(query),
            ..
        } => planner.visit_query(query)?,
        StatementPlanKind::Update {
            assignments,
            selection,
            ..
        } => {
            for assignment in assignments {
                planner.visit_expr(&mut assignment.value)?;
            }
            if let Some(expr) = selection {
                planner.visit_expr(expr)?;
            }
        }
        StatementPlanKind::Delete {
            selection: Some(expr),
            ..
        } => planner.visit_expr(expr)?,
        _ => {}
    }

    // Definitions enter the registry before their bodies are visited, so recursive
    // references reuse an index instead of recursively expanding the definition.
    let mut index = first_new;
    while index < planner.functions.len() {
        let mut body = mem::replace(
            &mut planner.functions[index].body,
            ExprPlan::Value(Value::Null),
        );
        planner.visit_expr(&mut body)?;
        aggregate::plan_expr(&mut body);
        planner.functions[index].body = body;
        index += 1;
    }
    Ok(statement)
}

struct CustomFunctionPlanner<'a, T: ?Sized> {
    storage: &'a T,
    names: HashMap<String, usize>,
    functions: &'a mut Vec<CustomFunctionPlan>,
}

impl<T: CustomFunction + ?Sized> VisitorMut for CustomFunctionPlanner<'_, T> {
    type Error = Error;

    fn visit_expr(&mut self, expr: &mut ExprPlan) -> Result<()> {
        walk_expr_mut(self, expr)?;
        if let ExprPlan::Function(function) = expr {
            match function.as_mut() {
                FunctionPlan::UnplannedCustom { name, exprs } => {
                    let function_id = self.prepare(name)?;
                    self.validate_call(function_id, exprs.len())?;
                    **function = FunctionPlan::Custom {
                        function_id,
                        exprs: mem::take(exprs),
                    };
                }
                FunctionPlan::Custom { function_id, exprs } => {
                    self.validate_call(*function_id, exprs.len())?;
                }
                _ => {}
            }
        }
        Ok(())
    }
}

impl<T: CustomFunction + ?Sized> CustomFunctionPlanner<'_, T> {
    fn prepare(&mut self, name: &str) -> Result<usize> {
        let key = name.to_uppercase();
        if let Some(index) = self.names.get(&key) {
            return Ok(*index);
        }
        let definition = self
            .storage
            .fetch_function(&key)?
            .ok_or_else(|| PlannerError::UnsupportedFunction(name.to_owned()))?;
        let function = validate_function_definition(
            &definition.func_name,
            &definition.args,
            &definition.body,
        )?;
        let index = self.functions.len();
        self.names.insert(key, index);
        self.functions.push(function);
        Ok(index)
    }

    fn validate_call(&self, index: usize, found: usize) -> Result<()> {
        let function = self
            .functions
            .get(index)
            .ok_or(PlannerError::MalformedPlanCustomFunctionIndex(index))?;
        let min = function
            .args
            .iter()
            .filter(|arg| arg.default.is_none())
            .count();
        let max = function.args.len();
        if !(min..=max).contains(&found) {
            return Err(PlannerError::FunctionArgsLengthNotWithinRange {
                name: function.name.clone(),
                expected_minimum: min,
                expected_maximum: max,
                found,
            }
            .into());
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use {
        super::plan,
        crate::{
            ast::{DataType, Expr},
            data::CustomFunction as Definition,
            mock::{MockStorage, run},
            parse_sql::{parse, parse_expr},
            plan::{
                AggregateExprPlan, AggregateFunctionPlan, AggregationInputPlan, AggregationPlan,
                CustomFunctionPlan, ExprPlan, FunctionPlan, OperateFunctionArgPlan,
                ProjectInputPlan, ProjectPlan, ProjectionPlan, QueryPlan, SelectItemPlan,
                SeriesSourcePlan, SourcePlan, StatementPlan, StatementPlanKind, TableAliasPlan,
                ValuesPlan,
            },
            planner::{PlannerError, aggregate},
            result::Error,
            store::{CustomFunction, CustomFunctionMut},
            translate::{NO_PARAMS, translate, translate_expr},
        },
        pretty_assertions::assert_eq,
    };

    fn statement(sql: &str) -> StatementPlan {
        let parsed = parse(sql).expect(sql).into_iter().next().expect(sql);
        translate(&parsed).expect(sql).into()
    }

    fn expr(sql: &str) -> Expr {
        translate_expr(&parse_expr(sql).expect(sql), NO_PARAMS).expect(sql)
    }

    #[test]
    fn call_sites() {
        let storage = MockStorage::default();
        for sql in [
            "INSERT INTO Item VALUES (missing())",
            "CREATE TABLE Item AS SELECT missing()",
            "UPDATE Item SET n = missing()",
            "UPDATE Item SET n = 1, m = missing()",
            "UPDATE Item SET n = 1 WHERE missing()",
            "DELETE FROM Item WHERE missing()",
            "VALUES ((SELECT missing()))",
            "VALUES (EXISTS (SELECT missing()))",
            "VALUES (1 IN (SELECT missing()))",
            "VALUES (ABS(missing()))",
        ] {
            let actual = plan(&storage, statement(sql));
            let expected = Err(Error::Planner(PlannerError::UnsupportedFunction(
                "MISSING".to_owned(),
            )));
            assert_eq!(actual, expected, "call must be visited: {sql}");
        }

        for sql in [
            "VALUES (ABS(1))",
            "INSERT INTO Item VALUES (1)",
            "CREATE TABLE Item AS SELECT 1",
            "CREATE TABLE Item (n INT)",
            "UPDATE Item SET n = ABS(1)",
            "UPDATE Item SET n = 1 WHERE n = 2",
            "DELETE FROM Item WHERE n = 1",
            "DELETE FROM Item",
            "CREATE FUNCTION f() RETURN missing()",
        ] {
            let actual = plan(&storage, statement(sql));
            let expected = statement(sql);
            assert_eq!(
                actual,
                Ok(expected),
                "statement without calls to bind: {sql}"
            );
        }
    }

    #[test]
    fn call_arguments() {
        let storage = run("CREATE FUNCTION f(x INT, y INT DEFAULT 2) RETURN x + y");
        for (sql, found) in [("VALUES (f())", 0), ("VALUES (f(1, 2, 3))", 3)] {
            let actual = plan(&storage, statement(sql));
            let expected = Err(Error::Planner(
                PlannerError::FunctionArgsLengthNotWithinRange {
                    name: "f".to_owned(),
                    expected_minimum: 1,
                    expected_maximum: 2,
                    found,
                },
            ));
            assert_eq!(actual, expected, "{sql}");
        }

        let functions = vec![CustomFunctionPlan {
            name: "f".to_owned(),
            args: vec![
                OperateFunctionArgPlan {
                    name: "x".to_owned(),
                    data_type: DataType::Int,
                    default: None,
                },
                OperateFunctionArgPlan {
                    name: "y".to_owned(),
                    data_type: DataType::Int,
                    default: Some(expr("2").into()),
                },
            ],
            body: expr("x + y").into(),
        }];
        for (sql, args) in [
            ("VALUES (f(1))", vec![expr("1").into()]),
            ("VALUES (f(1, 2))", vec![expr("1").into(), expr("2").into()]),
        ] {
            let expected = StatementPlan {
                kind: StatementPlanKind::Query(QueryPlan::Values(ValuesPlan(vec![vec![
                    ExprPlan::Function(Box::new(FunctionPlan::Custom {
                        function_id: 0,
                        exprs: args,
                    })),
                ]]))),
                functions: functions.clone(),
            };
            assert_eq!(plan(&storage, statement(sql)), Ok(expected), "{sql}");
        }

        let mut input = StatementPlan {
            kind: StatementPlanKind::Query(QueryPlan::Values(ValuesPlan(vec![vec![
                ExprPlan::Function(Box::new(FunctionPlan::Custom {
                    function_id: 0,
                    exprs: Vec::new(),
                })),
            ]]))),
            functions,
        };
        let expected = Err(Error::Planner(
            PlannerError::FunctionArgsLengthNotWithinRange {
                name: "f".to_owned(),
                expected_minimum: 1,
                expected_maximum: 2,
                found: 0,
            },
        ));
        assert_eq!(
            plan(&storage, input.clone()),
            expected,
            "prepared calls also validate arity"
        );

        input.functions.clear();
        let expected = Err(Error::Planner(
            PlannerError::MalformedPlanCustomFunctionIndex(0),
        ));
        assert_eq!(
            plan(&storage, input),
            expected,
            "prepared index must identify a definition"
        );
    }

    #[test]
    fn existing_functions() {
        let storage = run("CREATE FUNCTION fresh() RETURN 2");
        let existing = CustomFunctionPlan {
            name: "Existing".to_owned(),
            args: Vec::new(),
            body: expr("1").into(),
        };
        let mut input = statement("VALUES (existing(), fresh(), EXISTING())");
        input.functions.push(existing.clone());
        let expected = StatementPlan {
            kind: StatementPlanKind::Query(QueryPlan::Values(ValuesPlan(vec![vec![
                ExprPlan::Function(Box::new(FunctionPlan::Custom {
                    function_id: 0,
                    exprs: Vec::new(),
                })),
                ExprPlan::Function(Box::new(FunctionPlan::Custom {
                    function_id: 1,
                    exprs: Vec::new(),
                })),
                ExprPlan::Function(Box::new(FunctionPlan::Custom {
                    function_id: 0,
                    exprs: Vec::new(),
                })),
            ]]))),
            functions: vec![
                existing,
                CustomFunctionPlan {
                    name: "fresh".to_owned(),
                    args: Vec::new(),
                    body: expr("2").into(),
                },
            ],
        };
        assert_eq!(plan(&storage, input), Ok(expected));
    }

    #[test]
    fn function_body() {
        let storage =
            run("CREATE FUNCTION sum_to(n INT) RETURN (SELECT SUM(N) AS total FROM SERIES(n))");
        let sql = "VALUES (sum_to(3))";
        let aggregate = AggregateExprPlan {
            func: AggregateFunctionPlan::Sum(expr("N").into()),
            distinct: false,
            slot: Some(0),
        };
        let expected = StatementPlan {
            kind: StatementPlanKind::Query(QueryPlan::Values(ValuesPlan(vec![vec![
                ExprPlan::Function(Box::new(FunctionPlan::Custom {
                    function_id: 0,
                    exprs: vec![expr("3").into()],
                })),
            ]]))),
            functions: vec![CustomFunctionPlan {
                name: "sum_to".to_owned(),
                args: vec![OperateFunctionArgPlan {
                    name: "n".to_owned(),
                    data_type: DataType::Int,
                    default: None,
                }],
                body: ExprPlan::Subquery(Box::new(QueryPlan::Project(ProjectPlan {
                    input: ProjectInputPlan::Aggregation(AggregationPlan {
                        input: AggregationInputPlan::Source(SourcePlan::Series(SeriesSourcePlan {
                            alias: TableAliasPlan {
                                name: "SERIES".to_owned(),
                                columns: Vec::new(),
                            },
                            size: expr("n").into(),
                        })),
                        group_by: Vec::new(),
                        aggregate_slots: vec![aggregate.clone()],
                    }),
                    projection: ProjectionPlan::SelectItems(vec![SelectItemPlan::Expr {
                        expr: ExprPlan::Aggregate(Box::new(aggregate)),
                        label: "total".to_owned(),
                    }]),
                }))),
            }],
        };
        assert_eq!(
            plan(&storage, statement(sql)),
            Ok(expected),
            "newly fetched bodies receive aggregate slots: {sql}"
        );
    }

    #[test]
    fn calls() {
        let mut storage = run("
            CREATE FUNCTION plus_one(x INT DEFAULT 1) RETURN x + 1;
            CREATE FUNCTION twice(x INT) RETURN plus_one(plus_one(x));
        ");

        let sql = "VALUES (twice(2), plus_one())";
        let actual = plan(&storage, statement(sql));
        let expected = StatementPlan {
            kind: StatementPlanKind::Query(QueryPlan::Values(ValuesPlan(vec![vec![
                ExprPlan::Function(Box::new(FunctionPlan::Custom {
                    function_id: 0,
                    exprs: vec![expr("2").into()],
                })),
                ExprPlan::Function(Box::new(FunctionPlan::Custom {
                    function_id: 1,
                    exprs: Vec::new(),
                })),
            ]]))),
            functions: vec![
                CustomFunctionPlan {
                    name: "twice".to_owned(),
                    args: vec![OperateFunctionArgPlan {
                        name: "x".to_owned(),
                        data_type: DataType::Int,
                        default: None,
                    }],
                    body: ExprPlan::Function(Box::new(FunctionPlan::Custom {
                        function_id: 1,
                        exprs: vec![ExprPlan::Function(Box::new(FunctionPlan::Custom {
                            function_id: 1,
                            exprs: vec![expr("x").into()],
                        }))],
                    })),
                },
                CustomFunctionPlan {
                    name: "plus_one".to_owned(),
                    args: vec![OperateFunctionArgPlan {
                        name: "x".to_owned(),
                        data_type: DataType::Int,
                        default: Some(expr("1").into()),
                    }],
                    body: expr("x + 1").into(),
                },
            ],
        };
        assert_eq!(
            actual,
            Ok(expected.clone()),
            "nested and repeated calls: {sql}"
        );

        let prepared = expected;
        let aggregate_sql = "SELECT SUM(twice(N)) AS r FROM SERIES(3)";
        let actual = plan(&storage, aggregate::plan(statement(aggregate_sql)));
        let aggregate = AggregateExprPlan {
            func: AggregateFunctionPlan::Sum(ExprPlan::Function(Box::new(FunctionPlan::Custom {
                function_id: 0,
                exprs: vec![expr("N").into()],
            }))),
            distinct: false,
            slot: Some(0),
        };
        let expected = StatementPlan {
            kind: StatementPlanKind::Query(QueryPlan::Project(ProjectPlan {
                input: ProjectInputPlan::Aggregation(AggregationPlan {
                    input: AggregationInputPlan::Source(SourcePlan::Series(SeriesSourcePlan {
                        alias: TableAliasPlan {
                            name: "SERIES".to_owned(),
                            columns: Vec::new(),
                        },
                        size: expr("3").into(),
                    })),
                    group_by: Vec::new(),
                    aggregate_slots: vec![aggregate.clone()],
                }),
                projection: ProjectionPlan::SelectItems(vec![SelectItemPlan::Expr {
                    expr: ExprPlan::Aggregate(Box::new(aggregate)),
                    label: "r".to_owned(),
                }]),
            })),
            functions: prepared.functions.clone(),
        };
        assert_eq!(
            actual,
            Ok(expected),
            "aggregate slots and projection reuse definitions: {aggregate_sql}"
        );

        let mut definition = storage.fetch_function("plus_one").unwrap().unwrap().clone();
        definition.body = expr("x + 100");
        storage.insert_function(definition).unwrap();

        let actual = plan(&storage, prepared.clone());
        let expected = prepared.clone();
        assert_eq!(
            actual,
            Ok(expected),
            "prepared calls keep the original body: {sql}"
        );

        let actual = plan(&storage, statement(sql));
        let mut expected = prepared.clone();
        expected.functions[1].body = expr("x + 100").into();
        assert_eq!(
            actual,
            Ok(expected),
            "new calls use the current body: {sql}"
        );

        storage.delete_function("plus_one").unwrap();
        storage.delete_function("twice").unwrap();

        let actual = plan(&storage, prepared.clone());
        let expected = prepared;
        assert_eq!(
            actual,
            Ok(expected),
            "prepared calls keep deleted definitions: {sql}"
        );

        let actual = plan(&storage, statement(sql));
        let expected = Err(Error::Planner(PlannerError::UnsupportedFunction(
            "TWICE".to_owned(),
        )));
        assert_eq!(
            actual, expected,
            "new calls require the stored definition: {sql}"
        );
    }

    #[test]
    fn recursive_calls() {
        let storage = run("
            CREATE FUNCTION recur() RETURN recur();
            CREATE FUNCTION first() RETURN second();
            CREATE FUNCTION second() RETURN first();
        ");

        let sql = "VALUES (recur())";
        let actual = plan(&storage, statement(sql));
        let call = ExprPlan::Function(Box::new(FunctionPlan::Custom {
            function_id: 0,
            exprs: Vec::new(),
        }));
        let expected = StatementPlan {
            kind: StatementPlanKind::Query(QueryPlan::Values(ValuesPlan(vec![vec![call.clone()]]))),
            functions: vec![CustomFunctionPlan {
                name: "recur".to_owned(),
                args: Vec::new(),
                body: call,
            }],
        };
        assert_eq!(
            actual,
            Ok(expected),
            "self reference reuses the function index: {sql}"
        );

        let sql = "VALUES (first())";
        let actual = plan(&storage, statement(sql));
        let first = ExprPlan::Function(Box::new(FunctionPlan::Custom {
            function_id: 0,
            exprs: Vec::new(),
        }));
        let second = ExprPlan::Function(Box::new(FunctionPlan::Custom {
            function_id: 1,
            exprs: Vec::new(),
        }));
        let expected = StatementPlan {
            kind: StatementPlanKind::Query(QueryPlan::Values(ValuesPlan(vec![vec![
                first.clone(),
            ]]))),
            functions: vec![
                CustomFunctionPlan {
                    name: "first".to_owned(),
                    args: Vec::new(),
                    body: second,
                },
                CustomFunctionPlan {
                    name: "second".to_owned(),
                    args: Vec::new(),
                    body: first,
                },
            ],
        };
        assert_eq!(
            actual,
            Ok(expected),
            "mutual references use finite indices: {sql}"
        );
    }

    #[test]
    fn stored_definition() {
        struct UnsupportedFunctionStorage;
        impl CustomFunction for UnsupportedFunctionStorage {}

        let storage = UnsupportedFunctionStorage;
        let actual = plan(&storage, statement("VALUES (f())"));
        let expected = Err(Error::StorageMsg(
            "[Storage] CustomFunction is not supported".to_owned(),
        ));
        assert_eq!(actual, expected, "storage errors propagate unchanged");

        let storage = run("CREATE FUNCTION forward_call() RETURN missing()");
        let actual = plan(&storage, statement("VALUES (forward_call())"));
        let expected = Err(Error::Planner(PlannerError::UnsupportedFunction(
            "MISSING".to_owned(),
        )));
        assert_eq!(actual, expected, "a missing dependency fails body planning");

        let mut storage = run("CREATE FUNCTION indirect() RETURN leak()");
        storage
            .insert_function(Definition {
                func_name: "leak".to_owned(),
                args: Vec::new(),
                body: expr("(SELECT id FROM Item)"),
            })
            .unwrap();

        for sql in ["SELECT leak()", "SELECT indirect()"] {
            let actual = plan(&storage, statement(sql));
            let expected = Err(Error::Planner(PlannerError::StorageAccessInFunctionBody));
            assert_eq!(actual, expected, "stored definition is validated: {sql}");
        }
    }
}
