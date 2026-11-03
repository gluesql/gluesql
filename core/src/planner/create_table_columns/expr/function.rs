use {
    super::{ExprType, candidate_type, infer_type as infer_expr},
    crate::{
        ast::DataType,
        plan::FunctionPlan,
        planner::{
            PlannerError,
            create_table_columns::{
                Column, Context,
                source::{Relation, Scope},
            },
        },
        result::Result,
    },
    std::hash::BuildHasher,
};

pub(super) fn infer_type<S: BuildHasher>(
    context: &mut Context<'_, S>,
    scope: &Scope<'_>,
    function: &FunctionPlan,
) -> Result<ExprType> {
    use {DataType as D, ExprType as T, FunctionPlan as F};
    Ok(match function {
        F::Cast { data_type, .. } => T::Known(data_type.clone()),
        F::Lower(_)
        | F::Initcap(_)
        | F::Upper(_)
        | F::Left { .. }
        | F::Right { .. }
        | F::Lpad { .. }
        | F::Rpad { .. }
        | F::Replace { .. }
        | F::Concat(_)
        | F::ConcatWs { .. }
        | F::Trim { .. }
        | F::Ltrim { .. }
        | F::Rtrim { .. }
        | F::Reverse(_)
        | F::Repeat { .. }
        | F::Substr { .. }
        | F::Format { .. }
        | F::Chr(_)
        | F::Md5(_)
        | F::Hex(_) => T::Known(D::Text),
        F::Asin(_)
        | F::Acos(_)
        | F::Atan(_)
        | F::Ceil(_)
        | F::Rand(_)
        | F::Round(_)
        | F::Trunc(_)
        | F::Floor(_)
        | F::Exp(_)
        | F::Ln(_)
        | F::Log { .. }
        | F::Log2(_)
        | F::Log10(_)
        | F::Sin(_)
        | F::Cos(_)
        | F::Tan(_)
        | F::Sqrt(_)
        | F::Power { .. }
        | F::Radians(_)
        | F::Degrees(_)
        | F::Pi()
        | F::GetX(_)
        | F::GetY(_)
        | F::CalcDistance { .. } => T::Known(D::Float),
        F::Div { .. }
        | F::Gcd { .. }
        | F::Lcm { .. }
        | F::Extract { .. }
        | F::Position { .. }
        | F::FindIdx { .. } => T::Known(D::Int),
        F::Sign(_) => T::Known(D::Int8),
        F::Ascii(_) => T::Known(D::Uint8),
        F::Length(_) => T::Known(D::Uint64),
        F::Now() | F::CurrentTimestamp() | F::ToTimestamp { .. } => T::Known(D::Timestamp),
        F::CurrentDate() | F::LastDay(_) | F::ToDate { .. } | F::AddMonth { .. } => {
            T::Known(D::Date)
        }
        F::CurrentTime() | F::ToTime { .. } => T::Known(D::Time),
        F::GenerateUuid() => T::Known(D::Uuid),
        F::Point { .. } => T::Known(D::Point),
        F::IsEmpty(_) => T::Known(D::Boolean),
        F::Append { .. }
        | F::Sort { .. }
        | F::Slice { .. }
        | F::Prepend { .. }
        | F::Skip { .. }
        | F::Take { .. }
        | F::Entries(_)
        | F::Keys(_)
        | F::Values(_)
        | F::Splice { .. }
        | F::Dedup(_) => T::Known(D::List),
        F::Unwrap { .. } => T::Unknown,
        F::Abs(expr) => infer_expr(context, scope, expr)?
            .data_type()
            .map_or(T::Unknown, T::Known),
        F::Mod { dividend, divisor } => {
            let left = infer_expr(context, scope, dividend)?.data_type();
            let right = infer_expr(context, scope, divisor)?.data_type();
            match (left, right) {
                (Some(D::Float32 | D::Float), Some(D::Decimal)) => T::Known(D::Decimal),
                (Some(left), _) => T::Known(left),
                _ => T::Unknown,
            }
        }
        F::NullIf { expr1, .. } => infer_expr(context, scope, expr1)?,
        F::IfNull { expr, then } => candidate_type(&[
            infer_expr(context, scope, expr)?,
            infer_expr(context, scope, then)?,
        ]),
        F::Coalesce(exprs) | F::Greatest(exprs) => candidate_type(
            &exprs
                .iter()
                .map(|expr| infer_expr(context, scope, expr))
                .collect::<Result<Vec<_>>>()?,
        ),
        F::Custom { function_id, .. } => {
            let function = context
                .functions
                .get(*function_id)
                .ok_or(PlannerError::MalformedPlanCustomFunctionIndex(*function_id))?;
            if context.visiting_functions.contains(function_id) {
                return Ok(T::Unknown);
            }
            let scope = Scope {
                relations: vec![Relation {
                    alias: function.name.clone(),
                    columns: function
                        .args
                        .iter()
                        .map(|arg| Column::new(&arg.name, arg.data_type.clone()))
                        .collect(),
                    dynamic: false,
                }],
                outer: None,
            };
            context.visiting_functions.push(*function_id);
            let result = infer_expr(context, &scope, &function.body);
            context.visiting_functions.pop();
            match result? {
                T::Literal(data_type) => T::Known(data_type),
                T::Conflict { .. } => T::Unknown,
                other => other,
            }
        }
        F::UnplannedCustom { name, .. } => {
            return Err(PlannerError::UnplannedCustomFunction(name.clone()).into());
        }
    })
}

#[cfg(test)]
mod tests {
    use {
        super::{Context, ExprType, Scope, infer_type},
        crate::{
            ast::DataType,
            parse_sql::parse_expr,
            plan::{CustomFunctionPlan, ExprPlan, FunctionPlan, OperateFunctionArgPlan},
            planner::PlannerError,
            result::Error,
            translate::{NO_PARAMS, translate_expr},
        },
        std::collections::HashMap,
    };

    fn expr(sql: &str) -> ExprPlan {
        translate_expr(&parse_expr(sql).expect(sql), NO_PARAMS)
            .expect(sql)
            .into()
    }

    #[test]
    fn builtin_return_types() {
        let schemas = HashMap::new();
        let mut context = Context {
            schemas: &schemas,
            functions: &[],
            visiting_functions: Vec::new(),
        };
        let scope = Scope::default();
        let mut test = |sql: &str, expected: ExprType| {
            let ExprPlan::Function(function) = expr(sql) else {
                panic!("{sql}")
            };
            assert_eq!(
                infer_type(&mut context, &scope, &function),
                Ok(expected),
                "{sql}"
            );
        };
        test("CAST(NULL AS FLOAT)", ExprType::Known(DataType::Float));
        test("LOWER('A')", ExprType::Known(DataType::Text));
        test("SQRT(4)", ExprType::Known(DataType::Float));
        test("RAND()", ExprType::Known(DataType::Float));
        test("DIV(5, 2)", ExprType::Known(DataType::Int));
        test(
            "EXTRACT(YEAR FROM DATE '2024-01-01')",
            ExprType::Known(DataType::Int),
        );
        test("SIGN(-1)", ExprType::Known(DataType::Int8));
        test("ASCII('a')", ExprType::Known(DataType::Uint8));
        test("LENGTH('a')", ExprType::Known(DataType::Uint64));
        test("NOW()", ExprType::Known(DataType::Timestamp));
        test(
            "LAST_DAY(DATE '2024-01-01')",
            ExprType::Known(DataType::Date),
        );
        test(
            "ADD_MONTH('2024-01-01', 1)",
            ExprType::Known(DataType::Date),
        );
        test(
            "TO_TIME('12:00:00', '%H:%M:%S')",
            ExprType::Known(DataType::Time),
        );
        test("GENERATE_UUID()", ExprType::Known(DataType::Uuid));
        test("POINT(1, 2)", ExprType::Known(DataType::Point));
        test("IS_EMPTY([])", ExprType::Known(DataType::Boolean));
        test("APPEND([], 1)", ExprType::Known(DataType::List));
        test("UNWRAP(CAST('{}' AS MAP), 'x')", ExprType::Unknown);
        test("ABS(CAST(1 AS INT8))", ExprType::Known(DataType::Int8));
        test("ABS(NULL)", ExprType::Unknown);
        test(
            "MOD(CAST(3 AS FLOAT), CAST(2 AS DECIMAL))",
            ExprType::Known(DataType::Decimal),
        );
        test("MOD(CAST(3 AS INT), 2)", ExprType::Known(DataType::Int));
        test("MOD(NULL, 2)", ExprType::Unknown);
        test(
            "NULLIF(CAST(1 AS FLOAT), 2)",
            ExprType::Known(DataType::Float),
        );
        test(
            "IFNULL(NULL, CAST(1 AS FLOAT))",
            ExprType::Known(DataType::Float),
        );
        test(
            "COALESCE(NULL, CAST(1 AS FLOAT))",
            ExprType::Known(DataType::Float),
        );
        test(
            "GREATEST(CAST(1 AS FLOAT), CAST(2 AS FLOAT))",
            ExprType::Known(DataType::Float),
        );
    }

    #[test]
    fn prepared_custom_function() {
        let schemas = HashMap::new();
        let functions = vec![
            CustomFunctionPlan {
                name: "INC".to_owned(),
                args: vec![OperateFunctionArgPlan {
                    name: "x".to_owned(),
                    data_type: DataType::Int,
                    default: None,
                }],
                body: expr("x + 1"),
            },
            CustomFunctionPlan {
                name: "ONE".to_owned(),
                args: vec![],
                body: expr("1"),
            },
            CustomFunctionPlan {
                name: "RECURSIVE".to_owned(),
                args: vec![],
                body: ExprPlan::Function(Box::new(FunctionPlan::Custom {
                    function_id: 2,
                    exprs: vec![],
                })),
            },
            CustomFunctionPlan {
                name: "INDIRECT".to_owned(),
                args: vec![],
                body: ExprPlan::Function(Box::new(FunctionPlan::Custom {
                    function_id: 1,
                    exprs: vec![],
                })),
            },
            CustomFunctionPlan {
                name: "MIXED".to_owned(),
                args: vec![OperateFunctionArgPlan {
                    name: "x".to_owned(),
                    data_type: DataType::Float,
                    default: None,
                }],
                body: expr("CASE WHEN x IS NULL THEN 0 ELSE x END"),
            },
        ];
        let mut context = Context {
            schemas: &schemas,
            functions: &functions,
            visiting_functions: Vec::new(),
        };
        let scope = Scope::default();
        let actual = infer_type(
            &mut context,
            &scope,
            &FunctionPlan::Custom {
                function_id: 0,
                exprs: vec![expr("1")],
            },
        );
        assert_eq!(
            actual,
            Ok(ExprType::Known(DataType::Int)),
            "declared argument type"
        );
        let actual = infer_type(
            &mut context,
            &scope,
            &FunctionPlan::Custom {
                function_id: 1,
                exprs: vec![],
            },
        );
        assert_eq!(
            actual,
            Ok(ExprType::Known(DataType::Int)),
            "function returns a typed value"
        );
        let actual = infer_type(
            &mut context,
            &scope,
            &FunctionPlan::Custom {
                function_id: 2,
                exprs: vec![],
            },
        );
        assert_eq!(
            actual,
            Ok(ExprType::Unknown),
            "recursive type analysis terminates"
        );
        let actual = infer_type(
            &mut context,
            &scope,
            &FunctionPlan::Custom {
                function_id: 3,
                exprs: vec![],
            },
        );
        assert_eq!(
            actual,
            Ok(ExprType::Known(DataType::Int)),
            "nested call after recursive analysis"
        );
        let actual = infer_type(
            &mut context,
            &scope,
            &FunctionPlan::Custom {
                function_id: 4,
                exprs: vec![expr("NULL")],
            },
        );
        assert_eq!(
            actual,
            Ok(ExprType::Unknown),
            "body normalization is outside CTAS planning"
        );
        assert_eq!(
            functions[4].body,
            expr("CASE WHEN x IS NULL THEN 0 ELSE x END")
        );
        let actual = infer_type(
            &mut context,
            &scope,
            &FunctionPlan::Custom {
                function_id: 7,
                exprs: vec![],
            },
        );
        assert_eq!(
            actual,
            Err(Error::Planner(
                PlannerError::MalformedPlanCustomFunctionIndex(7)
            ))
        );
        let actual = infer_type(
            &mut context,
            &scope,
            &FunctionPlan::UnplannedCustom {
                name: "PENDING".to_owned(),
                exprs: vec![],
            },
        );
        assert_eq!(
            actual,
            Err(Error::Planner(PlannerError::UnplannedCustomFunction(
                "PENDING".to_owned()
            )))
        );
    }
}
