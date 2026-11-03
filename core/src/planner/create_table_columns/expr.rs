mod function;

use {
    super::{
        Context,
        query::{self, QueryColumns},
        source::Scope,
    },
    crate::{
        ast::{BinaryOperator, DataType, Literal, UnaryOperator},
        data::Value,
        executor::evaluate_stateless,
        plan::{AggregateFunctionPlan, ExprPlan, FunctionPlan, QueryPlan},
        planner::{
            PlannerError,
            visit_mut::{VisitorMut, walk_expr_mut},
        },
        result::{Error, Result},
    },
    std::hash::BuildHasher,
};

#[derive(Debug, Clone, PartialEq)]
pub(super) enum ExprType {
    Known(DataType),
    Literal(DataType),
    Null,
    Unknown,
    Conflict { expected: DataType, found: DataType },
}

impl ExprType {
    pub fn data_type(&self) -> Option<DataType> {
        match self {
            Self::Known(data_type) | Self::Literal(data_type) => Some(data_type.clone()),
            Self::Null | Self::Unknown | Self::Conflict { .. } => None,
        }
    }

    pub fn require_type(&self, column: &str) -> Result<DataType> {
        match self {
            Self::Conflict { expected, found } => Err(PlannerError::CtasColumnTypeMismatch {
                column: column.to_owned(),
                expected: expected.clone(),
                found: found.clone(),
            }
            .into()),
            _ => self
                .data_type()
                .ok_or_else(|| PlannerError::CtasColumnTypeUnknown(column.to_owned()).into()),
        }
    }
}

pub(super) fn infer_type<S: BuildHasher>(
    context: &mut Context<'_, S>,
    scope: &Scope<'_>,
    expr: &ExprPlan,
) -> Result<ExprType> {
    use {DataType as D, ExprType as T};
    if numeric_literal(expr) {
        return Ok(Value::try_from(evaluate_stateless(None, expr)?)?
            .get_type()
            .map_or(T::Null, T::Literal));
    }
    Ok(match expr {
        ExprPlan::Identifier(name) => scope.column(None, name)?,
        ExprPlan::CompoundIdentifier { alias, ident } => scope.column(Some(alias), ident)?,
        ExprPlan::Literal(Literal::QuotedString(_)) => T::Literal(D::Text),
        ExprPlan::Literal(Literal::Number(_)) => Value::try_from(evaluate_stateless(None, expr)?)?
            .get_type()
            .map_or(T::Null, T::Literal),
        ExprPlan::Value(value) => value.get_type().map_or(T::Null, T::Known),
        ExprPlan::TypedString { data_type, .. } => T::Known(data_type.clone()),
        ExprPlan::Nested(expr) => infer_type(context, scope, expr)?,
        ExprPlan::IsNull(_)
        | ExprPlan::IsNotNull(_)
        | ExprPlan::InList { .. }
        | ExprPlan::InSubquery { .. }
        | ExprPlan::Between { .. }
        | ExprPlan::Like { .. }
        | ExprPlan::ILike { .. }
        | ExprPlan::Regex { .. }
        | ExprPlan::Exists { .. } => T::Known(D::Boolean),
        ExprPlan::BinaryOp { op, left, right } => match op {
            BinaryOperator::Gt
            | BinaryOperator::Lt
            | BinaryOperator::GtEq
            | BinaryOperator::LtEq
            | BinaryOperator::Eq
            | BinaryOperator::NotEq
            | BinaryOperator::And
            | BinaryOperator::Or
            | BinaryOperator::Xor => T::Known(D::Boolean),
            BinaryOperator::StringConcat | BinaryOperator::LongArrow => T::Known(D::Text),
            BinaryOperator::Arrow => T::Unknown,
            BinaryOperator::Plus
            | BinaryOperator::Minus
            | BinaryOperator::Multiply
            | BinaryOperator::Divide
            | BinaryOperator::Modulo
            | BinaryOperator::BitwiseAnd
            | BinaryOperator::BitwiseShiftLeft
            | BinaryOperator::BitwiseShiftRight => {
                let left = infer_type(context, scope, left)?;
                let right = infer_type(context, scope, right)?;
                match (left.data_type(), right.data_type()) {
                    (Some(left), Some(right)) if numeric(&left) && numeric(&right) => {
                        let data_type =
                            if matches!(left, D::Float32 | D::Float) && right == D::Decimal {
                                D::Decimal
                            } else {
                                left
                            };
                        T::Known(data_type)
                    }
                    (Some(data_type), None) if right == T::Null && numeric(&data_type) => {
                        T::Known(data_type)
                    }
                    (None, _) if left == T::Null => T::Null,
                    _ => T::Unknown,
                }
            }
        },
        ExprPlan::UnaryOp { op, expr } => match op {
            UnaryOperator::Not => T::Known(D::Boolean),
            UnaryOperator::Factorial => T::Known(D::Int128),
            UnaryOperator::Plus | UnaryOperator::Minus | UnaryOperator::BitwiseNot => {
                match infer_type(context, scope, expr)? {
                    T::Literal(data_type) => T::Known(data_type),
                    other => other,
                }
            }
        },
        ExprPlan::Function(function) => function::infer_type(context, scope, function)?,
        ExprPlan::Aggregate(aggregate) => match &aggregate.func {
            AggregateFunctionPlan::Count(_) => T::Known(D::Int),
            AggregateFunctionPlan::Avg(_)
            | AggregateFunctionPlan::Variance(_)
            | AggregateFunctionPlan::Stdev(_) => T::Known(D::Float),
            AggregateFunctionPlan::Sum(expr)
            | AggregateFunctionPlan::Min(expr)
            | AggregateFunctionPlan::Max(expr) => infer_type(context, scope, expr)?
                .data_type()
                .map_or(T::Unknown, T::Known),
        },
        ExprPlan::Subquery(query) => scalar_type(query::infer_query(context, query, Some(scope))?)?,
        ExprPlan::Case {
            when_then,
            else_result,
            ..
        } => {
            let mut types = Vec::new();
            for (_, expr) in when_then {
                types.push(infer_type(context, scope, expr)?);
            }
            if let Some(expr) = else_result {
                types.push(infer_type(context, scope, expr)?);
            }
            candidate_type(&types)
        }
        ExprPlan::ArrayIndex { .. } => T::Unknown,
        ExprPlan::Interval { .. } => T::Known(D::Interval),
        ExprPlan::Array { .. } => T::Known(D::List),
    })
}

fn numeric(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int
            | DataType::Int128
            | DataType::Uint8
            | DataType::Uint16
            | DataType::Uint32
            | DataType::Uint64
            | DataType::Uint128
            | DataType::Float32
            | DataType::Float
            | DataType::Decimal
    )
}

fn numeric_literal(expr: &ExprPlan) -> bool {
    match expr {
        ExprPlan::Literal(Literal::Number(_)) => true,
        ExprPlan::Function(function) => match function.as_ref() {
            FunctionPlan::Mod { dividend, divisor } => {
                numeric_literal(dividend) && numeric_literal(divisor)
            }
            _ => false,
        },
        ExprPlan::Nested(expr)
        | ExprPlan::UnaryOp {
            op: UnaryOperator::Plus | UnaryOperator::Minus,
            expr,
        } => numeric_literal(expr),
        ExprPlan::BinaryOp {
            left,
            right,
            op:
                BinaryOperator::Plus
                | BinaryOperator::Minus
                | BinaryOperator::Multiply
                | BinaryOperator::Divide
                | BinaryOperator::Modulo,
        } => numeric_literal(left) && numeric_literal(right),
        _ => false,
    }
}

fn scalar_type(output: QueryColumns) -> Result<ExprType> {
    match output {
        QueryColumns::Columns(mut columns) => {
            query::check_width(1, columns.len())?;
            Ok(columns.remove(0).expr_type)
        }
        QueryColumns::Schemaless => Err(PlannerError::CtasSourceFormatMismatch.into()),
    }
}

// Read-only analysis uses the types that each branch actually returns. Preparation
// can make literals agree first; stored custom function bodies remain untouched.
fn candidate_type(types: &[ExprType]) -> ExprType {
    let mut candidate: Option<DataType> = None;
    let mut unknown = false;
    for expr_type in types {
        if matches!(expr_type, ExprType::Conflict { .. }) {
            return expr_type.clone();
        }
        if *expr_type == ExprType::Unknown {
            unknown = true;
        }
        if let Some(data_type) = expr_type.data_type() {
            if let Some(expected) = &candidate {
                if expected != &data_type {
                    return ExprType::Conflict {
                        expected: expected.clone(),
                        found: data_type,
                    };
                }
            } else {
                candidate = Some(data_type);
            }
        }
    }
    if unknown {
        ExprType::Unknown
    } else {
        candidate.map_or(ExprType::Null, ExprType::Known)
    }
}

pub(super) fn check_type(expr_type: &ExprType, expected: &DataType, column: &str) -> Result<()> {
    if matches!(expr_type, ExprType::Conflict { .. }) {
        expr_type.require_type(column)?;
    }
    if let Some(found) = expr_type.data_type()
        && &found != expected
    {
        return Err(PlannerError::CtasColumnTypeMismatch {
            column: column.to_owned(),
            expected: expected.clone(),
            found,
        }
        .into());
    }
    Ok(())
}

pub(super) fn prepare_expr<S: BuildHasher>(
    context: &mut Context<'_, S>,
    scope: &Scope<'_>,
    expr: &mut ExprPlan,
    target: Option<(&DataType, &str)>,
) -> Result<()> {
    // Preserve decimal literal arithmetic until the whole result is converted.
    if numeric_literal(expr) || matches!(expr, ExprPlan::Literal(_)) {
        if let Some((data_type, _)) = target {
            *expr =
                ExprPlan::Value(evaluate_stateless(None, expr)?.try_into_value(data_type, true)?);
        }
        return Ok(());
    }
    match expr {
        ExprPlan::Nested(inner) => return prepare_expr(context, scope, inner, target),
        ExprPlan::Subquery(query) => {
            let expected = target.map(|(data_type, _)| std::slice::from_ref(data_type));
            scalar_type(query::plan_query(context, query, Some(scope), expected)?)?;
        }
        _ => walk_expr_mut(&mut PrepareChildren { context, scope }, expr)?,
    }
    match expr {
        ExprPlan::Case {
            when_then,
            else_result,
            ..
        } => {
            let mut candidates: Vec<_> = when_then.iter_mut().map(|(_, then)| then).collect();
            if let Some(expr) = else_result {
                candidates.push(expr);
            }
            prepare_candidates(context, scope, candidates, target)?;
        }
        ExprPlan::Function(function) => match function.as_mut() {
            FunctionPlan::Coalesce(exprs) | FunctionPlan::Greatest(exprs) => {
                prepare_candidates(context, scope, exprs.iter_mut().collect(), target)?;
            }
            FunctionPlan::IfNull { expr, then } => {
                prepare_candidates(context, scope, vec![expr, then], target)?;
            }
            FunctionPlan::NullIf { expr1, .. } => prepare_expr(context, scope, expr1, target)?,
            _ => {}
        },
        _ => {}
    }
    if let Some((expected, label)) = target {
        check_type(&infer_type(context, scope, expr)?, expected, label)?;
    }
    Ok(())
}

fn prepare_candidates<S: BuildHasher>(
    context: &mut Context<'_, S>,
    scope: &Scope<'_>,
    candidates: Vec<&mut ExprPlan>,
    target: Option<(&DataType, &str)>,
) -> Result<()> {
    let types = candidates
        .iter()
        .map(|expr| infer_type(context, scope, expr))
        .collect::<Result<Vec<_>>>()?;
    let mut expected = target.map(|(data_type, _)| data_type.clone());
    for expr_type in &types {
        if let ExprType::Known(data_type) = expr_type {
            match &expected {
                Some(expected) if expected != data_type => {
                    // A surrounding CAST may resolve differing branch types. Only
                    // reject them when this expression's output type is required.
                    if target.is_none() {
                        return Ok(());
                    }
                    check_type(
                        expr_type,
                        expected,
                        target.map_or("return expression", |(_, name)| name),
                    )?;
                }
                None => expected = Some(data_type.clone()),
                _ => {}
            }
        }
    }
    let Some(expected) = expected else {
        return Ok(());
    };
    for (candidate, expr_type) in candidates.into_iter().zip(types) {
        if matches!(expr_type, ExprType::Literal(_)) {
            prepare_expr(
                context,
                scope,
                candidate,
                Some((&expected, "return expression")),
            )?;
        }
    }
    Ok(())
}

struct PrepareChildren<'a, 'b, 'c, S> {
    context: &'a mut Context<'b, S>,
    scope: &'c Scope<'c>,
}

impl<S: BuildHasher> VisitorMut for PrepareChildren<'_, '_, '_, S> {
    type Error = Error;

    fn visit_expr(&mut self, expr: &mut ExprPlan) -> Result<()> {
        prepare_expr(self.context, self.scope, expr, None)
    }

    fn visit_query(&mut self, query: &mut QueryPlan) -> Result<()> {
        query::plan_query(self.context, query, Some(self.scope), None).map(|_| ())
    }
}

#[cfg(test)]
mod tests {
    use {
        super::{Context, ExprType, infer_type, prepare_expr},
        crate::{
            ast::DataType,
            data::Value,
            parse_sql::parse_expr,
            plan::ExprPlan,
            planner::{
                PlannerError,
                create_table_columns::{
                    Column,
                    source::{Relation, Scope},
                },
            },
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

    fn scope() -> Scope<'static> {
        Scope {
            relations: vec![Relation {
                alias: "src".to_owned(),
                columns: vec![
                    Column::new("i", DataType::Int),
                    Column::new("f", DataType::Float),
                    Column::new("flag", DataType::Boolean),
                ],
                dynamic: false,
            }],
            outer: None,
        }
    }

    #[test]
    fn expression_types() {
        let schemas = HashMap::new();
        let mut context = Context {
            schemas: &schemas,
            functions: &[],
            visiting_functions: Vec::new(),
        };
        let scope = scope();
        let mut test = |sql: &str, expected: ExprType| {
            let actual = infer_type(&mut context, &scope, &expr(sql));
            assert_eq!(actual, Ok(expected), "{sql}");
        };
        test("1", ExprType::Literal(DataType::Int));
        test("1 / 2", ExprType::Literal(DataType::Float));
        test("-((1 + 2) * 3 % 5)", ExprType::Literal(DataType::Int));
        test("'text'", ExprType::Literal(DataType::Text));
        test("NULL", ExprType::Null);
        test("TRUE", ExprType::Known(DataType::Boolean));
        test("INT '1'", ExprType::Known(DataType::Int));
        test("(i)", ExprType::Known(DataType::Int));
        test("src.f", ExprType::Known(DataType::Float));
        test("f + i", ExprType::Known(DataType::Float));
        test("i + f", ExprType::Known(DataType::Int));
        test("f + CAST(1 AS DECIMAL)", ExprType::Known(DataType::Decimal));
        test("f * NULL", ExprType::Known(DataType::Float));
        test("NULL * f", ExprType::Null);
        test("DATE '2024-01-01' + INTERVAL '1' DAY", ExprType::Unknown);
        test("i IS NULL", ExprType::Known(DataType::Boolean));
        test("i IS NOT NULL", ExprType::Known(DataType::Boolean));
        test("i IN (1, 2)", ExprType::Known(DataType::Boolean));
        test("i IN (SELECT 1)", ExprType::Known(DataType::Boolean));
        test("i BETWEEN 1 AND 2", ExprType::Known(DataType::Boolean));
        test("'a' LIKE 'a'", ExprType::Known(DataType::Boolean));
        test("'a' ILIKE 'a'", ExprType::Known(DataType::Boolean));
        test("'a' ~ 'a'", ExprType::Known(DataType::Boolean));
        test("EXISTS(SELECT NULL)", ExprType::Known(DataType::Boolean));
        test("i > f AND flag", ExprType::Known(DataType::Boolean));
        test("'a' || 'b'", ExprType::Known(DataType::Text));
        test("CAST('{}' AS MAP) ->> 'x'", ExprType::Known(DataType::Text));
        test("CAST('{}' AS MAP) -> 'x'", ExprType::Unknown);
        test("~i", ExprType::Known(DataType::Int));
        test("+i", ExprType::Known(DataType::Int));
        test("-f", ExprType::Known(DataType::Float));
        test("i!", ExprType::Known(DataType::Int128));
        test("NOT flag", ExprType::Known(DataType::Boolean));
        test("COUNT(*)", ExprType::Known(DataType::Int));
        test("AVG(i)", ExprType::Known(DataType::Float));
        test("SUM(f)", ExprType::Known(DataType::Float));
        test("MIN(NULL)", ExprType::Unknown);
        test("(SELECT i)", ExprType::Known(DataType::Int));
        test("[1, 2]", ExprType::Known(DataType::List));
        test("[1, 2][1]", ExprType::Unknown);
        test("INTERVAL '1' DAY", ExprType::Known(DataType::Interval));
        test(
            "CASE WHEN flag THEN i ELSE NULL END",
            ExprType::Known(DataType::Int),
        );
        test("CASE WHEN flag THEN NULL END", ExprType::Null);
        test("CASE WHEN flag THEN i ELSE [1][1] END", ExprType::Unknown);
    }

    #[test]
    fn prepare_literals() {
        let schemas = HashMap::new();
        let mut context = Context {
            schemas: &schemas,
            functions: &[],
            visiting_functions: Vec::new(),
        };
        let scope = scope();
        let mut test = |sql: &str, target: DataType, expected: ExprPlan| {
            let mut actual = expr(sql);
            assert_eq!(
                prepare_expr(&mut context, &scope, &mut actual, Some((&target, "value"))),
                Ok(()),
                "{sql}"
            );
            assert_eq!(actual, expected, "{sql}");
        };
        test("1 / 2", DataType::Float, ExprPlan::Value(Value::F64(0.5)));
        test(
            "'text'",
            DataType::Text,
            ExprPlan::Value(Value::Str("text".to_owned())),
        );
        test("((1))", DataType::Float, ExprPlan::Value(Value::F64(1.0)));
        test(
            "CAST(1 / 2 AS FLOAT)",
            DataType::Float,
            expr("CAST(1 / 2 AS FLOAT)"),
        );
        test(
            "CAST(CASE WHEN flag THEN i ELSE f END AS FLOAT)",
            DataType::Float,
            expr("CAST(CASE WHEN flag THEN i ELSE f END AS FLOAT)"),
        );
        test("NULL", DataType::Float, expr("NULL"));
        test("[1][1]", DataType::Int, expr("[1][1]"));

        let mut input = expr("CAST(1 AS FLOAT)");
        let expected = Err(Error::Planner(PlannerError::CtasColumnTypeMismatch {
            column: "value".to_owned(),
            expected: DataType::Int,
            found: DataType::Float,
        }));
        assert_eq!(
            prepare_expr(
                &mut context,
                &scope,
                &mut input,
                Some((&DataType::Int, "value"))
            ),
            expected
        );

        let mut input = expr("'not a number'");
        assert_eq!(
            prepare_expr(
                &mut context,
                &scope,
                &mut input,
                Some((&DataType::Int, "value"))
            ),
            Err(Error::Evaluate(
                crate::executor::EvaluateError::TextParseFailed {
                    literal: "not a number".to_owned(),
                    data_type: DataType::Int
                }
            ))
        );
    }

    #[test]
    fn prepare_return_candidates() {
        let schemas = HashMap::new();
        let mut context = Context {
            schemas: &schemas,
            functions: &[],
            visiting_functions: Vec::new(),
        };
        let scope = scope();
        let mut actual = expr("CASE WHEN flag THEN 0 ELSE f END");
        let expected = ExprPlan::Case {
            operand: None,
            when_then: vec![(expr("flag"), ExprPlan::Value(Value::F64(0.0)))],
            else_result: Some(Box::new(expr("f"))),
        };
        assert_eq!(
            prepare_expr(&mut context, &scope, &mut actual, None),
            Ok(())
        );
        assert_eq!(actual, expected, "literal before typed branch");

        let mut actual = expr("CASE WHEN flag THEN f ELSE 0 END");
        let expected = ExprPlan::Case {
            operand: None,
            when_then: vec![(expr("flag"), expr("f"))],
            else_result: Some(Box::new(ExprPlan::Value(Value::F64(0.0)))),
        };
        assert_eq!(
            prepare_expr(&mut context, &scope, &mut actual, None),
            Ok(())
        );
        assert_eq!(actual, expected, "typed branch before literal");

        let mut actual = expr("CASE WHEN flag THEN 1 ELSE 2 END");
        assert_eq!(
            prepare_expr(&mut context, &scope, &mut actual, None),
            Ok(())
        );
        assert_eq!(
            actual,
            expr("CASE WHEN flag THEN 1 ELSE 2 END"),
            "no type promotion without a typed candidate"
        );

        let input = expr("CASE WHEN flag THEN i ELSE f END");
        let expected = Ok(ExprType::Conflict {
            expected: DataType::Int,
            found: DataType::Float,
        });
        assert_eq!(infer_type(&mut context, &scope, &input), expected);

        let mut input = expr("CASE WHEN flag THEN i ELSE 0 END");
        let expected = Err(Error::Planner(PlannerError::CtasColumnTypeMismatch {
            column: "value".to_owned(),
            expected: DataType::Float,
            found: DataType::Int,
        }));
        assert_eq!(
            prepare_expr(
                &mut context,
                &scope,
                &mut input,
                Some((&DataType::Float, "value"))
            ),
            expected
        );
    }

    #[test]
    fn subquery_output() {
        let schemas = HashMap::new();
        let mut context = Context {
            schemas: &schemas,
            functions: &[],
            visiting_functions: Vec::new(),
        };
        let scope = scope();
        let mut actual = expr("(SELECT CAST(1 / 2 AS FLOAT))");
        let expected = expr("(SELECT CAST(1 / 2 AS FLOAT))");
        assert_eq!(
            prepare_expr(
                &mut context,
                &scope,
                &mut actual,
                Some((&DataType::Float, "value"))
            ),
            Ok(())
        );
        assert_eq!(actual, expected);

        let actual = infer_type(&mut context, &scope, &expr("(SELECT 1, 2)"));
        let expected = Err(Error::Planner(PlannerError::CtasColumnCountMismatch {
            expected: 1,
            found: 2,
        }));
        assert_eq!(actual, expected, "scalar subquery width");

        let mut actual = expr("EXISTS(SELECT NULL)");
        assert_eq!(
            prepare_expr(&mut context, &scope, &mut actual, None),
            Ok(()),
            "untyped EXISTS projection needs no column type"
        );
    }
}
