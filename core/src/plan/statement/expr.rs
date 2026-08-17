mod aggregate;
mod function;

pub use {
    aggregate::{AggregateExprPlan, AggregateFunctionPlan, CountArgExprPlan},
    function::FunctionExprPlan,
};

use {
    super::QueryPlan,
    crate::{
        ast::{self, BinaryOperator, DataType, DateTimeField, Literal, ToSql, UnaryOperator},
        data::Value,
        plan::explain::{Explain, ExplainContext, ExplainSubqueryMode},
    },
    serde::{Deserialize, Serialize},
};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ExprPlan {
    Identifier(String),
    CompoundIdentifier {
        alias: String,
        ident: String,
    },
    IsNull(Box<ExprPlan>),
    IsNotNull(Box<ExprPlan>),
    InList {
        expr: Box<ExprPlan>,
        list: Vec<ExprPlan>,
        negated: bool,
    },
    InSubquery {
        expr: Box<ExprPlan>,
        subquery: Box<QueryPlan>,
        negated: bool,
    },
    Between {
        expr: Box<ExprPlan>,
        negated: bool,
        low: Box<ExprPlan>,
        high: Box<ExprPlan>,
    },
    Like {
        expr: Box<ExprPlan>,
        negated: bool,
        pattern: Box<ExprPlan>,
    },
    ILike {
        expr: Box<ExprPlan>,
        negated: bool,
        pattern: Box<ExprPlan>,
    },
    BinaryOp {
        left: Box<ExprPlan>,
        op: BinaryOperator,
        right: Box<ExprPlan>,
    },
    UnaryOp {
        op: UnaryOperator,
        expr: Box<ExprPlan>,
    },
    Nested(Box<ExprPlan>),
    Literal(Literal),
    Value(Value),
    TypedString {
        data_type: DataType,
        value: String,
    },
    Function(Box<FunctionExprPlan>),
    Aggregate(Box<AggregateExprPlan>),
    Exists {
        subquery: Box<QueryPlan>,
        negated: bool,
    },
    Subquery(Box<QueryPlan>),
    Case {
        operand: Option<Box<ExprPlan>>,
        when_then: Vec<(ExprPlan, ExprPlan)>,
        else_result: Option<Box<ExprPlan>>,
    },
    ArrayIndex {
        obj: Box<ExprPlan>,
        indexes: Vec<ExprPlan>,
    },
    Interval {
        expr: Box<ExprPlan>,
        leading_field: Option<DateTimeField>,
        last_field: Option<DateTimeField>,
    },
    Array {
        elem: Vec<ExprPlan>,
    },
}

pub fn plan_scalar_expr(expr: ast::Expr) -> ExprPlan {
    expr.into()
}

impl From<ast::Expr> for ExprPlan {
    fn from(expr: ast::Expr) -> Self {
        match expr {
            ast::Expr::Identifier(ident) => Self::Identifier(ident),
            ast::Expr::CompoundIdentifier { alias, ident } => {
                Self::CompoundIdentifier { alias, ident }
            }
            ast::Expr::IsNull(expr) => Self::IsNull(Box::new((*expr).into())),
            ast::Expr::IsNotNull(expr) => Self::IsNotNull(Box::new((*expr).into())),
            ast::Expr::InList {
                expr,
                list,
                negated,
            } => Self::InList {
                expr: Box::new((*expr).into()),
                list: list.into_iter().map(Into::into).collect(),
                negated,
            },
            ast::Expr::InSubquery {
                expr,
                subquery,
                negated,
            } => Self::InSubquery {
                expr: Box::new((*expr).into()),
                subquery: Box::new((*subquery).into()),
                negated,
            },
            ast::Expr::Between {
                expr,
                negated,
                low,
                high,
            } => Self::Between {
                expr: Box::new((*expr).into()),
                negated,
                low: Box::new((*low).into()),
                high: Box::new((*high).into()),
            },
            ast::Expr::Like {
                expr,
                negated,
                pattern,
            } => Self::Like {
                expr: Box::new((*expr).into()),
                negated,
                pattern: Box::new((*pattern).into()),
            },
            ast::Expr::ILike {
                expr,
                negated,
                pattern,
            } => Self::ILike {
                expr: Box::new((*expr).into()),
                negated,
                pattern: Box::new((*pattern).into()),
            },
            ast::Expr::BinaryOp { left, op, right } => Self::BinaryOp {
                left: Box::new((*left).into()),
                op,
                right: Box::new((*right).into()),
            },
            ast::Expr::UnaryOp { op, expr } => Self::UnaryOp {
                op,
                expr: Box::new((*expr).into()),
            },
            ast::Expr::Nested(expr) => Self::Nested(Box::new((*expr).into())),
            ast::Expr::Literal(literal) => Self::Literal(literal),
            ast::Expr::Value(value) => Self::Value(value),
            ast::Expr::TypedString { data_type, value } => Self::TypedString { data_type, value },
            ast::Expr::Function(function) => Self::Function(Box::new((*function).into())),
            ast::Expr::Aggregate(aggregate) => Self::Aggregate(Box::new((*aggregate).into())),
            ast::Expr::Exists { subquery, negated } => Self::Exists {
                subquery: Box::new((*subquery).into()),
                negated,
            },
            ast::Expr::Subquery(query) => Self::Subquery(Box::new((*query).into())),
            ast::Expr::Case {
                operand,
                when_then,
                else_result,
            } => Self::Case {
                operand: operand.map(|expr| Box::new((*expr).into())),
                when_then: when_then
                    .into_iter()
                    .map(|(when, then)| (when.into(), then.into()))
                    .collect(),
                else_result: else_result.map(|expr| Box::new((*expr).into())),
            },
            ast::Expr::ArrayIndex { obj, indexes } => Self::ArrayIndex {
                obj: Box::new((*obj).into()),
                indexes: indexes.into_iter().map(Into::into).collect(),
            },
            ast::Expr::Interval {
                expr,
                leading_field,
                last_field,
            } => Self::Interval {
                expr: Box::new((*expr).into()),
                leading_field,
                last_field,
            },
            ast::Expr::Array { elem } => Self::Array {
                elem: elem.into_iter().map(Into::into).collect(),
            },
        }
    }
}

impl Explain for ExprPlan {
    type Output = String;

    fn explain(&self, context: &mut ExplainContext) -> String {
        let mut output = String::new();
        fmt_expr(self, context, &mut output);
        output
    }
}

fn fmt_expr(expr: &ExprPlan, context: &mut ExplainContext, output: &mut String) {
    match expr {
        ExprPlan::Identifier(ident) => output.push_str(ident),
        ExprPlan::CompoundIdentifier { alias, ident } => {
            output.push_str(alias);
            output.push('.');
            output.push_str(ident);
        }
        ExprPlan::IsNull(expr) => {
            fmt_expr(expr, context, output);
            output.push_str(" IS NULL");
        }
        ExprPlan::IsNotNull(expr) => {
            fmt_expr(expr, context, output);
            output.push_str(" IS NOT NULL");
        }
        ExprPlan::InList {
            expr,
            list,
            negated,
        } => {
            fmt_expr(expr, context, output);
            output.push_str(if *negated { " NOT IN (" } else { " IN (" });
            fmt_expr_list(list, context, output);
            output.push(')');
        }
        ExprPlan::InSubquery {
            expr,
            subquery,
            negated,
        } => {
            fmt_expr(expr, context, output);
            let id = context.register_subquery(subquery, ExplainSubqueryMode::AllRows);
            output.push(' ');
            if *negated {
                output.push_str("NOT ");
            }
            output.push_str("IN (");
            output.push_str(&id);
            output.push(')');
        }
        ExprPlan::Between {
            expr,
            negated,
            low,
            high,
        } => {
            fmt_expr(expr, context, output);
            output.push_str(if *negated {
                " NOT BETWEEN "
            } else {
                " BETWEEN "
            });
            fmt_expr(low, context, output);
            output.push_str(" AND ");
            fmt_expr(high, context, output);
        }
        ExprPlan::Like {
            expr,
            negated,
            pattern,
        } => {
            fmt_expr(expr, context, output);
            output.push_str(if *negated { " NOT LIKE " } else { " LIKE " });
            fmt_expr(pattern, context, output);
        }
        ExprPlan::ILike {
            expr,
            negated,
            pattern,
        } => {
            fmt_expr(expr, context, output);
            output.push_str(if *negated { " NOT ILIKE " } else { " ILIKE " });
            fmt_expr(pattern, context, output);
        }
        ExprPlan::BinaryOp { left, op, right } => {
            fmt_expr(left, context, output);
            output.push(' ');
            output.push_str(&op.to_sql());
            output.push(' ');
            fmt_expr(right, context, output);
        }
        ExprPlan::UnaryOp { op, expr } => {
            if op == &UnaryOperator::Factorial {
                fmt_expr(expr, context, output);
                output.push_str(&op.to_sql());
            } else {
                output.push_str(&op.to_sql());
                fmt_expr(expr, context, output);
            }
        }
        ExprPlan::Nested(expr) => {
            output.push('(');
            fmt_expr(expr, context, output);
            output.push(')');
        }
        ExprPlan::Literal(literal) => output.push_str(&literal.to_sql()),
        ExprPlan::Value(value) => output.push_str(&value.to_sql()),
        ExprPlan::TypedString { data_type, value } => {
            output.push_str(&data_type.to_string());
            output.push_str(" '");
            output.push_str(value);
            output.push('\'');
        }
        ExprPlan::Function(function) => output.push_str(&function.explain(context)),
        ExprPlan::Aggregate(aggregate) => output.push_str(&aggregate.explain(context)),
        ExprPlan::Exists { subquery, negated } => {
            let id = context.register_subquery(subquery, ExplainSubqueryMode::Exists);
            if *negated {
                output.push_str("NOT ");
            }
            output.push_str("EXISTS (");
            output.push_str(&id);
            output.push(')');
        }
        ExprPlan::Subquery(subquery) => {
            let id = context.register_subquery(subquery, ExplainSubqueryMode::OneRow);
            output.push_str(&id);
        }
        ExprPlan::Case {
            operand,
            when_then,
            else_result,
        } => {
            output.push_str("CASE");
            if let Some(operand) = operand {
                output.push(' ');
                fmt_expr(operand, context, output);
            }
            for (when, then) in when_then {
                output.push_str(" WHEN ");
                fmt_expr(when, context, output);
                output.push_str(" THEN ");
                fmt_expr(then, context, output);
            }
            if let Some(else_result) = else_result {
                output.push_str(" ELSE ");
                fmt_expr(else_result, context, output);
            }
            output.push_str(" END");
        }
        ExprPlan::ArrayIndex { obj, indexes } => {
            fmt_expr(obj, context, output);
            for index in indexes {
                output.push('[');
                fmt_expr(index, context, output);
                output.push(']');
            }
        }
        ExprPlan::Interval {
            expr,
            leading_field,
            last_field,
        } => {
            output.push_str("INTERVAL ");
            fmt_expr(expr, context, output);
            if let Some(field) = leading_field {
                output.push(' ');
                output.push_str(&field.to_string());
            }
            if let Some(field) = last_field {
                output.push_str(" TO ");
                output.push_str(&field.to_string());
            }
        }
        ExprPlan::Array { elem } => {
            output.push('[');
            fmt_expr_list(elem, context, output);
            output.push(']');
        }
    }
}

impl Explain for [ExprPlan] {
    type Output = String;

    fn explain(&self, context: &mut ExplainContext) -> String {
        let mut output = String::new();
        fmt_expr_list(self, context, &mut output);
        output
    }
}

fn fmt_expr_list(exprs: &[ExprPlan], context: &mut ExplainContext, output: &mut String) {
    for (index, expr) in exprs.iter().enumerate() {
        if index > 0 {
            output.push_str(", ");
        }
        fmt_expr(expr, context, output);
    }
}

#[cfg(test)]
mod tests {
    use {
        super::{
            AggregateExprPlan, AggregateFunctionPlan, CountArgExprPlan, ExprPlan, FunctionExprPlan,
        },
        crate::{
            ast::{BinaryOperator, DataType, DateTimeField, Literal, UnaryOperator},
            data::Value,
            plan::{
                QueryPlan, ValuesPlan,
                explain::{Explain, ExplainContext},
            },
        },
    };

    fn test(actual: &ExprPlan, expected: &str) {
        assert_eq!(actual.explain(&mut ExplainContext::default()), expected);
    }

    #[test]
    fn explain() {
        let actual = ExprPlan::Identifier("id".to_owned());
        let expected = "id";
        test(&actual, expected);

        let actual = ExprPlan::CompoundIdentifier {
            alias: "Player".to_owned(),
            ident: "id".to_owned(),
        };
        let expected = "Player.id";
        test(&actual, expected);

        let actual = ExprPlan::IsNull(Box::new(ExprPlan::Identifier("team_id".to_owned())));
        let expected = "team_id IS NULL";
        test(&actual, expected);

        let actual = ExprPlan::IsNotNull(Box::new(ExprPlan::Identifier("team_id".to_owned())));
        let expected = "team_id IS NOT NULL";
        test(&actual, expected);

        let actual = ExprPlan::InList {
            expr: Box::new(ExprPlan::Identifier("id".to_owned())),
            list: vec![
                ExprPlan::Literal(Literal::Number(1.into())),
                ExprPlan::Literal(Literal::Number(2.into())),
            ],
            negated: false,
        };
        let expected = "id IN (1, 2)";
        test(&actual, expected);

        let actual = ExprPlan::InList {
            expr: Box::new(ExprPlan::Identifier("id".to_owned())),
            list: vec![ExprPlan::Literal(Literal::Number(1.into()))],
            negated: true,
        };
        let expected = "id NOT IN (1)";
        test(&actual, expected);

        let subquery = QueryPlan::Values(ValuesPlan(vec![vec![ExprPlan::Literal(
            Literal::Number(1.into()),
        )]]));
        let actual = ExprPlan::InSubquery {
            expr: Box::new(ExprPlan::Identifier("id".to_owned())),
            subquery: Box::new(subquery.clone()),
            negated: false,
        };
        let expected = "id IN (@S1)";
        test(&actual, expected);

        let actual = ExprPlan::InSubquery {
            expr: Box::new(ExprPlan::Identifier("id".to_owned())),
            subquery: Box::new(subquery.clone()),
            negated: true,
        };
        let expected = "id NOT IN (@S1)";
        test(&actual, expected);

        let actual = ExprPlan::Between {
            expr: Box::new(ExprPlan::Identifier("score".to_owned())),
            negated: false,
            low: Box::new(ExprPlan::Literal(Literal::Number(1.into()))),
            high: Box::new(ExprPlan::Literal(Literal::Number(10.into()))),
        };
        let expected = "score BETWEEN 1 AND 10";
        test(&actual, expected);

        let actual = ExprPlan::Between {
            expr: Box::new(ExprPlan::Identifier("score".to_owned())),
            negated: true,
            low: Box::new(ExprPlan::Literal(Literal::Number(1.into()))),
            high: Box::new(ExprPlan::Literal(Literal::Number(10.into()))),
        };
        let expected = "score NOT BETWEEN 1 AND 10";
        test(&actual, expected);

        let actual = ExprPlan::Like {
            expr: Box::new(ExprPlan::Identifier("name".to_owned())),
            negated: false,
            pattern: Box::new(ExprPlan::Literal(Literal::QuotedString("A%".to_owned()))),
        };
        let expected = "name LIKE 'A%'";
        test(&actual, expected);

        let actual = ExprPlan::Like {
            expr: Box::new(ExprPlan::Identifier("name".to_owned())),
            negated: true,
            pattern: Box::new(ExprPlan::Literal(Literal::QuotedString("A%".to_owned()))),
        };
        let expected = "name NOT LIKE 'A%'";
        test(&actual, expected);

        let actual = ExprPlan::ILike {
            expr: Box::new(ExprPlan::Identifier("name".to_owned())),
            negated: false,
            pattern: Box::new(ExprPlan::Literal(Literal::QuotedString("a%".to_owned()))),
        };
        let expected = "name ILIKE 'a%'";
        test(&actual, expected);

        let actual = ExprPlan::ILike {
            expr: Box::new(ExprPlan::Identifier("name".to_owned())),
            negated: true,
            pattern: Box::new(ExprPlan::Literal(Literal::QuotedString("a%".to_owned()))),
        };
        let expected = "name NOT ILIKE 'a%'";
        test(&actual, expected);

        let actual = ExprPlan::BinaryOp {
            left: Box::new(ExprPlan::CompoundIdentifier {
                alias: "Player".to_owned(),
                ident: "id".to_owned(),
            }),
            op: BinaryOperator::Eq,
            right: Box::new(ExprPlan::Literal(Literal::Number(1.into()))),
        };
        let expected = "Player.id = 1";
        test(&actual, expected);

        let actual = ExprPlan::UnaryOp {
            op: UnaryOperator::Minus,
            expr: Box::new(ExprPlan::Literal(Literal::Number(1.into()))),
        };
        let expected = "-1";
        test(&actual, expected);

        let actual = ExprPlan::UnaryOp {
            op: UnaryOperator::Factorial,
            expr: Box::new(ExprPlan::Literal(Literal::Number(5.into()))),
        };
        let expected = "5!";
        test(&actual, expected);

        let actual = ExprPlan::Nested(Box::new(ExprPlan::BinaryOp {
            left: Box::new(ExprPlan::Identifier("a".to_owned())),
            op: BinaryOperator::Plus,
            right: Box::new(ExprPlan::Identifier("b".to_owned())),
        }));
        let expected = "(a + b)";
        test(&actual, expected);

        let actual = ExprPlan::Literal(Literal::QuotedString("GlueSQL".to_owned()));
        let expected = "'GlueSQL'";
        test(&actual, expected);

        let actual = ExprPlan::Value(Value::Bool(true));
        let expected = "TRUE";
        test(&actual, expected);

        let actual = ExprPlan::TypedString {
            data_type: DataType::Date,
            value: "2026-08-17".to_owned(),
        };
        let expected = "DATE '2026-08-17'";
        test(&actual, expected);

        let actual = ExprPlan::Function(Box::new(FunctionExprPlan::Abs(ExprPlan::Identifier(
            "score".to_owned(),
        ))));
        let expected = "ABS(score)";
        test(&actual, expected);

        let actual = ExprPlan::Aggregate(Box::new(AggregateExprPlan {
            func: AggregateFunctionPlan::Count(CountArgExprPlan::Wildcard),
            distinct: false,
            slot: Some(0),
        }));
        let expected = "COUNT(*)";
        test(&actual, expected);

        let actual = ExprPlan::Exists {
            subquery: Box::new(subquery.clone()),
            negated: false,
        };
        let expected = "EXISTS (@S1)";
        test(&actual, expected);

        let actual = ExprPlan::Exists {
            subquery: Box::new(subquery.clone()),
            negated: true,
        };
        let expected = "NOT EXISTS (@S1)";
        test(&actual, expected);

        let actual = ExprPlan::Subquery(Box::new(subquery));
        let expected = "@S1";
        test(&actual, expected);

        let actual = ExprPlan::Case {
            operand: Some(Box::new(ExprPlan::Identifier("status".to_owned()))),
            when_then: vec![(
                ExprPlan::Literal(Literal::Number(1.into())),
                ExprPlan::Literal(Literal::QuotedString("active".to_owned())),
            )],
            else_result: Some(Box::new(ExprPlan::Literal(Literal::QuotedString(
                "inactive".to_owned(),
            )))),
        };
        let expected = "CASE status WHEN 1 THEN 'active' ELSE 'inactive' END";
        test(&actual, expected);

        let actual = ExprPlan::Case {
            operand: None,
            when_then: Vec::new(),
            else_result: None,
        };
        let expected = "CASE END";
        test(&actual, expected);

        let actual = ExprPlan::ArrayIndex {
            obj: Box::new(ExprPlan::Identifier("matrix".to_owned())),
            indexes: vec![
                ExprPlan::Literal(Literal::Number(1.into())),
                ExprPlan::Literal(Literal::Number(2.into())),
            ],
        };
        let expected = "matrix[1][2]";
        test(&actual, expected);

        let actual = ExprPlan::Interval {
            expr: Box::new(ExprPlan::Literal(Literal::QuotedString("1".to_owned()))),
            leading_field: Some(DateTimeField::Day),
            last_field: Some(DateTimeField::Hour),
        };
        let expected = "INTERVAL '1' DAY TO HOUR";
        test(&actual, expected);

        let actual = ExprPlan::Interval {
            expr: Box::new(ExprPlan::Literal(Literal::QuotedString("1 day".to_owned()))),
            leading_field: None,
            last_field: None,
        };
        let expected = "INTERVAL '1 day'";
        test(&actual, expected);

        let actual = ExprPlan::Array {
            elem: vec![
                ExprPlan::Literal(Literal::Number(1.into())),
                ExprPlan::Literal(Literal::Number(2.into())),
            ],
        };
        let expected = "[1, 2]";
        test(&actual, expected);

        let actual = [
            ExprPlan::Identifier("id".to_owned()),
            ExprPlan::Identifier("name".to_owned()),
        ];
        let expected = "id, name";

        assert_eq!(
            actual.as_slice().explain(&mut ExplainContext::default()),
            expected
        );
    }
}
