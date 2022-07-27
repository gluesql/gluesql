use {
    super::{
        Aggregate, AstLiteral, BinaryOperator, DataType, DateTimeField, Function, Query, ToSql,
        UnaryOperator,
    },
    serde::{Deserialize, Serialize},
};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Expr {
    Identifier(String),
    CompoundIdentifier(Vec<String>),
    IsNull(Box<Expr>),
    IsNotNull(Box<Expr>),
    InList {
        expr: Box<Expr>,
        list: Vec<Expr>,
        negated: bool,
    },
    InSubquery {
        expr: Box<Expr>,
        subquery: Box<Query>,
        negated: bool,
    },
    Between {
        expr: Box<Expr>,
        negated: bool,
        low: Box<Expr>,
        high: Box<Expr>,
    },
    BinaryOp {
        left: Box<Expr>,
        op: BinaryOperator,
        right: Box<Expr>,
    },
    UnaryOp {
        op: UnaryOperator,
        expr: Box<Expr>,
    },
    Cast {
        expr: Box<Expr>,
        data_type: DataType,
    },
    Extract {
        field: DateTimeField,
        expr: Box<Expr>,
    },
    Nested(Box<Expr>),
    Literal(AstLiteral),
    TypedString {
        data_type: DataType,
        value: String,
    },
    Function(Box<Function>),
    Aggregate(Box<Aggregate>),
    Exists(Box<Query>),
    Subquery(Box<Query>),
    Case {
        operand: Option<Box<Expr>>,
        when_then: Vec<(Expr, Expr)>,
        else_result: Option<Box<Expr>>,
    },
}

impl ToSql for Expr {
    fn to_sql(&self) -> String {
        match self {
            Expr::Identifier(s) => s.to_string(),
            Expr::BinaryOp { left, op, right } => {
                format!("{} {} {}", left.to_sql(), op.to_sql(), right.to_sql())
            }
            Expr::CompoundIdentifier(idents) => idents.join("."),
            Expr::IsNull(s) => format!("{} IS NULL", s.to_sql()),
            Expr::IsNotNull(s) => format!("{} IS NOT NULL", s.to_sql()),
            Expr::InList {
                expr,
                list,
                negated,
            } => {
                let expr = expr.to_sql();
                let list = list
                    .iter()
                    .map(ToSql::to_sql)
                    .collect::<Vec<_>>()
                    .join(", ");

                match negated {
                    true => format!("{expr} NOT IN ({list})"),
                    false => format!("{expr} IN ({list})"),
                }
            }
            Expr::Between {
                expr,
                negated,
                low,
                high,
            } => {
                let expr = expr.to_sql();
                let low = low.to_sql();
                let high = high.to_sql();

                match negated {
                    true => format!("{expr} NOT BETWEEN {low} AND {high}"),
                    false => format!("{expr} BETWEEN {low} AND {high}"),
                }
            }
            Expr::UnaryOp { op, expr } => match op {
                UnaryOperator::Factorial => format!("{}{}", expr.to_sql(), op.to_sql()),
                _ => format!("{}{}", op.to_sql(), expr.to_sql()),
            },
            Expr::Cast { expr, data_type } => {
                format!("CAST({} AS {data_type})", expr.to_sql())
            }
            Expr::Extract { field, expr } => {
                format!(r#"EXTRACT({field} FROM "{}")"#, expr.to_sql())
            }
            Expr::Nested(expr) => format!("({})", expr.to_sql()),
            Expr::Literal(s) => s.to_sql(),
            Expr::TypedString { data_type, value } => format!("{data_type}(\"{value}\")"),
            Expr::Case {
                operand,
                when_then,
                else_result,
            } => {
                let operand = match operand {
                    Some(operand) => format!("CASE {}", operand.to_sql()),
                    None => "CASE".to_owned(),
                };

                let when_then = when_then
                    .iter()
                    .map(|(when, then)| format!("WHEN {} THEN {}", when.to_sql(), then.to_sql()))
                    .collect::<Vec<_>>()
                    .join("\n");

                let else_result = match else_result {
                    Some(else_result) => format!("ELSE {}", else_result.to_sql()),
                    None => String::new(),
                };

                [operand, when_then, else_result, "END".to_owned()].join("\n")
            }
            Expr::Aggregate(a) => a.to_sql(),
            Expr::Function(func) => format!("{func}(..)"),
            Expr::InSubquery { expr, negated, .. } => match negated {
                true => format!("{} NOT IN (..query..)", expr.to_sql()),
                false => format!("{} IN (..query..)", expr.to_sql()),
            },
            Expr::Exists(_) => "EXISTS(..query..)".to_string(),
            Expr::Subquery(_) => "(..query..)".to_string(),
        }
    }
}

#[cfg(test)]
mod tests {
    use {
        crate::ast::{
            Aggregate, AstLiteral, BinaryOperator, CountArgExpr, DataType, DateTimeField, Expr,
            Function, ToSql, UnaryOperator,
        },
        bigdecimal::BigDecimal,
        regex::Regex,
        std::str::FromStr,
    };

    #[test]
    fn to_sql() {
        let re = Regex::new(r"\n\s+").unwrap();
        let trim = |s: &str| re.replace_all(s.trim(), "\n").into_owned();

        assert_eq!("id", Expr::Identifier("id".to_string()).to_sql());

        assert_eq!(
            "id + num",
            Expr::BinaryOp {
                left: Box::new(Expr::Identifier("id".to_string())),
                op: BinaryOperator::Plus,
                right: Box::new(Expr::Identifier("num".to_string()))
            }
            .to_sql()
        );

        assert_eq!(
            "id - num",
            Expr::BinaryOp {
                left: Box::new(Expr::Identifier("id".to_string())),
                op: BinaryOperator::Minus,
                right: Box::new(Expr::Identifier("num".to_string()))
            }
            .to_sql()
        );

        assert_eq!(
            "id * num",
            Expr::BinaryOp {
                left: Box::new(Expr::Identifier("id".to_string())),
                op: BinaryOperator::Multiply,
                right: Box::new(Expr::Identifier("num".to_string()))
            }
            .to_sql()
        );

        assert_eq!(
            "id / num",
            Expr::BinaryOp {
                left: Box::new(Expr::Identifier("id".to_string())),
                op: BinaryOperator::Divide,
                right: Box::new(Expr::Identifier("num".to_string()))
            }
            .to_sql()
        );

        assert_eq!(
            "id % num",
            &Expr::BinaryOp {
                left: Box::new(Expr::Identifier("id".to_string())),
                op: BinaryOperator::Modulo,
                right: Box::new(Expr::Identifier("num".to_string()))
            }
            .to_sql()
        );

        assert_eq!(
            "+id",
            Expr::UnaryOp {
                op: UnaryOperator::Plus,
                expr: Box::new(Expr::Identifier("id".to_owned())),
            }
            .to_sql(),
        );

        assert_eq!(
            "-id",
            Expr::UnaryOp {
                op: UnaryOperator::Minus,
                expr: Box::new(Expr::Identifier("id".to_owned())),
            }
            .to_sql(),
        );

        assert_eq!(
            "NOT id",
            Expr::UnaryOp {
                op: UnaryOperator::Not,
                expr: Box::new(Expr::Identifier("id".to_owned())),
            }
            .to_sql(),
        );

        assert_eq!(
            "id!",
            Expr::UnaryOp {
                op: UnaryOperator::Factorial,
                expr: Box::new(Expr::Identifier("id".to_owned())),
            }
            .to_sql(),
        );

        assert_eq!(
            "id.name.first",
            Expr::CompoundIdentifier(vec![
                "id".to_string(),
                "name".to_string(),
                "first".to_string()
            ])
            .to_sql()
        );

        let id_expr: Box<Expr> = Box::new(Expr::Identifier("id".to_string()));
        assert_eq!("id IS NULL", Expr::IsNull(id_expr).to_sql());

        let id_expr: Box<Expr> = Box::new(Expr::Identifier("id".to_string()));
        assert_eq!("id IS NOT NULL", Expr::IsNotNull(id_expr).to_sql());

        assert_eq!(
            "CAST(1.0 AS INT)",
            Expr::Cast {
                expr: Box::new(Expr::Literal(AstLiteral::Number(
                    BigDecimal::from_str("1.0").unwrap()
                ))),
                data_type: DataType::Int
            }
            .to_sql()
        );

        assert_eq!(
            r#"INT("1")"#,
            Expr::TypedString {
                data_type: DataType::Int,
                value: "1".to_string()
            }
            .to_sql()
        );

        assert_eq!(
            r#"EXTRACT(MINUTE FROM "2022-05-05 01:02:03")"#,
            Expr::Extract {
                field: DateTimeField::Minute,
                expr: Box::new(Expr::Identifier("2022-05-05 01:02:03".to_string()))
            }
            .to_sql()
        );

        assert_eq!(
            "(id)",
            Expr::Nested(Box::new(Expr::Identifier("id".to_owned()))).to_sql(),
        );

        assert_eq!(
            "id BETWEEN low AND high",
            Expr::Between {
                expr: Box::new(Expr::Identifier("id".to_string())),
                negated: false,
                low: Box::new(Expr::Identifier("low".to_string())),
                high: Box::new(Expr::Identifier("high".to_string()))
            }
            .to_sql()
        );

        assert_eq!(
            "id NOT BETWEEN low AND high",
            Expr::Between {
                expr: Box::new(Expr::Identifier("id".to_string())),
                negated: true,
                low: Box::new(Expr::Identifier("low".to_string())),
                high: Box::new(Expr::Identifier("high".to_string()))
            }
            .to_sql()
        );

        assert_eq!(
            r#"id IN ("a", "b", "c")"#,
            Expr::InList {
                expr: Box::new(Expr::Identifier("id".to_string())),
                list: vec![
                    Expr::Literal(AstLiteral::QuotedString("a".to_string())),
                    Expr::Literal(AstLiteral::QuotedString("b".to_string())),
                    Expr::Literal(AstLiteral::QuotedString("c".to_string()))
                ],
                negated: false
            }
            .to_sql()
        );

        assert_eq!(
            r#"id NOT IN ("a", "b", "c")"#,
            Expr::InList {
                expr: Box::new(Expr::Identifier("id".to_string())),
                list: vec![
                    Expr::Literal(AstLiteral::QuotedString("a".to_string())),
                    Expr::Literal(AstLiteral::QuotedString("b".to_string())),
                    Expr::Literal(AstLiteral::QuotedString("c".to_string()))
                ],
                negated: true
            }
            .to_sql()
        );

        assert_eq!(
            trim(
                r#"                                                                           
                CASE id
                  WHEN 1 THEN "a"
                  WHEN 2 THEN "b"
                  ELSE "c"
                END
                "#
            ),
            Expr::Case {
                operand: Some(Box::new(Expr::Identifier("id".to_string()))),
                when_then: vec![
                    (
                        Expr::Literal(AstLiteral::Number(BigDecimal::from_str("1").unwrap())),
                        Expr::Literal(AstLiteral::QuotedString("a".to_string()))
                    ),
                    (
                        Expr::Literal(AstLiteral::Number(BigDecimal::from_str("2").unwrap())),
                        Expr::Literal(AstLiteral::QuotedString("b".to_string()))
                    )
                ],
                else_result: Some(Box::new(Expr::Literal(AstLiteral::QuotedString(
                    "c".to_string()
                ))))
            }
            .to_sql()
        );

        assert_eq!(
            "SIGN(..)",
            &Expr::Function(Box::new(Function::Sign(Expr::Literal(AstLiteral::Number(
                BigDecimal::from_str("1.0").unwrap()
            )))))
            .to_sql()
        );

        assert_eq!(
            "MAX(id)",
            Expr::Aggregate(Box::new(Aggregate::Max(Expr::Identifier("id".to_string())))).to_sql()
        );

        assert_eq!(
            "COUNT(*)",
            Expr::Aggregate(Box::new(Aggregate::Count(CountArgExpr::Wildcard))).to_sql()
        );

        assert_eq!(
            "MIN(id)",
            Expr::Aggregate(Box::new(Aggregate::Min(Expr::Identifier("id".to_string())))).to_sql()
        );

        assert_eq!(
            "SUM(price)",
            &Expr::Aggregate(Box::new(Aggregate::Sum(Expr::Identifier(
                "price".to_string()
            ))))
            .to_sql()
        );

        assert_eq!(
            "AVG(pay)",
            &Expr::Aggregate(Box::new(Aggregate::Avg(Expr::Identifier(
                "pay".to_string()
            ))))
            .to_sql()
        );
        assert_eq!(
            "VARIANCE(pay)",
            &Expr::Aggregate(Box::new(Aggregate::Variance(Expr::Identifier(
                "pay".to_string()
            ))))
            .to_sql()
        );
        assert_eq!(
            "STDEV(total)",
            &Expr::Aggregate(Box::new(Aggregate::Stdev(Expr::Identifier(
                "total".to_string()
            ))))
            .to_sql()
        );
    }
}
