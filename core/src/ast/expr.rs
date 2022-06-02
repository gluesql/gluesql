use {
    super::{
        Aggregate, AstLiteral, BinaryOperator, CountArgExpr, DataType, DateTimeField, Function,
        Query, ToSql, UnaryOperator,
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
                format!("{} {} {}", &*left.to_sql(), op.to_sql(), &*right.to_sql())
            }
            Expr::CompoundIdentifier(idents) => idents.join("."),
            Expr::IsNull(s) => format!("{} IS NULL", s.to_sql()),
            Expr::IsNotNull(s) => format!("{} IS NOT NULL", s.to_sql()),
            Expr::InList {
                expr,
                list,
                negated,
            } => {
                let mut s: String = "".to_string();

                for item in list {
                    if !s.is_empty() {
                        s += ",";
                    }
                    s += &item.to_sql();
                }

                match negated {
                    true => format!("{} NOT IN ({})", expr.to_sql(), s),
                    false => format!("{} IN ({})", expr.to_sql(), s),
                }
            }
            Expr::Between {
                expr,
                negated,
                low,
                high,
            } => match negated {
                true => format!(
                    "{} NOT BETWEEN {} AND {}",
                    &*expr.to_sql(),
                    &*low.to_sql(),
                    &*high.to_sql()
                ),

                false => format!(
                    "{} BETWEEN {} AND {}",
                    &*expr.to_sql(),
                    &*low.to_sql(),
                    &*high.to_sql()
                ),
            },
            Expr::UnaryOp { op, expr } => format!("{}{}", op.to_sql(), &*expr.to_sql()),
            Expr::Cast { expr, data_type } => {
                format!("CAST({} AS {})", &*expr.to_sql(), data_type)
            }
            Expr::Extract { field, expr } => {
                format!("EXTRACT({} FROM \"{}\")", field, &*expr.to_sql())
            }
            Expr::Nested(expr) => format!("todo:Nested({})", &*expr.to_sql()),
            Expr::Literal(s) => match s {
                AstLiteral::Boolean(b) => format!("{}", b),
                AstLiteral::Number(d) => format!("{}", d),
                AstLiteral::QuotedString(qs) => format!("\"{}\"", qs),
                AstLiteral::HexString(hs) => format!("\"{}\"", hs),
                AstLiteral::Null => "Null".to_string(),
                AstLiteral::Interval { .. } => "Interval not implemented yet..".to_string(),
            },
            Expr::TypedString { data_type, value } => format!("{}(\"{}\")", data_type, value),
            Expr::Case {
                operand,
                when_then,
                else_result,
            } => {
                let mut str = match operand {
                    Some(s) => format!("CASE {}", s.to_sql()),
                    None => "CASE ".to_string(),
                };
                for (_when, _then) in when_then {
                    str += format!("\nWHEN {} THEN {}", _when.to_sql(), _then.to_sql()).as_str();
                }

                match else_result {
                    Some(s) => str += format!("\nELSE {}", s.to_sql()).as_str(),
                    None => str += "", // no operation?
                };
                str + "\nEND"
            }
            Expr::Aggregate(a) => match &**a {
                Aggregate::Count(c) => match c {
                    CountArgExpr::Expr(e) => format!("Count({})", e.to_sql()),
                    CountArgExpr::Wildcard => "Count(*)".to_string(),
                },
                Aggregate::Sum(e) => format!("Sum({})", e.to_sql()),
                Aggregate::Max(e) => format!("Max({})", e.to_sql()),
                Aggregate::Min(e) => format!("Min({})", e.to_sql()),
                Aggregate::Avg(e) => format!("Avg({})", e.to_sql()),
            },
            Expr::Function(f) => {
                format!("{}(todo:args)", f)
            }
            // todo's...  these require enum query..
            Expr::InSubquery {
                expr: _,
                subquery: _,
                negated: _,
            } => "InSubquery(..)".to_string(),
            Expr::Exists(_q) => "Exists(..)".to_string(),
            Expr::Subquery(_q) => "Subquery(..)".to_string(),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::ast::{
        Aggregate, AstLiteral, BinaryOperator, CountArgExpr, DataType, DateTimeField, Expr,
        Function, ToSql, UnaryOperator,
    };
    use bigdecimal::BigDecimal;
    use std::str::FromStr;

    #[test]
    fn to_sql() {
        assert_eq!("id", &Expr::Identifier("id".to_string()).to_sql());

        assert_eq!(
            "id + num",
            &Expr::BinaryOp {
                left: Box::new(Expr::Identifier("id".to_string())),
                op: BinaryOperator::Plus,
                right: Box::new(Expr::Identifier("num".to_string()))
            }
            .to_sql()
        );

        assert_eq!(
            "-id",
            &Expr::UnaryOp {
                op: UnaryOperator::Minus,
                expr: Box::new(Expr::Identifier("id".to_string()))
            }
            .to_sql()
        );

        assert_eq!(
            "id.name.first",
            &Expr::CompoundIdentifier(vec![
                "id".to_string(),
                "name".to_string(),
                "first".to_string()
            ])
            .to_sql()
        );

        let id_expr: Box<Expr> = Box::new(Expr::Identifier("id".to_string()));
        assert_eq!("id IS NULL", &Expr::IsNull(id_expr).to_sql());

        let id_expr: Box<Expr> = Box::new(Expr::Identifier("id".to_string()));
        assert_eq!("id IS NOT NULL", &Expr::IsNotNull(id_expr).to_sql());

        assert_eq!(
            "CAST(1.0 AS INT)",
            &Expr::Cast {
                expr: Box::new(Expr::Literal(AstLiteral::Number(
                    BigDecimal::from_str("1.0").unwrap()
                ))),
                data_type: DataType::Int
            }
            .to_sql()
        );

        assert_eq!(
            r#"INT("1")"#,
            &Expr::TypedString {
                data_type: DataType::Int,
                value: "1".to_string()
            }
            .to_sql()
        );

        assert_eq!(
            r#"EXTRACT(MINUTE FROM "2022-05-05 01:02:03")"#,
            &Expr::Extract {
                field: DateTimeField::Minute,
                expr: Box::new(Expr::Identifier("2022-05-05 01:02:03".to_string()))
            }
            .to_sql()
        );

        assert_eq!(
            "id BETWEEN low AND high",
            &Expr::Between {
                expr: Box::new(Expr::Identifier("id".to_string())),
                negated: false,
                low: Box::new(Expr::Identifier("low".to_string())),
                high: Box::new(Expr::Identifier("high".to_string()))
            }
            .to_sql()
        );

        assert_eq!(
            "id NOT BETWEEN low AND high",
            &Expr::Between {
                expr: Box::new(Expr::Identifier("id".to_string())),
                negated: true,
                low: Box::new(Expr::Identifier("low".to_string())),
                high: Box::new(Expr::Identifier("high".to_string()))
            }
            .to_sql()
        );

        assert_eq!(
            r#"id IN ("a","b","c")"#,
            &Expr::InList {
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
            r#"id NOT IN ("a","b","c")"#,
            &Expr::InList {
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
            "CASE id\nWHEN 1 THEN \"a\"\nWHEN 2 THEN \"b\"\nELSE \"c\"\nEND",
            &Expr::Case {
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

        // todo..
        assert_eq!(
            "SIGN(todo:args)",
            &Expr::Function(Box::new(Function::Sign(Expr::Literal(AstLiteral::Number(
                BigDecimal::from_str("1.0").unwrap()
            )))))
            .to_sql()
        );

        assert_eq!(
            "Max(id)",
            &Expr::Aggregate(Box::new(Aggregate::Max(Expr::Identifier("id".to_string())))).to_sql()
        );

        assert_eq!(
            "Count(*)",
            &Expr::Aggregate(Box::new(Aggregate::Count(CountArgExpr::Wildcard))).to_sql()
        );

        assert_eq!(
            "Min(id)",
            &Expr::Aggregate(Box::new(Aggregate::Min(Expr::Identifier("id".to_string())))).to_sql()
        );

        assert_eq!(
            "Sum(price)",
            &Expr::Aggregate(Box::new(Aggregate::Sum(Expr::Identifier(
                "price".to_string()
            ))))
            .to_sql()
        );

        assert_eq!(
            "Avg(pay)",
            &Expr::Aggregate(Box::new(Aggregate::Avg(Expr::Identifier(
                "pay".to_string()
            ))))
            .to_sql()
        );
    }
}
