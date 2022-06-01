use super::{Aggregate, AstLiteral, CountArgExpr, Expr};

pub fn decode(expr: &Expr) -> String {
    match expr {
        Expr::Identifier(s) => s.to_string(),
        Expr::BinaryOp { left, op, right } => {
            format!("{:} {:} {:}", decode(&*left), op, decode(&*right))
        }
        Expr::CompoundIdentifier(idents) => idents.join("."),
        Expr::IsNull(s) => format!("{:} IS NULL", decode(s)),
        Expr::IsNotNull(s) => format!("{:} IS NOT NULL", decode(s)),
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
                s += &decode(item);
            }

            match negated {
                true => format!("{:} NOT IN ({:})", decode(expr), s),
                false => format!("{:} IN ({:})", decode(expr), s),
            }
        }
        Expr::Between {
            expr,
            negated,
            low,
            high,
        } => match negated {
            true => format!(
                "{:} NOT BETWEEN {:} AND {:}",
                decode(&*expr),
                decode(&*low),
                decode(&*high)
            ),

            false => format!(
                "{:} BETWEEN {:} AND {:}",
                decode(&*expr),
                decode(&*low),
                decode(&*high)
            ),
        },
        Expr::UnaryOp { op, expr } => format!("{:}{:}", op, decode(&*expr)),
        Expr::Cast { expr, data_type } => {
            format!("CAST({:} AS {:})", decode(&*expr), data_type)
        }
        Expr::Extract { field, expr } => {
            format!("EXTRACT({:} FROM \"{:}\")", field, decode(&*expr))
        }
        Expr::Nested(expr) => format!("todo:Nested({:})", decode(&*expr)),
        Expr::Literal(s) => match s {
            AstLiteral::Boolean(b) => format!("{:}", b),
            AstLiteral::Number(d) => format!("{:}", d),
            AstLiteral::QuotedString(qs) => format!("\"{:}\"", qs),
            AstLiteral::HexString(hs) => format!("\"{:}\"", hs),
            AstLiteral::Null => "Null".to_string(),
            AstLiteral::Interval { .. } => "Interval not implemented yet..".to_string(),
        },
        Expr::TypedString { data_type, value } => format!("{:}(\"{:}\")", data_type, value),
        Expr::Case {
            operand,
            when_then,
            else_result,
        } => {
            let mut str = match operand {
                Some(s) => format!("CASE {:}", decode(s)),
                None => "CASE ".to_string(),
            };
            for (_when, _then) in when_then {
                str += format!("\nWHEN {:} THEN {:}", decode(_when), decode(_then)).as_str();
            }

            match else_result {
                Some(s) => str += format!("\nELSE {:}", decode(s)).as_str(),
                None => str += "", // no operation?
            };
            str + "\nEND"
        }
        Expr::Aggregate(a) => match &**a {
            Aggregate::Count(c) => match c {
                CountArgExpr::Expr(e) => format!("Count({:})", decode(e)),
                CountArgExpr::Wildcard => "Count(*)".to_string(),
            },
            Aggregate::Sum(e) => format!("Sum({:})", decode(e)),
            Aggregate::Max(e) => format!("Max({:})", decode(e)),
            Aggregate::Min(e) => format!("Min({:})", decode(e)),
            Aggregate::Avg(e) => format!("Avg({:})", decode(e)),
        },
        Expr::Function(f) => {
            format!("{:}(todo:args)", f)
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

#[cfg(test)]
mod tests {

    use crate::ast::{
        expr_decoder::decode, Aggregate, AstLiteral, BinaryOperator, CountArgExpr, DataType,
        DateTimeField, Expr, Function, UnaryOperator,
    };
    use bigdecimal::BigDecimal;
    use std::str::FromStr;

    #[test]
    fn basic_decoder() {
        assert_eq!(
            "id".to_string(),
            decode(&Expr::Identifier("id".to_string()))
        );

        assert_eq!(
            "id + num",
            decode(&Expr::BinaryOp {
                left: Box::new(Expr::Identifier("id".to_string())),
                op: BinaryOperator::Plus,
                right: Box::new(Expr::Identifier("num".to_string()))
            })
        );

        assert_eq!(
            "-id",
            decode(&Expr::UnaryOp {
                op: UnaryOperator::Minus,
                expr: Box::new(Expr::Identifier("id".to_string()))
            })
        );

        assert_eq!(
            "id.name.first",
            decode(&Expr::CompoundIdentifier(vec![
                "id".to_string(),
                "name".to_string(),
                "first".to_string()
            ]))
        );

        let id_expr: Box<Expr> = Box::new(Expr::Identifier("id".to_string()));
        assert_eq!("id IS NULL", decode(&Expr::IsNull(id_expr)));

        let id_expr: Box<Expr> = Box::new(Expr::Identifier("id".to_string()));
        assert_eq!("id IS NOT NULL", decode(&Expr::IsNotNull(id_expr)));

        assert_eq!(
            "CAST(1.0 AS INT)",
            decode(&Expr::Cast {
                expr: Box::new(Expr::Literal(AstLiteral::Number(
                    BigDecimal::from_str("1.0").unwrap()
                ))),
                data_type: DataType::Int
            })
        );

        assert_eq!(
            r#"INT("1")"#,
            decode(&Expr::TypedString {
                data_type: DataType::Int,
                value: "1".to_string()
            })
        );

        assert_eq!(
            r#"EXTRACT(MINUTE FROM "2022-05-05 01:02:03")"#,
            decode(&Expr::Extract {
                field: DateTimeField::Minute,
                expr: Box::new(Expr::Identifier("2022-05-05 01:02:03".to_string()))
            })
        );

        assert_eq!(
            "id BETWEEN low AND high",
            decode(&Expr::Between {
                expr: Box::new(Expr::Identifier("id".to_string())),
                negated: false,
                low: Box::new(Expr::Identifier("low".to_string())),
                high: Box::new(Expr::Identifier("high".to_string()))
            })
        );

        assert_eq!(
            "id NOT BETWEEN low AND high",
            decode(&Expr::Between {
                expr: Box::new(Expr::Identifier("id".to_string())),
                negated: true,
                low: Box::new(Expr::Identifier("low".to_string())),
                high: Box::new(Expr::Identifier("high".to_string()))
            })
        );

        assert_eq!(
            r#"id IN ("a","b","c")"#,
            decode(&Expr::InList {
                expr: Box::new(Expr::Identifier("id".to_string())),
                list: vec![
                    Expr::Literal(AstLiteral::QuotedString("a".to_string())),
                    Expr::Literal(AstLiteral::QuotedString("b".to_string())),
                    Expr::Literal(AstLiteral::QuotedString("c".to_string()))
                ],
                negated: false
            })
        );

        assert_eq!(
            r#"id NOT IN ("a","b","c")"#,
            decode(&Expr::InList {
                expr: Box::new(Expr::Identifier("id".to_string())),
                list: vec![
                    Expr::Literal(AstLiteral::QuotedString("a".to_string())),
                    Expr::Literal(AstLiteral::QuotedString("b".to_string())),
                    Expr::Literal(AstLiteral::QuotedString("c".to_string()))
                ],
                negated: true
            })
        );

        assert_eq!(
            "CASE id\nWHEN 1 THEN \"a\"\nWHEN 2 THEN \"b\"\nELSE \"c\"\nEND",
            decode(&Expr::Case {
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
            })
        );

        // todo..
        assert_eq!(
            "SIGN(todo:args)",
            decode(&Expr::Function(Box::new(Function::Sign(Expr::Literal(
                AstLiteral::Number(BigDecimal::from_str("1.0").unwrap())
            )))))
        );

        // aggregate max
        assert_eq!(
            "Max(id)",
            decode(&Expr::Aggregate(Box::new(Aggregate::Max(
                Expr::Identifier("id".to_string())
            ))))
        );

        //aggregate count
        assert_eq!(
            "Count(*)",
            decode(&Expr::Aggregate(Box::new(Aggregate::Count(
                CountArgExpr::Wildcard
            ))))
        );

        //aggregate min
        assert_eq!(
            "Min(id)",
            decode(&Expr::Aggregate(Box::new(Aggregate::Min(
                Expr::Identifier("id".to_string())
            ))))
        );

        //aggregate sum
        assert_eq!(
            "Sum(price)",
            decode(&Expr::Aggregate(Box::new(Aggregate::Sum(
                Expr::Identifier("price".to_string())
            ))))
        );

        //aggregate avg
        assert_eq!(
            "Avg(pay)",
            decode(&Expr::Aggregate(Box::new(Aggregate::Avg(
                Expr::Identifier("pay".to_string())
            ))))
        );
    }
}
