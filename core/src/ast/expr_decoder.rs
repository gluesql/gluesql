use super::Expr;

//pub enum DecoderErrors {
//    #[error("unimplemented Decode Expression: {:}")]
//   UnimplementedDecoded(String)
//}

//decode a list of exprs to something more readable.

//fn unbox<T>(value: Box<T>) -> T {
//    *value
//}

fn decode(e: Expr) -> String {
    match e {
        Expr::Identifier(s) => s.to_string(),
        Expr::BinaryOp { left, op, right } => {
            format!("{:} {:} {:}", decode(*left), op.to_string(), decode(*right))
        }

        Expr::CompoundIdentifier(s) => s.iter().fold("".to_string(), |acc, x| acc + x),
        Expr::IsNull(s) => format!("isNull({:})", *s),
        Expr::IsNotNull(s) => format!("isNotNull({:})", *s),
        Expr::InList {
            expr,
            list,
            negated,
        } => format!(
            "InList({:}, {:}, negated:{:})",
            decode(*expr),
            (*list).iter().fold("".to_string(), |acc, x| acc + decode(*x)),
            negated
        ),
        //   Expr::InSubquery {expr, subquery, negated} => format!("InSubquery({:}, subquery:{:}, negated: {:})", decode(*expr),  *subquery, negated),
        Expr::Between {
            expr,
            negated,
            low,
            high,
        } => format!(
            "Between ({:}, negated:{:}, low:{:}, high:{:})",
            decode(*expr),
            negated,
            decode(*low),
            decode(*high)
        ),
        Expr::UnaryOp { op, expr } => format!("{:} {:}", op.to_string(), decode(*expr)),
        Expr::Cast { expr, data_type } => {
            format!("cast({:} as {:}", decode(*expr), data_type.to_string())
        }
        Expr::Extract { field, expr } => {
            format!("extract({:} from {:}", field.to_string(), decode(*expr))
        }
        Expr::Nested(expr) => format!("Nested({:})", decode(*expr)),
        //    Expr::Literal(s) => format!("{:}", s),
        Expr::TypedString { data_type, value } => format!("{:}({:})", data_type.to_string(), value),
        // Expr::Function(f) => format!("{:}", *f.to_string()),
        //  Expr::Aggregate(a) => format!("{}", *a.to_string()),
        //   Expr::Exists(q) => format!("Exists({:})", *query),
        //   Expr::Subquery(q) => format!("Subquery({:})", *query),
        /*
             Expr::Case { operand, when_then, else_result } => format!("case(operand:{:}, when_then:{:}, else_result:{:}", )
             Case {
            operand: Option<Box<Expr>>,
            when_then: Vec<(Expr, Expr)>,
            else_result: Option<Box<Expr>>,
        },
        */
        _ => format!("Unimplemented Decode Expression: {:}", e.to_string()),
    }
}

#[cfg(test)]
mod tests {

    use super::Expr;
    use crate::ast::expr_decoder::decode;
    use crate::ast::BinaryOperator;

    #[test]
    fn basic_decoder() {
        assert_eq!("id".to_string(), decode(Expr::Identifier("id".to_string())));

        assert_eq!(
            "id + num",
            decode(Expr::BinaryOp {
                left: Box::new(Expr::Identifier("id".to_string())),
                op: BinaryOperator::Plus,
                right: Box::new(Expr::Identifier("num".to_string()))
            })
        );
    }
}
