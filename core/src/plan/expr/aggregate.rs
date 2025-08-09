use crate::ast::{Aggregate, CountArgExpr, Expr};

impl Aggregate {
    pub fn as_expr(&self) -> Option<&Expr> {
        match self {
            Aggregate::Count {
                expr: CountArgExpr::Wildcard,
                distinct: _,
            } => None,
            Aggregate::Count {
                expr: CountArgExpr::Expr(expr),
                distinct: _,
            }
            | Aggregate::Sum { expr, distinct: _ }
            | Aggregate::Max { expr, distinct: _ }
            | Aggregate::Min { expr, distinct: _ }
            | Aggregate::Avg { expr, distinct: _ }
            | Aggregate::Variance { expr, distinct: _ }
            | Aggregate::Stdev { expr, distinct: _ } => Some(expr),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        ast::{Aggregate, Expr},
        parse_sql::parse_expr,
        translate::translate_expr,
    };

    fn parse(sql: &str) -> Aggregate {
        let parsed = parse_expr(sql).unwrap();
        let expr = translate_expr(&parsed).unwrap();

        match expr {
            Expr::Aggregate(aggregate) => *aggregate,
            _ => unreachable!("only for aggregate tests"),
        }
    }

    #[test]
    fn as_expr() {
        assert_eq!(parse("COUNT(*)").as_expr(), None);

        let actual = parse("COUNT(id)");
        let expected = Expr::Identifier("id".to_owned());
        assert_eq!(actual.as_expr(), Some(&expected));

        let actual = parse("SUM(id)");
        let expected = Expr::Identifier("id".to_owned());
        assert_eq!(actual.as_expr(), Some(&expected));

        let actual = parse("MAX(id)");
        let expected = Expr::Identifier("id".to_owned());
        assert_eq!(actual.as_expr(), Some(&expected));

        let actual = parse("MIN(id)");
        let expected = Expr::Identifier("id".to_owned());
        assert_eq!(actual.as_expr(), Some(&expected));

        let actual = parse("AVG(id)");
        let expected = Expr::Identifier("id".to_owned());
        assert_eq!(actual.as_expr(), Some(&expected));

        let actual = parse("VARIANCE(id)");
        let expected = Expr::Identifier("id".to_owned());
        assert_eq!(actual.as_expr(), Some(&expected));
    }
}
