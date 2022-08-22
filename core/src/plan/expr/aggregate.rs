use crate::ast::{Aggregate, CountArgExpr, Expr};

impl Aggregate {
    pub fn as_expr(&self) -> Option<&Expr> {
        match self {
            Aggregate::Count(CountArgExpr::Wildcard) => None,
            Aggregate::Count(CountArgExpr::Expr(expr))
            | Aggregate::Sum(expr)
            | Aggregate::Max(expr)
            | Aggregate::Min(expr)
            | Aggregate::Avg(expr)
            | Aggregate::Variance(expr)
            | Aggregate::Stdev(expr) => Some(expr),
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
