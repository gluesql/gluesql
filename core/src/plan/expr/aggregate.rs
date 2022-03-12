use crate::ast::{Aggregate, Expr};

impl Aggregate {
    pub fn as_expr(&self) -> &Expr {
        match self {
            Aggregate::Count(expr)
            | Aggregate::Sum(expr)
            | Aggregate::Max(expr)
            | Aggregate::Min(expr)
            | Aggregate::Avg(expr) => expr,
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
        let actual = parse("COUNT(id)");
        let expected = Expr::Identifier("id".to_owned());
        assert_eq!(actual.as_expr(), &expected);

        let actual = parse("SUM(id)");
        let expected = Expr::Identifier("id".to_owned());
        assert_eq!(actual.as_expr(), &expected);

        let actual = parse("MAX(id)");
        let expected = Expr::Identifier("id".to_owned());
        assert_eq!(actual.as_expr(), &expected);

        let actual = parse("MIN(id)");
        let expected = Expr::Identifier("id".to_owned());
        assert_eq!(actual.as_expr(), &expected);

        let actual = parse("AVG(id)");
        let expected = Expr::Identifier("id".to_owned());
        assert_eq!(actual.as_expr(), &expected);
    }
}
