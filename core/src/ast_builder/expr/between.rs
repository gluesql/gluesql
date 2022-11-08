use super::ExprNode;

impl<'a> ExprNode<'a> {
    pub fn between<T: Into<Self>, U: Into<Self>>(self, low: T, high: U) -> Self {
        Self::Between {
            expr: Box::new(self),
            negated: false,
            low: Box::new(low.into()),
            high: Box::new(high.into()),
        }
    }

    pub fn not_between<T: Into<Self>, U: Into<Self>>(self, low: T, high: U) -> Self {
        Self::Between {
            expr: Box::new(self),
            negated: true,
            low: Box::new(low.into()),
            high: Box::new(high.into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::ast_builder::{col, num, test_expr, text};

    #[test]
    fn between() {
        let actual = col("num").between(1, 10);
        let expected = "num BETWEEN 1 AND 10";
        test_expr(actual, expected);

        let actual = col("date").between(text("2022-01-01"), "'2023-01-01'");
        let expected = "date BETWEEN '2022-01-01' AND '2023-01-01'";
        test_expr(actual, expected);

        let actual = col("num").not_between(num(1), 10);
        let expected = "num NOT BETWEEN 1 AND 10";
        test_expr(actual, expected);

        let actual = col("date").not_between(text("2022-01-01"), text("2023-01-01"));
        let expected = "date NOT BETWEEN '2022-01-01' AND '2023-01-01'";
        test_expr(actual, expected);
    }
}
