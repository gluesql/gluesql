use super::ExprNode;

impl ExprNode {
    pub fn between<T: Into<Self>>(self, negated: bool, low: T, high: T) -> Self {
        Self::Between {
            expr: Box::new(self),
            negated,
            low: Box::new(low.into()),
            high: Box::new(high.into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::ast_builder::{col, test_expr, text};

    #[test]
    fn between() {
        let actual = col("num").between(false, 1, 10);
        let expected = "num BETWEEN 1 AND 10";
        test_expr(actual, expected);

        let actual = col("date").between(false, text("2022-01-01"), text("2023-01-01"));
        let expected = "date BETWEEN '2022-01-01' AND '2023-01-01'";
        test_expr(actual, expected);

        let actual = col("num").between(true, 1, 10);
        let expected = "num NOT BETWEEN 1 AND 10";
        test_expr(actual, expected);

        let actual = col("date").between(true, text("2022-01-01"), text("2023-01-01"));
        let expected = "date NOT BETWEEN '2022-01-01' AND '2023-01-01'";
        test_expr(actual, expected);
    }
}
