use {super::ExprNode, crate::ast::DateTimeField};

impl ExprNode {
    pub fn extract(self, field: DateTimeField) -> Self {
        Self::Extract {
            field,
            expr: Box::new(self),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        ast::DateTimeField,
        ast_builder::{col, test_expr},
    };

    #[test]
    fn extract() {
        let actual = col("date").extract(DateTimeField::Year);
        let expected = "EXTRACT(YEAR FROM date)";
        test_expr(actual, expected);

        let actual = col("date").extract(DateTimeField::Month);
        let expected = "EXTRACT(MONTH FROM date)";
        test_expr(actual, expected);

        let actual = col("date").extract(DateTimeField::Day);
        let expected = "EXTRACT(DAY FROM date)";
        test_expr(actual, expected);

        let actual = col("date").extract(DateTimeField::Hour);
        let expected = "EXTRACT(HOUR FROM date)";
        test_expr(actual, expected);

        let actual = col("date").extract(DateTimeField::Minute);
        let expected = "EXTRACT(MINUTE FROM date)";
        test_expr(actual, expected);

        let actual = col("date").extract(DateTimeField::Second);
        let expected = "EXTRACT(SECOND FROM date)";
        test_expr(actual, expected);
    }
}
