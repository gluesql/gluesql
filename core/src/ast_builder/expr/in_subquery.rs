use {super::ExprNode, crate::ast_builder::QueryNode};

impl ExprNode {
    pub fn in_subquery<T: Into<QueryNode>>(self, subquery: T) -> Self {
        Self::InSubquery {
            expr: Box::new(self),
            subquery: Box::new(subquery.into()),
            negated: false,
        }
    }

    pub fn not_in_subquery<T: Into<QueryNode>>(self, subquery: T) -> Self {
        Self::InSubquery {
            expr: Box::new(self),
            subquery: Box::new(subquery.into()),
            negated: true,
        }
    }
}

#[cfg(test)]
mod test {
    use crate::ast_builder::{col, table, test_expr};

    #[test]
    fn in_subquery() {
        let actual = col("id").in_subquery(table("FOO").select().filter("id IS NULL").limit(10));
        let expected = "id IN (SELECT * FROM FOO WHERE id IS NULL LIMIT 10)";
        test_expr(actual, expected);

        let actual = col("id").not_in_subquery(table("FOO").select().filter("id IS NULL").limit(10));
        let expected = "id NOT IN (SELECT * FROM FOO WHERE id IS NULL LIMIT 10)";
        test_expr(actual, expected);

        let actual = col("id").in_subquery("SELECT id FROM FOO");
        let expected = "id IN (SELECT id FROM FOO)";
        test_expr(actual, expected);

        let actual = col("id").not_in_subquery("SELECT id FROM FOO");
        let expected = "id NOT IN (SELECT id FROM FOO)";
        test_expr(actual, expected);
    }
}
