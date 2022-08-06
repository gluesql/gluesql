use super::ExprNode;

impl ExprNode {
    pub fn in_list(self, list: Vec<ExprNode>) -> Self {
        Self::InList {
            expr: Box::new(self),
            list,
            negated: false,
        }
    }

    pub fn not_in_list(self, list: Vec<ExprNode>) -> Self {
        Self::InList {
            expr: Box::new(self),
            list,
            negated: true,
        }
    }
}

#[cfg(test)]
mod test {

    use crate::ast_builder::{col, test_expr, text};

    #[test]
    fn in_list() {
        let list_in = vec![text("a"), text("b"), text("c")];

        let actual = col("id").in_list(list_in);
        let expected = "id IN ('a', 'b', 'c')";
        test_expr(actual, expected);

        let list_not_in = vec![text("a"), text("b"), text("c")];

        let actual = col("id").not_in_list(list_not_in);
        let expected = "id NOT IN ('a', 'b', 'c')";
        test_expr(actual, expected);
    }
}
