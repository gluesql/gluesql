use {super::ExprNode, crate::ast_builder::OrderByExprNode};

impl<'a> ExprNode<'a> {
    #[must_use]
    pub fn asc(self) -> OrderByExprNode<'a> {
        OrderByExprNode::Expr {
            expr: self,
            asc: Some(true),
        }
    }

    #[must_use]
    pub fn desc(self) -> OrderByExprNode<'a> {
        OrderByExprNode::Expr {
            expr: self,
            asc: Some(false),
        }
    }
}

#[cfg(test)]
mod tests {
    use {
        crate::{
            ast_builder::{OrderByExprNode, col},
            parse_sql::parse_order_by_expr,
            translate::{NO_PARAMS, translate_order_by_expr},
        },
        pretty_assertions::assert_eq,
    };

    fn test(actual: OrderByExprNode, expected: &str) {
        let parsed = &parse_order_by_expr(expected).expect(expected);
        let expected = translate_order_by_expr(parsed, NO_PARAMS);
        assert_eq!(actual.try_into(), expected);
    }

    #[test]
    fn order_by() {
        let actual = col("foo").asc();
        let expected = "foo ASC";
        test(actual, expected);

        let actual = col("foo").desc();
        let expected = "foo DESC";
        test(actual, expected);
    }
}
