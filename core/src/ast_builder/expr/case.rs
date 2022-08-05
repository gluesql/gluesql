use super::ExprNode;

impl ExprNode {
    pub fn case(
        self,
        when_then: Vec<(ExprNode, ExprNode)>,
        else_result: Option<Box<ExprNode>>,
    ) -> Self {
        Self::Case {
            operand: Some(Box::new(self)),
            when_then,
            else_result,
        }
    }
}

pub fn case(
    operand: Option<Box<ExprNode>>,
    when_then: Vec<(ExprNode, ExprNode)>,
    else_result: Option<Box<ExprNode>>,
) -> ExprNode {
    ExprNode::Case {
        operand,
        when_then,
        else_result,
    }
}

#[cfg(test)]
mod tests {
    use regex::Regex;

    use crate::ast_builder::{case, col, num, test_expr, text};

    #[test]
    fn test_case() {
        let re = Regex::new(r"\n\s+").unwrap();
        let trim = |s: &str| re.replace_all(s.trim(), "\n").into_owned();

        let actual = col("id").case(vec![(num(1), text("a")), (num(2), text("b"))], None);
        let expected = trim(
            r#"                                                                           
            CASE id
              WHEN 1 THEN "a"
              WHEN 2 THEN "b"
            END
            "#,
        );
        test_expr(actual, expected.as_str());

        let actual = col("id").case(
            vec![(num(1), text("a")), (num(2), text("b"))],
            Some(text("c").into()),
        );
        let expected = trim(
            r#"                                                                           
            CASE id
              WHEN 1 THEN "a"
              WHEN 2 THEN "b"
              ELSE "c"
            END
            "#,
        );
        test_expr(actual, expected.as_str());

        let actual = case(
            None,
            vec![(col("City").is_null(), col("Country"))],
            Some(col("City").into()),
        );
        let expected = trim(
            r#"                                                                           
            CASE
                WHEN City IS NULL THEN Country
                ELSE City
            END
            "#,
        );
        test_expr(actual, expected.as_str());
    }
}
