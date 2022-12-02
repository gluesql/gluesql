use super::ExprNode;

impl<'a> ExprNode<'a> {
    pub fn case(self) -> CaseNode<'a> {
        CaseNode {
            operand: Some(Box::new(self)),
        }
    }
}

pub fn case() -> CaseNode<'static> {
    CaseNode { operand: None }
}

#[derive(Clone, Debug)]
pub struct CaseNode<'a> {
    operand: Option<Box<ExprNode<'a>>>,
}

impl<'a> CaseNode<'a> {
    pub fn when_then<W: Into<ExprNode<'a>>, T: Into<ExprNode<'a>>>(
        self,
        when: W,
        then: T,
    ) -> WhenThenNode<'a> {
        WhenThenNode {
            prev_node: self,
            when_then: vec![(when.into(), then.into())],
        }
    }
}

#[derive(Clone, Debug)]
pub struct WhenThenNode<'a> {
    prev_node: CaseNode<'a>,
    when_then: Vec<(ExprNode<'a>, ExprNode<'a>)>,
}

impl<'a> WhenThenNode<'a> {
    pub fn when_then<W: Into<ExprNode<'a>>, T: Into<ExprNode<'a>>>(
        mut self,
        when: W,
        then: T,
    ) -> Self {
        self.when_then.push((when.into(), then.into()));
        self
    }

    pub fn or_else<T: Into<ExprNode<'a>>>(self, else_result: T) -> ExprNode<'a> {
        ExprNode::Case {
            operand: self.prev_node.operand,
            when_then: self.when_then,
            else_result: Some(Box::new(else_result.into())),
        }
    }

    pub fn end(self) -> ExprNode<'a> {
        ExprNode::Case {
            operand: self.prev_node.operand,
            when_then: self.when_then,
            else_result: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::ast_builder::{case, col, num, test_expr, text};

    #[test]
    fn case_with_operand() {
        let actual = col("id")
            .case()
            .when_then(num(1), text("a"))
            .when_then(2, text("b"))
            .or_else(text("c"));
        let expected = "
            CASE id
              WHEN 1 THEN 'a'
              WHEN 2 THEN 'b'
              ELSE 'c'
            END
            ";
        test_expr(actual, expected);

        let actual = col("id")
            .gt(10)
            .case()
            .when_then(true, text("a"))
            .when_then(false, text("b"))
            .end();
        let expected = "
            CASE id > 10
              WHEN True THEN 'a'
              WHEN False THEN 'b'
            END
            ";
        test_expr(actual, expected);
    }

    #[test]
    fn case_without_operand() {
        let actual = case()
            .when_then(
                "City IS NULL",
                case()
                    .when_then("Country IS NULL", text("weird"))
                    .or_else("Country"),
            )
            .or_else("City");
        let expected = r#"
            CASE
              WHEN City IS NULL THEN CASE WHEN Country IS NULL THEN 'weird'
                                          ELSE Country
                                          END
              ELSE City
            END
            "#;
        test_expr(actual, expected);
    }
}
