use super::ExprNode;
use crate::{
    ast::{Assignment, Expr},
    parse_sql::parse_sql_assignment,
    result::{Error, Result},
    translate::translate_assignment,
};

#[derive(Clone)]
pub enum AssignmentNode {
    Expr(String, ExprNode),
    Text(String),
}

impl From<&str> for AssignmentNode {
    fn from(expr: &str) -> Self {
        Self::Text(expr.to_owned())
    }
}

impl TryFrom<AssignmentNode> for Assignment {
    type Error = Error;

    fn try_from(node: AssignmentNode) -> Result<Self> {
        match node {
            AssignmentNode::Text(expr) => {
                let expr = parse_sql_assignment(expr)
                    .and_then(|assignment| translate_assignment(&assignment))?;
                Ok(expr)
            }
            AssignmentNode::Expr(col, expr_node) => {
                let value = Expr::try_from(expr_node)?;
                let id = col;
                Ok(Assignment { id, value })
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        ast_builder::AssignmentNode, parse_sql::parse_sql_assignment,
        translate::translate_assignment,
    };

    fn test(actual: AssignmentNode, expected: &str) {
        let parsed = &parse_sql_assignment(expected).expect(expected);
        let expected = translate_assignment(parsed);
        assert_eq!(actual.try_into(), expected);
    }

    #[test]
    fn assignment() {
        let actual = "foo = 1".into();
        let expected = "foo = 1";
        test(actual, expected);

        let actual = r#"foo = "choco""#.into();
        let expected = r#"foo = "choco""#;
        test(actual, expected);

        let actual = r#"Bar = mild"#.into();
        let expected = r#"Bar = mild"#;
        test(actual, expected);

        let actual = AssignmentNode::Expr("foo".into(), "1".into());
        let expected = "foo = 1";
        test(actual, expected);

        let actual = AssignmentNode::Expr("foo".into(), r#""cocoa""#.into());
        let expected = r#"foo = "cocoa""#;
        test(actual, expected);

        let actual = AssignmentNode::Expr("Bar".into(), "mild".into());
        let expected = "Bar = mild";
        test(actual, expected);
    }
}
