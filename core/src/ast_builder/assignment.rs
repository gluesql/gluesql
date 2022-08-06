use crate::{
    ast::Assignment,
    parse_sql::parse_sql_assignment,
    result::{Error, Result},
    translate::translate_assignment,
};

#[derive(Clone)]
pub enum AssignmentNode {
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
                let expr = parse_sql_assignment(expr).and_then(|op| translate_assignment(&op))?;
                Ok(expr)
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
        let parsed = &parse_sql_assignment(expected).unwrap();
        let expected = translate_assignment(parsed);
        assert_eq!(actual.try_into(), expected);
    }

    #[test]
    fn assignment() {
        let actual = AssignmentNode::Text("foo = 1".into());
        let expected = "foo = 1";
        test(actual, expected);

        let actual = AssignmentNode::Text("foo = \"cocoa\"".into());
        let expected = "foo = \"cocoa\"";
        test(actual, expected);
    }
}
