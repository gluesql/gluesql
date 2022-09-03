use {
    super::ExprNode,
    crate::ast_builder::{
        GroupByNode, HavingNode, LimitNode, LimitOffsetNode, OffsetLimitNode, OffsetNode,
        QueryNode, SelectNode,
    },
};

#[derive(Clone)]
pub enum InListNode {
    InList(Vec<ExprNode>),
    Query(QueryNode),
    Text(String),
}

impl From<Vec<ExprNode>> for InListNode {
    fn from(list: Vec<ExprNode>) -> Self {
        InListNode::InList(list)
    }
}

impl From<SelectNode> for InListNode {
    fn from(node: SelectNode) -> Self {
        InListNode::Query(node.into())
    }
}

impl From<GroupByNode> for InListNode {
    fn from(node: GroupByNode) -> Self {
        InListNode::Query(node.into())
    }
}

impl From<HavingNode> for InListNode {
    fn from(node: HavingNode) -> Self {
        InListNode::Query(node.into())
    }
}

impl From<LimitNode> for InListNode {
    fn from(node: LimitNode) -> Self {
        InListNode::Query(node.into())
    }
}

impl From<LimitOffsetNode> for InListNode {
    fn from(node: LimitOffsetNode) -> Self {
        InListNode::Query(node.into())
    }
}

impl From<OffsetNode> for InListNode {
    fn from(node: OffsetNode) -> Self {
        InListNode::Query(node.into())
    }
}

impl From<OffsetLimitNode> for InListNode {
    fn from(node: OffsetLimitNode) -> Self {
        InListNode::Query(node.into())
    }
}

impl From<&str> for InListNode {
    fn from(query: &str) -> Self {
        InListNode::Text(query.to_owned())
    }
}

impl ExprNode {
    pub fn in_list<T: Into<InListNode>>(self, value: T) -> Self {
        Self::InList {
            expr: Box::new(self),
            list: Box::new(value.into()),
            negated: false,
        }
    }

    pub fn not_in_list<T: Into<InListNode>>(self, value: T) -> Self {
        Self::InList {
            expr: Box::new(self),
            list: Box::new(value.into()),
            negated: true,
        }
    }
}

#[cfg(test)]
mod test {

    use crate::ast_builder::{col, table, test_expr, text};

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

        let actual = col("id").in_list("a, b, c");
        let expected = "id IN (a, b, c)";
        test_expr(actual, expected);

        let actual = col("id").not_in_list("a, b, c");
        let expected = "id NOT IN (a, b, c)";
        test_expr(actual, expected);

        let actual = col("id").in_list(table("FOO").select().filter("id IS NULL").limit(10));
        let expected = "id IN (SELECT * FROM FOO WHERE id IS NULL LIMIT 10)";
        test_expr(actual, expected);

        let actual = col("id").not_in_list(table("FOO").select().filter("id IS NULL").limit(10));
        let expected = "id NOT IN (SELECT * FROM FOO WHERE id IS NULL LIMIT 10)";
        test_expr(actual, expected);

        let actual = col("id").in_list("SELECT id FROM FOO");
        let expected = "id IN (SELECT id FROM FOO)";
        test_expr(actual, expected);

        let actual = col("id").not_in_list("SELECT id FROM FOO");
        let expected = "id NOT IN (SELECT id FROM FOO)";
        test_expr(actual, expected);
    }
}
