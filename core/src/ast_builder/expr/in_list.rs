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
    Query(Box<QueryNode>),
    Text(String),
}

impl From<Vec<ExprNode>> for InListNode {
    fn from(list: Vec<ExprNode>) -> Self {
        InListNode::InList(list)
    }
}

impl From<SelectNode> for InListNode {
    fn from(node: SelectNode) -> Self {
        let node = Box::new(node.into());
        InListNode::Query(node)
    }
}

impl From<GroupByNode> for InListNode {
    fn from(node: GroupByNode) -> Self {
        let node = Box::new(node.into());
        InListNode::Query(node)
    }
}

impl From<HavingNode> for InListNode {
    fn from(node: HavingNode) -> Self {
        let node = Box::new(node.into());
        InListNode::Query(node)
    }
}

impl From<LimitNode> for InListNode {
    fn from(node: LimitNode) -> Self {
        let node = Box::new(node.into());
        InListNode::Query(node)
    }
}

impl From<LimitOffsetNode> for InListNode {
    fn from(node: LimitOffsetNode) -> Self {
        let node = Box::new(node.into());
        InListNode::Query(node)
    }
}

impl From<OffsetNode> for InListNode {
    fn from(node: OffsetNode) -> Self {
        let node = Box::new(node.into());
        InListNode::Query(node)
    }
}

impl From<OffsetLimitNode> for InListNode {
    fn from(node: OffsetLimitNode) -> Self {
        let node = Box::new(node.into());
        InListNode::Query(node)
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

        let actual = col("id").in_list(table("FOO").select());
        let expected = "id IN (SELECT * FROM FOO)";
        test_expr(actual, expected);

        let actual = col("id").not_in_list(table("FOO").select());
        let expected = "id NOT IN (SELECT * FROM FOO)";
        test_expr(actual, expected);

        let actual = col("id").in_list(
            table("Bar")
                .select()
                .filter(col("id").is_null())
                .group_by("id, (a + name)"),
        );
        let expected = "id IN (SELECT * FROM Bar WHERE id IS NULL GROUP BY id, (a + name))";
        test_expr(actual, expected);

        let actual = col("id").not_in_list(
            table("Bar")
                .select()
                .filter(col("id").is_null())
                .group_by("id, (a + name)"),
        );
        let expected = "id NOT IN (SELECT * FROM Bar WHERE id IS NULL GROUP BY id, (a + name))";
        test_expr(actual, expected);

        let actual = col("id").in_list(
            table("Bar")
                .select()
                .filter("id IS NULL")
                .group_by("id, (a + name)")
                .having("COUNT(id) > 10"),
        );
        let expected = "id IN (SELECT * FROM Bar WHERE id IS NULL GROUP BY id, (a + name) HAVING COUNT(id) > 10)";
        test_expr(actual, expected);

        let actual = col("id").not_in_list(
            table("Bar")
                .select()
                .filter("id IS NULL")
                .group_by("id, (a + name)")
                .having("COUNT(id) > 10"),
        );
        let expected = "id NOT IN (SELECT * FROM Bar WHERE id IS NULL GROUP BY id, (a + name) HAVING COUNT(id) > 10)";
        test_expr(actual, expected);

        let actual = col("id").in_list(
            table("World")
                .select()
                .filter("id > 2")
                .limit(100)
                .offset(3),
        );
        let expected = "id IN (SELECT * FROM World WHERE id > 2 OFFSET 3 LIMIT 100)";
        test_expr(actual, expected);

        let actual = col("id").not_in_list(
            table("World")
                .select()
                .filter("id > 2")
                .limit(100)
                .offset(3),
        );
        let expected = "id NOT IN (SELECT * FROM World WHERE id > 2 OFFSET 3 LIMIT 100)";
        test_expr(actual, expected);

        let actual = col("id").in_list(table("Hello").select().offset(10));
        let expected = "id IN (SELECT * FROM Hello OFFSET 10)";
        test_expr(actual, expected);

        let actual = col("id").not_in_list(table("Hello").select().offset(10));
        let expected = "id NOT IN (SELECT * FROM Hello OFFSET 10)";
        test_expr(actual, expected);

        let actual = col("id").in_list(
            table("Bar")
                .select()
                .group_by("city")
                .having("COUNT(name) < 100")
                .offset(1)
                .limit(3),
        );
        let expected =
            "id IN (SELECT * FROM Bar GROUP BY city HAVING COUNT(name) < 100 OFFSET 1 LIMIT 3)";
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
