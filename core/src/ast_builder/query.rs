use {
    super::{
        GroupByNode, HavingNode, LimitNode, LimitOffsetNode, OffsetLimitNode, OffsetNode,
        SelectNode, select::NodeData, select::Prebuild
    },
    crate::{
        ast::Query,
        parse_sql::parse_query,
        result::{Error, Result},
        translate::translate_query,
    },
};

#[derive(Clone)]
pub enum QueryNode {
    Select(SelectNode),
    GroupBy(GroupByNode),
    Having(HavingNode),
    Limit(LimitNode),
    LimitOffset(LimitOffsetNode),
    Offset(OffsetNode),
    OffsetLimit(OffsetLimitNode),
    Text(String),
}

impl From<SelectNode> for QueryNode {
    fn from(node: SelectNode) -> Self {
        QueryNode::Select(node)
    }
}

impl From<GroupByNode> for QueryNode {
    fn from(node: GroupByNode) -> Self {
        QueryNode::GroupBy(node)
    }
}

impl From<HavingNode> for QueryNode {
    fn from(node: HavingNode) -> Self {
        QueryNode::Having(node)
    }
}

impl From<LimitNode> for QueryNode {
    fn from(node: LimitNode) -> Self {
        QueryNode::Limit(node)
    }
}

impl From<LimitOffsetNode> for QueryNode {
    fn from(node: LimitOffsetNode) -> Self {
        QueryNode::LimitOffset(node)
    }
}

impl From<OffsetNode> for QueryNode {
    fn from(node: OffsetNode) -> Self {
        QueryNode::Offset(node)
    }
}

impl From<OffsetLimitNode> for QueryNode {
    fn from(node: OffsetLimitNode) -> Self {
        QueryNode::OffsetLimit(node)
    }
}

impl From<&str> for QueryNode {
    fn from(query: &str) -> Self {
        Self::Text(query.to_owned())
    }
}

impl TryFrom<QueryNode> for Query {
    type Error = Error;

    fn try_from(query_node: QueryNode) -> Result<Self> {
        match query_node {
            QueryNode::Select(query_node) => query_node.prebuild().map(NodeData::build_query),
            QueryNode::GroupBy(query_node) => query_node.prebuild().map(NodeData::build_query),
            QueryNode::Having(query_node) => query_node.prebuild().map(NodeData::build_query),
            QueryNode::Limit(query_node) => query_node.prebuild().map(NodeData::build_query),
            QueryNode::LimitOffset(query_node) => query_node.prebuild().map(NodeData::build_query),
            QueryNode::Offset(query_node) => query_node.prebuild().map(NodeData::build_query),
            QueryNode::OffsetLimit(query_node) => query_node.prebuild().map(NodeData::build_query),
            QueryNode::Text(query_node) => {
                parse_query(query_node).and_then(|item| translate_query(&item))
            }
        }
    }
}

#[cfg(test)]
mod test {

    use {
        super::QueryNode,
        crate::ast_builder::{table, test_query}
    };

    #[test]
    fn query() {
        let actual = QueryNode::Select(table("FOO").select());
        let expected = "SELECT * FROM FOO";
        test_query(actual, expected);

        let actual = QueryNode::GroupBy(table("FOO").select().group_by("id"));
        let expected = "SELECT * FROM FOO GROUP BY id";
        test_query(actual, expected);

        let actual = QueryNode::Having(table("FOO").select().group_by("id").having("COUNT(id) > 10"));
        let expected = "SELECT * FROM FOO GROUP BY id HAVING COUNT(id) > 10";
        test_query(actual, expected);

        let actual = QueryNode::Limit(table("FOO").select().group_by("city").having("COUNT(name) < 100").limit(3));
        let expected = "SELECT * FROM FOO GROUP BY city HAVING COUNT(name) < 100 LIMIT 3";
        test_query(actual, expected);

        let actual = QueryNode::LimitOffset(table("FOO").select().filter("id > 2").limit(100).offset(3));
        let expected = "SELECT * FROM FOO WHERE id > 2 OFFSET 3 LIMIT 100";
        test_query(actual, expected);

        let actual = QueryNode::Offset(table("FOO").select().offset(10));
        let expected = "SELECT * FROM FOO OFFSET 10";
        test_query(actual, expected);

        let actual = QueryNode::OffsetLimit(table("FOO").select().group_by("city").having("COUNT(name) < 100").offset(1).limit(3));
        let expected = "SELECT * FROM FOO GROUP BY city HAVING COUNT(name) < 100 OFFSET 1 LIMIT 3";
        test_query(actual, expected);
    }
}