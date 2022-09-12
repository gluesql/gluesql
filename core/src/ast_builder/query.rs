use {
    super::{
        select::NodeData, select::Prebuild, ExprList, GroupByNode, HavingNode, LimitNode,
        LimitOffsetNode, OffsetLimitNode, OffsetNode, ProjectNode, SelectNode,
    },
    crate::{
        ast::{Expr, Query, SetExpr, Values},
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
    Values(Vec<ExprList>),
    Project(ProjectNode),
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

impl From<ProjectNode> for QueryNode {
    fn from(node: ProjectNode) -> Self {
        QueryNode::Project(node)
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
            QueryNode::Project(query_node) => query_node.prebuild().map(NodeData::build_query),
            QueryNode::Text(query_node) => {
                parse_query(query_node).and_then(|item| translate_query(&item))
            }
            QueryNode::Values(values) => {
                // let values: Vec<ExprList> = values;
                let values: Vec<Vec<Expr>> = values
                    .into_iter()
                    // .map(|expr_list: ExprList| expr_list.try_into()) // Vec<Expr>
                    .map(TryInto::try_into)
                    // .collect::<Result<Vec<Vec<Expr>>>>()?;
                    .collect::<Result<Vec<_>>>()?;

                Ok(Query {
                    body: SetExpr::Values(Values(values)),
                    order_by: Vec::new(),
                    limit: None,
                    offset: None,
                })
            }
        }
    }
}

#[cfg(test)]
mod test {

    use crate::ast_builder::{table, test_query};

    #[test]
    fn query() {
        let actual = table("FOO").select().into();
        let expected = "SELECT * FROM FOO";
        test_query(actual, expected);

        let actual = table("FOO").select().group_by("id").into();
        let expected = "SELECT * FROM FOO GROUP BY id";
        test_query(actual, expected);

        let actual = table("FOO")
            .select()
            .group_by("id")
            .having("COUNT(id) > 10")
            .into();
        let expected = "SELECT * FROM FOO GROUP BY id HAVING COUNT(id) > 10";
        test_query(actual, expected);

        let actual = table("FOO")
            .select()
            .group_by("city")
            .having("COUNT(name) < 100")
            .limit(3)
            .into();
        let expected = "SELECT * FROM FOO GROUP BY city HAVING COUNT(name) < 100 LIMIT 3";
        test_query(actual, expected);

        let actual = table("FOO")
            .select()
            .filter("id > 2")
            .limit(100)
            .offset(3)
            .into();
        let expected = "SELECT * FROM FOO WHERE id > 2 OFFSET 3 LIMIT 100";
        test_query(actual, expected);

        let actual = table("FOO").select().offset(10).into();
        let expected = "SELECT * FROM FOO OFFSET 10";
        test_query(actual, expected);

        let actual = table("FOO")
            .select()
            .group_by("city")
            .having("COUNT(name) < 100")
            .offset(1)
            .limit(3)
            .into();
        let expected = "SELECT * FROM FOO GROUP BY city HAVING COUNT(name) < 100 OFFSET 1 LIMIT 3";
        test_query(actual, expected);

        let actual = table("FOO").select().limit(10).project("id, name").into();
        let expected = r#"SELECT id, name FROM FOO LIMIT 10"#;
        test_query(actual, expected);
    }
}
