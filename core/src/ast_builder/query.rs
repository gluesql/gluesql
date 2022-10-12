use {
    super::{
        select::NodeData, select::Prebuild, ExprList, FilterNode, GroupByNode, HashJoinNode,
        HavingNode, JoinConstraintNode, JoinNode, LimitNode, LimitOffsetNode, OffsetLimitNode,
        OffsetNode, ProjectNode, SelectNode,
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
    Join(JoinNode),
    JoinConstraint(JoinConstraintNode),
    HashJoin(HashJoinNode),
    GroupBy(GroupByNode),
    Having(HavingNode),
    Limit(LimitNode),
    LimitOffset(LimitOffsetNode),
    Offset(OffsetNode),
    OffsetLimit(OffsetLimitNode),
    Filter(FilterNode),
    Text(String),
    Values(Vec<ExprList>),
    Project(ProjectNode),
}

impl From<SelectNode> for QueryNode {
    fn from(node: SelectNode) -> Self {
        QueryNode::Select(node)
    }
}

impl From<JoinNode> for QueryNode {
    fn from(node: JoinNode) -> Self {
        QueryNode::Join(node)
    }
}

impl From<JoinConstraintNode> for QueryNode {
    fn from(node: JoinConstraintNode) -> Self {
        QueryNode::JoinConstraint(node)
    }
}

impl From<HashJoinNode> for QueryNode {
    fn from(node: HashJoinNode) -> Self {
        QueryNode::HashJoin(node)
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

impl From<FilterNode> for QueryNode {
    fn from(node: FilterNode) -> Self {
        QueryNode::Filter(node)
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
            QueryNode::Join(query_node) => query_node.prebuild().map(NodeData::build_query),
            QueryNode::JoinConstraint(query_node) => {
                query_node.prebuild().map(NodeData::build_query)
            }
            QueryNode::HashJoin(query_node) => query_node.prebuild().map(NodeData::build_query),
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
                let values: Vec<Vec<Expr>> = values
                    .into_iter()
                    .map(TryInto::try_into)
                    .collect::<Result<Vec<_>>>()?;

                Ok(Query {
                    body: SetExpr::Values(Values(values)),
                    order_by: Vec::new(),
                    limit: None,
                    offset: None,
                })
            }
            QueryNode::Filter(query_node) => query_node.prebuild().map(NodeData::build_query),
        }
    }
}

#[cfg(test)]
mod test {
    use {
        super::QueryNode,
        crate::{
            ast::{
                Join, JoinConstraint, JoinExecutor, JoinOperator, Query, Select, SetExpr,
                TableFactor, TableWithJoins,
            },
            ast_builder::{col, table, test_query, SelectItemList},
        },
    };

    #[test]
    fn query() {
        let actual = table("FOO").select().into();
        let expected = "SELECT * FROM FOO";
        test_query(actual, expected);

        let actual = table("Bar").select().join("Foo").into();
        let expected = "SELECT * FROM Bar JOIN Foo";
        test_query(actual, expected);

        let actual = table("Bar")
            .select()
            .join("Foo")
            .on("Foo.id = Bar.foo_id")
            .into();
        let expected = "SELECT * FROM Bar JOIN Foo ON Foo.id = Bar.foo_id";
        test_query(actual, expected);

        let actual: QueryNode = table("Player")
            .select()
            .join("PlayerItem")
            .hash_executor("PlayerItem.user_id", "Player.id")
            .into();
        let expected = {
            let join = Join {
                relation: TableFactor::Table {
                    name: "PlayerItem".to_owned(),
                    alias: None,
                    index: None,
                },
                join_operator: JoinOperator::Inner(JoinConstraint::None),
                join_executor: JoinExecutor::Hash {
                    key_expr: col("PlayerItem.user_id").try_into().unwrap(),
                    value_expr: col("Player.id").try_into().unwrap(),
                    where_clause: None,
                },
            };
            let select = Select {
                projection: SelectItemList::from("*").try_into().unwrap(),
                from: TableWithJoins {
                    relation: TableFactor::Table {
                        name: "Player".to_owned(),
                        alias: None,
                        index: None,
                    },
                    joins: vec![join],
                },
                selection: None,
                group_by: Vec::new(),
                having: None,
            };

            Query {
                body: SetExpr::Select(Box::new(select)),
                order_by: Vec::new(),
                limit: None,
                offset: None,
            }
        };
        assert_eq!(Query::try_from(actual).unwrap(), expected);

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
