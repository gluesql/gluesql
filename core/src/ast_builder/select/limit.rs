use {
    super::{values::ValuesNode, Prebuild},
    crate::{
        ast::Query,
        ast_builder::{
            ExprNode, FilterNode, GroupByNode, HashJoinNode, HavingNode, JoinConstraintNode,
            JoinNode, OrderByNode, ProjectNode, QueryNode, SelectNode, TableFactorNode,
        },
        result::Result,
    },
};

#[derive(Clone, Debug)]
pub enum PrevNode<'a> {
    Select(SelectNode<'a>),
    Values(ValuesNode<'a>),
    GroupBy(GroupByNode<'a>),
    Having(HavingNode<'a>),
    Join(Box<JoinNode<'a>>),
    JoinConstraint(Box<JoinConstraintNode<'a>>),
    HashJoin(HashJoinNode<'a>),
    Filter(FilterNode<'a>),
    OrderBy(OrderByNode<'a>),
    ProjectNode(Box<ProjectNode<'a>>),
}

impl<'a> Prebuild<Query> for PrevNode<'a> {
    fn prebuild(self) -> Result<Query> {
        match self {
            Self::Select(node) => node.prebuild(),
            Self::Values(node) => node.prebuild(),
            Self::GroupBy(node) => node.prebuild(),
            Self::Having(node) => node.prebuild(),
            Self::Join(node) => node.prebuild(),
            Self::JoinConstraint(node) => node.prebuild(),
            Self::HashJoin(node) => node.prebuild(),
            Self::Filter(node) => node.prebuild(),
            Self::OrderBy(node) => node.prebuild(),
            Self::ProjectNode(node) => node.prebuild(),
        }
    }
}

impl<'a> From<SelectNode<'a>> for PrevNode<'a> {
    fn from(node: SelectNode<'a>) -> Self {
        PrevNode::Select(node)
    }
}

impl<'a> From<ValuesNode<'a>> for PrevNode<'a> {
    fn from(node: ValuesNode<'a>) -> Self {
        PrevNode::Values(node)
    }
}

impl<'a> From<GroupByNode<'a>> for PrevNode<'a> {
    fn from(node: GroupByNode<'a>) -> Self {
        PrevNode::GroupBy(node)
    }
}

impl<'a> From<HavingNode<'a>> for PrevNode<'a> {
    fn from(node: HavingNode<'a>) -> Self {
        PrevNode::Having(node)
    }
}

impl<'a> From<JoinConstraintNode<'a>> for PrevNode<'a> {
    fn from(node: JoinConstraintNode<'a>) -> Self {
        PrevNode::JoinConstraint(Box::new(node))
    }
}

impl<'a> From<JoinNode<'a>> for PrevNode<'a> {
    fn from(node: JoinNode<'a>) -> Self {
        PrevNode::Join(Box::new(node))
    }
}

impl<'a> From<HashJoinNode<'a>> for PrevNode<'a> {
    fn from(node: HashJoinNode<'a>) -> Self {
        PrevNode::HashJoin(node)
    }
}

impl<'a> From<FilterNode<'a>> for PrevNode<'a> {
    fn from(node: FilterNode<'a>) -> Self {
        PrevNode::Filter(node)
    }
}

impl<'a> From<OrderByNode<'a>> for PrevNode<'a> {
    fn from(node: OrderByNode<'a>) -> Self {
        PrevNode::OrderBy(node)
    }
}

impl<'a> From<ProjectNode<'a>> for PrevNode<'a> {
    fn from(node: ProjectNode<'a>) -> Self {
        PrevNode::ProjectNode(Box::new(node))
    }
}

#[derive(Clone, Debug)]
pub struct LimitNode<'a> {
    prev_node: PrevNode<'a>,
    expr: ExprNode<'a>,
}

impl<'a> LimitNode<'a> {
    pub fn new<N: Into<PrevNode<'a>>, T: Into<ExprNode<'a>>>(prev_node: N, expr: T) -> Self {
        Self {
            prev_node: prev_node.into(),
            expr: expr.into(),
        }
    }

    pub fn alias_as(self, table_alias: &'a str) -> TableFactorNode {
        QueryNode::LimitNode(self).alias_as(table_alias)
    }
}

impl<'a> Prebuild<Query> for LimitNode<'a> {
    fn prebuild(self) -> Result<Query> {
        let mut node_data = self.prev_node.prebuild()?;
        node_data.limit = Some(self.expr.try_into()?);

        Ok(node_data)
    }
}

#[cfg(test)]
mod tests {
    use {
        crate::{
            ast::{
                Join, JoinConstraint, JoinExecutor, JoinOperator, Query, Select, SetExpr,
                Statement, TableFactor, TableWithJoins,
            },
            ast_builder::{col, num, table, test, Build, SelectItemList},
        },
        pretty_assertions::assert_eq,
    };

    #[test]
    fn limit() {
        // select node -> limit node -> build
        let actual = table("Foo").select().limit(10).build();
        let expected = "SELECT * FROM Foo LIMIT 10";
        test(actual, expected);

        // group by node -> limit node -> build
        let actual = table("Foo").select().group_by("bar").limit(10).build();
        let expected = "SELECT * FROM Foo GROUP BY bar LIMIT 10";
        test(actual, expected);

        // having node -> limit node -> build
        let actual = table("Foo")
            .select()
            .group_by("bar")
            .having("bar = 10")
            .limit(10)
            .build();
        let expected = "SELECT * FROM Foo GROUP BY bar HAVING bar = 10 LIMIT 10";
        test(actual, expected);

        // join node -> limit node -> build
        let actual = table("Foo").select().join("Bar").limit(10).build();
        let expected = "SELECT * FROM Foo JOIN Bar LIMIT 10";
        test(actual, expected);

        // join node -> limit node -> build
        let actual = table("Foo").select().join_as("Bar", "B").limit(10).build();
        let expected = "SELECT * FROM Foo JOIN Bar AS B LIMIT 10";
        test(actual, expected);

        // join node -> limit node -> build
        let actual = table("Foo").select().left_join("Bar").limit(10).build();
        let expected = "SELECT * FROM Foo LEFT JOIN Bar LIMIT 10";
        test(actual, expected);

        // join node -> limit node -> build
        let actual = table("Foo")
            .select()
            .left_join_as("Bar", "B")
            .limit(10)
            .build();
        let expected = "SELECT * FROM Foo LEFT JOIN Bar AS B LIMIT 10";
        test(actual, expected);

        // group by node -> limit node -> build
        let actual = table("Foo").select().group_by("id").limit(10).build();
        let expected = "SELECT * FROM Foo GROUP BY id LIMIT 10";
        test(actual, expected);

        // having node -> limit node -> build
        let actual = table("Foo")
            .select()
            .group_by("id")
            .having(col("id").gt(10))
            .limit(10)
            .build();
        let expected = "SELECT * FROM Foo GROUP BY id HAVING id > 10 LIMIT 10";
        test(actual, expected);

        // join constraint node -> limit node -> build
        let actual = table("Foo")
            .select()
            .join("Bar")
            .on("Foo.id = Bar.id")
            .limit(10)
            .build();
        let expected = "SELECT * FROM Foo JOIN Bar ON Foo.id = Bar.id LIMIT 10";
        test(actual, expected);

        // filter node -> limit node -> build
        let actual = table("World")
            .select()
            .filter(col("id").gt(2))
            .limit(100)
            .build();
        let expected = "SELECT * FROM World WHERE id > 2 LIMIT 100";
        test(actual, expected);

        // order by node -> limit node -> build
        let actual = table("Hello").select().order_by("score").limit(3).build();
        let expected = "SELECT * FROM Hello ORDER BY score LIMIT 3";
        test(actual, expected);

        // project node -> limit node -> build
        let actual = table("Item").select().project("*").limit(10).build();
        let expected = "SELECT * FROM Item LIMIT 10";
        test(actual, expected);

        // hash join node -> limit node -> build
        let actual = table("Player")
            .select()
            .join("PlayerItem")
            .hash_executor("PlayerItem.user_id", "Player.id")
            .limit(100)
            .build();
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

            Ok(Statement::Query(Query {
                body: SetExpr::Select(Box::new(select)),
                order_by: Vec::new(),
                limit: Some(num(100).try_into().unwrap()),
                offset: None,
            }))
        };
        assert_eq!(actual, expected);

        // select node -> limit node -> derived subquery
        let actual = table("Foo")
            .select()
            .limit(10)
            .alias_as("Sub")
            .select()
            .build();
        let expected = "SELECT * FROM (SELECT * FROM Foo LIMIT 10) Sub";
        test(actual, expected);
    }
}
