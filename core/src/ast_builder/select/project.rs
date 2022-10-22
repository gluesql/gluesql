use {
    super::{NodeData, Prebuild},
    crate::{
        ast_builder::{
            FilterNode, GroupByNode, HashJoinNode, HavingNode, JoinConstraintNode, JoinNode,
            LimitNode, LimitOffsetNode, OffsetLimitNode, OffsetNode, OrderByNode, SelectItemList,
            SelectNode,
        },
        result::Result,
    },
};

#[derive(Clone)]
pub enum PrevNode<'a> {
    Select(SelectNode),
    GroupBy(GroupByNode<'a>),
    Having(HavingNode<'a>),
    Limit(LimitNode<'a>),
    LimitOffset(LimitOffsetNode<'a>),
    Offset(OffsetNode<'a>),
    OffsetLimit(OffsetLimitNode<'a>),
    Join(Box<JoinNode<'a>>),
    JoinConstraint(Box<JoinConstraintNode<'a>>),
    HashJoin(HashJoinNode<'a>),
    Filter(FilterNode<'a>),
    OrderBy(OrderByNode<'a>),
}

impl<'a> Prebuild for PrevNode<'a> {
    fn prebuild(self) -> Result<NodeData> {
        match self {
            Self::Select(node) => node.prebuild(),
            Self::GroupBy(node) => node.prebuild(),
            Self::Having(node) => node.prebuild(),
            Self::Limit(node) => node.prebuild(),
            Self::LimitOffset(node) => node.prebuild(),
            Self::Offset(node) => node.prebuild(),
            Self::OffsetLimit(node) => node.prebuild(),
            Self::Join(node) => node.prebuild(),
            Self::JoinConstraint(node) => node.prebuild(),
            Self::HashJoin(node) => node.prebuild(),
            Self::Filter(node) => node.prebuild(),
            Self::OrderBy(node) => node.prebuild(),
        }
    }
}

impl<'a> From<SelectNode> for PrevNode<'a> {
    fn from(node: SelectNode) -> Self {
        PrevNode::Select(node)
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

impl<'a> From<LimitNode<'a>> for PrevNode<'a> {
    fn from(node: LimitNode<'a>) -> Self {
        PrevNode::Limit(node)
    }
}

impl<'a> From<LimitOffsetNode<'a>> for PrevNode<'a> {
    fn from(node: LimitOffsetNode<'a>) -> Self {
        PrevNode::LimitOffset(node)
    }
}

impl<'a> From<OffsetNode<'a>> for PrevNode<'a> {
    fn from(node: OffsetNode<'a>) -> Self {
        PrevNode::Offset(node)
    }
}

impl<'a> From<OffsetLimitNode<'a>> for PrevNode<'a> {
    fn from(node: OffsetLimitNode<'a>) -> Self {
        PrevNode::OffsetLimit(node)
    }
}

impl<'a> From<JoinNode<'a>> for PrevNode<'a> {
    fn from(node: JoinNode<'a>) -> Self {
        PrevNode::Join(Box::new(node))
    }
}

impl<'a> From<JoinConstraintNode<'a>> for PrevNode<'a> {
    fn from(node: JoinConstraintNode<'a>) -> Self {
        PrevNode::JoinConstraint(Box::new(node))
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

#[derive(Clone)]
pub struct ProjectNode<'a> {
    prev_node: PrevNode<'a>,
    select_items_list: Vec<SelectItemList<'a>>,
}

impl<'a> ProjectNode<'a> {
    pub fn new<N: Into<PrevNode<'a>>, T: Into<SelectItemList<'a>>>(
        prev_node: N,
        select_items: T,
    ) -> Self {
        Self {
            prev_node: prev_node.into(),
            select_items_list: vec![select_items.into()],
        }
    }

    pub fn project<T: Into<SelectItemList<'a>>>(mut self, select_items: T) -> Self {
        self.select_items_list.push(select_items.into());

        self
    }
}

impl<'a> Prebuild for ProjectNode<'a> {
    fn prebuild(self) -> Result<NodeData> {
        let mut select_data = self.prev_node.prebuild()?;
        select_data.projection = self
            .select_items_list
            .into_iter()
            .map(TryInto::try_into)
            .collect::<Result<Vec<Vec<_>>>>()?
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();

        Ok(select_data)
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        ast::{
            Join, JoinConstraint, JoinExecutor, JoinOperator, Query, Select, SetExpr, Statement,
            TableFactor, TableWithJoins,
        },
        ast_builder::{col, table, test, Build, SelectItemList},
    };

    #[test]
    fn project() {
        // select node -> project node -> build
        let actual = table("Good").select().project("id").build();
        let expected = "SELECT id FROM Good";
        test(actual, expected);

        // select node -> project node -> build
        let actual = table("Group").select().project("*, Group.*, name").build();
        let expected = "SELECT *, Group.*, name FROM Group";
        test(actual, expected);

        // project node -> project node -> build
        let actual = table("Foo")
            .select()
            .project(vec!["col1", "col2"])
            .project("col3")
            .project(vec!["col4".into(), col("col5")])
            .project(col("col6"))
            .project("col7 as hello")
            .build();
        let expected = "
            SELECT
                col1, col2, col3,
                col4, col5, col6,
                col7 as hello
            FROM
                Foo
        ";
        test(actual, expected);

        // select node -> project node -> build
        let actual = table("Aliased")
            .select()
            .project("1 + 1 as col1, col2")
            .build();
        let expected = "SELECT 1 + 1 as col1, col2 FROM Aliased";
        test(actual, expected);
    }

    #[test]
    fn prev_nodes() {
        // select node -> project node -> build
        let actual = table("Foo").select().project("*").build();
        let expected = "SELECT * FROM Foo";
        test(actual, expected);

        // group by node -> project node -> build
        let actual = table("Bar")
            .select()
            .group_by("city")
            .project("city, COUNT(name) as num")
            .build();
        let expected = "
            SELECT
              city, COUNT(name) as num
            FROM Bar
            GROUP BY city
        ";
        test(actual, expected);

        // having node -> project node -> build
        let actual = table("Cat")
            .select()
            .filter(r#"type = "cute""#)
            .group_by("age")
            .having("SUM(length) < 1000")
            .project(col("age"))
            .project("SUM(length)")
            .build();
        let expected = r#"
            SELECT age, SUM(length)
            FROM Cat
            WHERE type = "cute"
            GROUP BY age
            HAVING SUM(length) < 1000;
        "#;
        test(actual, expected);

        // limit node -> project node -> build
        let actual = table("Item").select().limit(10).project("*").build();
        let expected = "SELECT * FROM Item LIMIT 10";
        test(actual, expected);

        // limit offset node -> project node -> build
        let actual = table("Operator")
            .select()
            .limit(100)
            .offset(50)
            .project("name")
            .build();
        let expected = "SELECT name FROM Operator LIMIT 100 OFFSET 50";
        test(actual, expected);

        // offset node -> project node -> build
        let actual = table("Item").select().offset(10).project("*").build();
        let expected = "SELECT * FROM Item OFFSET 10";
        test(actual, expected);

        // offset limit node -> project node -> build
        let actual = table("Operator")
            .select()
            .offset(3)
            .limit(10)
            .project("name")
            .build();
        let expected = "SELECT name FROM Operator LIMIT 10 OFFSET 3";
        test(actual, expected);

        // order by node -> project node -> build
        let actual = table("Foo")
            .select()
            .order_by("id asc")
            .project("id")
            .build();
        let expected = "SELECT id FROM Foo ORDER BY id asc";
        test(actual, expected);

        // hash join node -> project node -> build
        let actual = table("Player")
            .select()
            .join("PlayerItem")
            .hash_executor("PlayerItem.user_id", "Player.id")
            .project("Player.name, PlayerItem.name")
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
                projection: SelectItemList::from("Player.name, PlayerItem.name")
                    .try_into()
                    .unwrap(),
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
                limit: None,
                offset: None,
            }))
        };
        assert_eq!(actual, expected);
    }
}
