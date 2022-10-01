use {
    super::{NodeData, Prebuild},
    crate::{
        ast_builder::{
            FilterNode, GroupByNode, HavingNode, JoinConstraintNode, JoinNode, LimitNode,
            LimitOffsetNode, OffsetLimitNode, OffsetNode, OrderByNode, SelectItemList, SelectNode,
        },
        result::Result,
    },
};

#[derive(Clone)]
pub enum PrevNode {
    Select(SelectNode),
    GroupBy(GroupByNode),
    Having(HavingNode),
    Limit(LimitNode),
    LimitOffset(LimitOffsetNode),
    Offset(OffsetNode),
    OffsetLimit(OffsetLimitNode),
    Join(Box<JoinNode>),
    JoinConstraint(Box<JoinConstraintNode>),
    Filter(FilterNode),
    OrderBy(OrderByNode),
}

impl Prebuild for PrevNode {
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
            Self::Filter(node) => node.prebuild(),
            Self::OrderBy(node) => node.prebuild(),
        }
    }
}

impl From<SelectNode> for PrevNode {
    fn from(node: SelectNode) -> Self {
        PrevNode::Select(node)
    }
}

impl From<GroupByNode> for PrevNode {
    fn from(node: GroupByNode) -> Self {
        PrevNode::GroupBy(node)
    }
}

impl From<HavingNode> for PrevNode {
    fn from(node: HavingNode) -> Self {
        PrevNode::Having(node)
    }
}

impl From<LimitNode> for PrevNode {
    fn from(node: LimitNode) -> Self {
        PrevNode::Limit(node)
    }
}

impl From<LimitOffsetNode> for PrevNode {
    fn from(node: LimitOffsetNode) -> Self {
        PrevNode::LimitOffset(node)
    }
}

impl From<OffsetNode> for PrevNode {
    fn from(node: OffsetNode) -> Self {
        PrevNode::Offset(node)
    }
}

impl From<OffsetLimitNode> for PrevNode {
    fn from(node: OffsetLimitNode) -> Self {
        PrevNode::OffsetLimit(node)
    }
}

impl From<JoinNode> for PrevNode {
    fn from(node: JoinNode) -> Self {
        PrevNode::Join(Box::new(node))
    }
}

impl From<JoinConstraintNode> for PrevNode {
    fn from(node: JoinConstraintNode) -> Self {
        PrevNode::JoinConstraint(Box::new(node))
    }
}

impl From<FilterNode> for PrevNode {
    fn from(node: FilterNode) -> Self {
        PrevNode::Filter(node)
    }
}

impl From<OrderByNode> for PrevNode {
    fn from(node: OrderByNode) -> Self {
        PrevNode::OrderBy(node)
    }
}

#[derive(Clone)]
pub struct ProjectNode {
    prev_node: PrevNode,
    select_items_list: Vec<SelectItemList>,
}

impl ProjectNode {
    pub fn new<N: Into<PrevNode>, T: Into<SelectItemList>>(prev_node: N, select_items: T) -> Self {
        Self {
            prev_node: prev_node.into(),
            select_items_list: vec![select_items.into()],
        }
    }

    pub fn project<T: Into<SelectItemList>>(mut self, select_items: T) -> Self {
        self.select_items_list.push(select_items.into());

        self
    }
}

impl Prebuild for ProjectNode {
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
    use crate::ast_builder::{col, table, test, Build};

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
    }
}
