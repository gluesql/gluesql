use {
    super::{NodeData, Prebuild},
    crate::{
        ast::Statement,
        ast_builder::{
            ExprNode, FilterNode, GroupByNode, HavingNode, JoinConstraintNode, JoinNode,
            OffsetLimitNode, ProjectNode, SelectItemList, SelectNode,
        },
        result::Result,
    },
};

#[derive(Clone)]
pub enum PrevNode {
    Select(SelectNode),
    GroupBy(GroupByNode),
    Having(HavingNode),
    Join(Box<JoinNode>),
    JoinConstraint(Box<JoinConstraintNode>),
    Filter(FilterNode),
}

impl Prebuild for PrevNode {
    fn prebuild(self) -> Result<NodeData> {
        match self {
            Self::Select(node) => node.prebuild(),
            Self::GroupBy(node) => node.prebuild(),
            Self::Having(node) => node.prebuild(),
            Self::Join(node) => node.prebuild(),
            Self::JoinConstraint(node) => node.prebuild(),
            Self::Filter(node) => node.prebuild(),
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

impl From<JoinConstraintNode> for PrevNode {
    fn from(node: JoinConstraintNode) -> Self {
        PrevNode::JoinConstraint(Box::new(node))
    }
}

impl From<JoinNode> for PrevNode {
    fn from(node: JoinNode) -> Self {
        PrevNode::Join(Box::new(node))
    }
}

impl From<FilterNode> for PrevNode {
    fn from(node: FilterNode) -> Self {
        PrevNode::Filter(node)
    }
}

#[derive(Clone)]
pub struct OffsetNode {
    prev_node: PrevNode,
    expr: ExprNode,
}

impl OffsetNode {
    pub fn new<N: Into<PrevNode>, T: Into<ExprNode>>(prev_node: N, expr: T) -> Self {
        Self {
            prev_node: prev_node.into(),
            expr: expr.into(),
        }
    }

    pub fn limit<T: Into<ExprNode>>(self, expr: T) -> OffsetLimitNode {
        OffsetLimitNode::new(self, expr)
    }

    pub fn project<T: Into<SelectItemList>>(self, select_items: T) -> ProjectNode {
        ProjectNode::new(self, select_items)
    }

    pub fn build(self) -> Result<Statement> {
        self.prebuild().map(NodeData::build_stmt)
    }
}

impl Prebuild for OffsetNode {
    fn prebuild(self) -> Result<NodeData> {
        let mut select_data = self.prev_node.prebuild()?;
        select_data.offset = Some(self.expr.try_into()?);

        Ok(select_data)
    }
}

#[cfg(test)]
mod tests {
    use crate::ast_builder::{table, test};

    #[test]
    fn offset() {
        // select node -> offset node -> build
        let actual = table("Foo").select().offset(10).build();
        let expected = "SELECT * FROM Foo OFFSET 10";
        test(actual, expected);

        // group by node -> offset node -> build
        let actual = table("Foo").select().group_by("id").offset(10).build();
        let expected = "SELECT * FROM Foo GROUP BY id OFFSET 10";
        test(actual, expected);

        // having node -> offset node -> build
        let actual = table("Foo")
            .select()
            .group_by("id")
            .having("id > 10")
            .offset(10)
            .build();
        let expected = "SELECT * FROM Foo GROUP BY id HAVING id > 10 OFFSET 10";
        test(actual, expected);

        // join node -> offset node -> build
        let actual = table("Foo").select().join("Bar").offset(10).build();
        let expected = "SELECT * FROM Foo JOIN Bar OFFSET 10";
        test(actual, expected);

        // join node -> offset node -> build
        let actual = table("Foo").select().join_as("Bar", "B").offset(10).build();
        let expected = "SELECT * FROM Foo JOIN Bar AS B OFFSET 10";
        test(actual, expected);

        // join node -> offset node -> build
        let actual = table("Foo")
            .select()
            .left_join("Bar")
            .on("Foo.id = Bar.id")
            .offset(10)
            .build();
        let expected = "SELECT * FROM Foo LEFT JOIN Bar ON Foo.id = Bar.id OFFSET 10";
        test(actual, expected);

        // join node -> offset node -> build
        let actual = table("Foo")
            .select()
            .left_join_as("Bar", "B")
            .on("Foo.id = B.id")
            .offset(10)
            .build();
        let expected = "SELECT * FROM Foo LEFT JOIN Bar AS B ON Foo.id = B.id OFFSET 10";
        test(actual, expected);

        // join constraint node -> offset node -> build
        let actual = table("Foo")
            .select()
            .join("Bar")
            .on("Foo.id = Bar.id")
            .offset(10)
            .build();
        let expected = "SELECT * FROM Foo JOIN Bar ON Foo.id = Bar.id OFFSET 10";
        test(actual, expected);

        // filter node -> offset node -> build
        let actual = table("Bar").select().filter("id > 2").offset(100).build();
        let expected = "SELECT * FROM Bar WHERE id > 2 OFFSET 100";
        test(actual, expected);
    }
}
