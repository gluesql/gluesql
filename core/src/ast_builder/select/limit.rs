use {
    super::{NodeData, Prebuild},
    crate::{
        ast::Statement,
        ast_builder::{
            ExprNode, GroupByNode, HavingNode, LimitOffsetNode, OrderByNode, ProjectNode,
            SelectItemList, SelectNode,
        },
        result::Result,
    },
};

#[derive(Clone)]
pub enum PrevNode {
    Select(SelectNode),
    GroupBy(GroupByNode),
    Having(HavingNode),
    OrderBy(OrderByNode),
}

impl Prebuild for PrevNode {
    fn prebuild(self) -> Result<NodeData> {
        match self {
            Self::Select(node) => node.prebuild(),
            Self::GroupBy(node) => node.prebuild(),
            Self::Having(node) => node.prebuild(),
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

impl From<OrderByNode> for PrevNode {
    fn from(node: OrderByNode) -> Self {
        PrevNode::OrderBy(node)
    }
}

#[derive(Clone)]
pub struct LimitNode {
    prev_node: PrevNode,
    expr: ExprNode,
}

impl LimitNode {
    pub fn new<N: Into<PrevNode>, T: Into<ExprNode>>(prev_node: N, expr: T) -> Self {
        Self {
            prev_node: prev_node.into(),
            expr: expr.into(),
        }
    }

    pub fn offset<T: Into<ExprNode>>(self, expr: T) -> LimitOffsetNode {
        LimitOffsetNode::new(self, expr)
    }

    pub fn project<T: Into<SelectItemList>>(self, select_items: T) -> ProjectNode {
        ProjectNode::new(self, select_items)
    }

    pub fn build(self) -> Result<Statement> {
        self.prebuild().map(NodeData::build_stmt)
    }
}

impl Prebuild for LimitNode {
    fn prebuild(self) -> Result<NodeData> {
        let mut select_data = self.prev_node.prebuild()?;
        select_data.limit = Some(self.expr.try_into()?);

        Ok(select_data)
    }
}

#[cfg(test)]
mod tests {
    use crate::ast_builder::{col, table, test};

    #[test]
    fn limit() {
        let actual = table("Hello").select().limit(10).build();
        let expected = "SELECT * FROM Hello LIMIT 10";
        test(actual, expected);

        let actual = table("World")
            .select()
            .filter(col("id").gt(2))
            .limit(100)
            .build();
        let expected = "SELECT * FROM World WHERE id > 2 LIMIT 100";
        test(actual, expected);

        let actual = table("Foo").select().group_by("name").limit(5).build();
        let expected = "SELECT * FROM Foo GROUP BY name LIMIT 5";
        test(actual, expected);

        let actual = table("Bar")
            .select()
            .group_by("city")
            .having("COUNT(name) < 100")
            .limit(3)
            .build();
        let expected = "
            SELECT * FROM Bar
            GROUP BY city
            HAVING COUNT(name) < 100
            LIMIT 3;
        ";
        test(actual, expected);
    }
}
