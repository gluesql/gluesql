use {
    super::{build_stmt, NodeData, Prebuild},
    crate::{
        ast::Statement,
        ast_builder::{ExprNode, GroupByNode, HavingNode, OffsetLimitNode, SelectNode},
        result::Result,
    },
};

#[derive(Clone)]
pub enum PrevNode {
    Select(SelectNode),
    GroupBy(GroupByNode),
    Having(HavingNode),
}

impl Prebuild for PrevNode {
    fn prebuild(self) -> Result<NodeData> {
        match self {
            Self::Select(node) => node.prebuild(),
            Self::GroupBy(node) => node.prebuild(),
            Self::Having(node) => node.prebuild(),
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

#[derive(Clone)]
pub struct OffsetNode {
    prev_node: PrevNode,
    expr: ExprNode,
}

impl OffsetNode {
    pub fn offset<N: Into<PrevNode>, T: Into<ExprNode>>(prev_node: N, expr: T) -> Self {
        Self {
            prev_node: prev_node.into(),
            expr: expr.into(),
        }
    }

    pub fn limit<T: Into<ExprNode>>(self, expr: T) -> OffsetLimitNode {
        OffsetLimitNode::limit(self, expr)
    }

    pub fn build(self) -> Result<Statement> {
        let select_data = self.prebuild()?;

        Ok(build_stmt(select_data))
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
    use crate::ast_builder::{select::test, Builder};

    #[test]
    fn offset() {
        let actual = Builder::table("Hello").select().offset(10).build();
        let expected = "SELECT * FROM Hello OFFSET 10";
        test(actual, expected);

        let actual = Builder::table("World")
            .select()
            .filter("id > 2")
            .offset(100)
            .build();
        let expected = "SELECT * FROM World WHERE id > 2 OFFSET 100";
        test(actual, expected);

        let actual = Builder::table("Foo")
            .select()
            .group_by("name")
            .offset(5)
            .build();
        let expected = "SELECT * FROM Foo GROUP BY name OFFSET 5";
        test(actual, expected);

        let actual = Builder::table("Bar")
            .select()
            .group_by("city")
            .having("COUNT(name) < 100")
            .offset(3)
            .build();
        let expected = "
            SELECT * FROM Bar
            GROUP BY city
            HAVING COUNT(name) < 100
            OFFSET 3;
        ";
        test(actual, expected);
    }
}
