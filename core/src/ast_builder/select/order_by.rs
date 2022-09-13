use {
    super::{NodeData, Prebuild},
    crate::{
        ast::Statement,
        ast_builder::{GroupByNode, HavingNode, OrderByExprList, SelectNode},
        result::Result,
    },
};

#[derive(Clone)]
pub enum PrevNode {
    Select(SelectNode),
    Having(HavingNode),
    GroupBy(GroupByNode),
}

impl Prebuild for PrevNode {
    fn prebuild(self) -> Result<NodeData> {
        match self {
            Self::Select(node) => node.prebuild(),
            Self::Having(node) => node.prebuild(),
            Self::GroupBy(node) => node.prebuild(),
        }
    }
}

impl From<SelectNode> for PrevNode {
    fn from(node: SelectNode) -> Self {
        PrevNode::Select(node)
    }
}

impl From<HavingNode> for PrevNode {
    fn from(node: HavingNode) -> Self {
        PrevNode::Having(node)
    }
}

impl From<GroupByNode> for PrevNode {
    fn from(node: GroupByNode) -> Self {
        PrevNode::GroupBy(node)
    }
}

#[derive(Clone)]
pub struct OrderByNode {
    prev_node: PrevNode,
    expr_list: OrderByExprList,
}

impl OrderByNode {
    pub fn new<N: Into<PrevNode>, T: Into<OrderByExprList>>(prev_node: N, expr_list: T) -> Self {
        Self {
            prev_node: prev_node.into(),
            expr_list: expr_list.into(),
        }
    }

    pub fn build(self) -> Result<Statement> {
        self.prebuild().map(NodeData::build_stmt)
    }
}

impl Prebuild for OrderByNode {
    fn prebuild(self) -> Result<NodeData> {
        let mut select_data = self.prev_node.prebuild()?;
        select_data.order_by = self.expr_list.try_into()?;

        Ok(select_data)
    }
}

#[cfg(test)]
mod tests {
    use crate::ast_builder::{table, test};

    #[test]
    fn order_by() {
        let actual = table("Foo").select().order_by(vec!["name desc"]).build();
        let expected = "
            SELECT * FROM Foo
            ORDER BY name DESC
        ";
        test(actual, expected);
    }
}
