use {
    super::{NodeData, Prebuild},
    crate::{
        ast::Statement,
        ast_builder::{ExprNode, JoinNode, ProjectNode, SelectItemList},
        result::Result,
    },
};

#[derive(Clone)]
pub enum PrevNode {
    Join(JoinNode),
}

impl Prebuild for PrevNode {
    fn prebuild(self) -> Result<NodeData> {
        match self {
            Self::Join(node) => node.prebuild(),
        }
    }
}

impl From<JoinNode> for PrevNode {
    fn from(node: JoinNode) -> Self {
        PrevNode::Join(node)
    }
}

#[derive(Clone)]
pub struct JoinConstraintNode {
    prev_node: PrevNode,
    expr: On(ExprNode),
}

impl JoinConstraintNode {
    pub fn new<N: Into<PrevNode>, T: Into<On>>(prev_node: N, expr: T) -> Self {
        Self {
            prev_node: prev_node.into(),
            expr: expr.into(),
        }
    }
    pub fn build(self) -> Result<Statement> {
        self.prebuild().map(NodeData::build_stmt)
    }
}

impl Prebuild for JoinConstraintNode {
    fn prebuild(self) -> Result<NodeData> {
        let mut select_data = self.prev_node.prebuild()?;
        select_data.join_constraint = Some(self.expr.try_into()?);
        Ok(select_data)
    }
}

#[cfg(test)]
mod tests {
    use crate::ast_builder::{table, test};

    #[test]
    fn join_constraint() {
        let actual = table("Bar")
            .select()
            .inner_join("b")
            .on("a.id = b.id")
            .build();
        let statement = "SELECT * FROM Bar INNER JOIN b ON a.id = b.id";
        test(actual, statement);
    }
}
