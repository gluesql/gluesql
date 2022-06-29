use crate::{
    ast::{Query, Statement},
    ast_builder::{ExprNode, GroupByNode, HavingNode, LimitNode, SelectNode},
    result::Result,
};

#[derive(Clone)]
pub enum PrevNode {
    Select(SelectNode),
    GroupBy(GroupByNode),
    Having(HavingNode),
    Limit(LimitNode),
}

impl PrevNode {
    fn build_query(self) -> Result<Query> {
        match self {
            Self::Select(node) => node.build_query(),
            Self::GroupBy(node) => node.build_query(),
            Self::Having(node) => node.build_query(),
            Self::Limit(node) => node.build_query(),
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

#[derive(Clone)]
pub struct LimitOffsetNode {
    prev_node: PrevNode,
    expr: ExprNode,
}

impl LimitOffsetNode {
    pub fn offset<N: Into<PrevNode>, T: Into<ExprNode>>(prev_node: N, expr: T) -> Self {
        Self {
            prev_node: prev_node.into(),
            expr: expr.into(),
        }
    }

    pub fn build(self) -> Result<Statement> {
        let mut query = self.prev_node.build_query()?;
        query.offset = Some(self.expr.try_into()?);

        Ok(Statement::Query(Box::new(query)))
    }
}
