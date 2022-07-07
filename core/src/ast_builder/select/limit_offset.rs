use {
    super::{NodeData, Prebuild},
    crate::{
        ast::Statement,
        ast_builder::{ExprNode, LimitNode, ProjectNode, SelectItemList},
        result::Result,
    },
};

#[derive(Clone)]
pub enum PrevNode {
    Limit(LimitNode),
}

impl Prebuild for PrevNode {
    fn prebuild(self) -> Result<NodeData> {
        match self {
            Self::Limit(node) => node.prebuild(),
        }
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
    pub fn new<N: Into<PrevNode>, T: Into<ExprNode>>(prev_node: N, expr: T) -> Self {
        Self {
            prev_node: prev_node.into(),
            expr: expr.into(),
        }
    }

    pub fn project<T: Into<SelectItemList>>(self, select_items: T) -> ProjectNode {
        ProjectNode::new(self, select_items)
    }

    pub fn build(self) -> Result<Statement> {
        self.prebuild().map(NodeData::build_stmt)
    }
}

impl Prebuild for LimitOffsetNode {
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
    fn limit_offset() {
        let actual = table("World")
            .select()
            .filter("id > 2")
            .limit(100)
            .offset(3)
            .build();
        let expected = "SELECT * FROM World WHERE id > 2 OFFSET 3 LIMIT 100";
        test(actual, expected);
    }
}
