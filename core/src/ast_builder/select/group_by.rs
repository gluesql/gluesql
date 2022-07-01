use {
    super::{build_stmt, NodeData, Prebuild},
    crate::{
        ast::Statement,
        ast_builder::{ExprList, ExprNode, HavingNode, LimitNode, OffsetNode, SelectNode},
        result::Result,
    },
};

#[derive(Clone)]
pub enum PrevNode {
    Select(SelectNode),
}

impl Prebuild for PrevNode {
    fn prebuild(self) -> Result<NodeData> {
        match self {
            Self::Select(node) => node.prebuild(),
        }
    }
}

impl From<SelectNode> for PrevNode {
    fn from(node: SelectNode) -> Self {
        PrevNode::Select(node)
    }
}

#[derive(Clone)]
pub struct GroupByNode {
    prev_node: PrevNode,
    expr_list: ExprList,
}

impl GroupByNode {
    pub fn group_by<N: Into<PrevNode>, T: Into<ExprList>>(prev_node: N, expr_list: T) -> Self {
        Self {
            prev_node: prev_node.into(),
            expr_list: expr_list.into(),
        }
    }

    pub fn having<T: Into<ExprNode>>(self, expr: T) -> HavingNode {
        HavingNode::having(self, expr)
    }

    pub fn offset<T: Into<ExprNode>>(self, expr: T) -> OffsetNode {
        OffsetNode::offset(self, expr)
    }

    pub fn limit<T: Into<ExprNode>>(self, expr: T) -> LimitNode {
        LimitNode::limit(self, expr)
    }

    pub fn build(self) -> Result<Statement> {
        let select_data = self.prebuild()?;

        Ok(build_stmt(select_data))
    }
}

impl Prebuild for GroupByNode {
    fn prebuild(self) -> Result<NodeData> {
        let mut select_data = self.prev_node.prebuild()?;
        select_data.group_by = self.expr_list.try_into()?;

        Ok(select_data)
    }
}

#[cfg(test)]
mod tests {
    use crate::ast_builder::{select::test, Builder};

    #[test]
    fn select() {
        let actual = Builder::table("Bar")
            .select()
            .filter("id IS NULL")
            .group_by("id, (a + name)")
            .build();
        let expected = "
            SELECT * FROM Bar
            WHERE id IS NULL
            GROUP BY id, (a + name)
        ";
        test(actual, expected);
    }
}
