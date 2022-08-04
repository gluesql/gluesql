use crate::ast_builder::OrderByExprList;

use super::OrderByNode;

use {
    super::{NodeData, Prebuild},
    crate::{
        ast::Statement,
        ast_builder::{ExprNode, GroupByNode, LimitNode, OffsetNode, ProjectNode, SelectItemList},
        result::Result,
    },
};

#[derive(Clone)]
pub enum PrevNode {
    GroupBy(GroupByNode),
}

impl Prebuild for PrevNode {
    fn prebuild(self) -> Result<NodeData> {
        match self {
            Self::GroupBy(node) => node.prebuild(),
        }
    }
}

impl From<GroupByNode> for PrevNode {
    fn from(node: GroupByNode) -> Self {
        PrevNode::GroupBy(node)
    }
}

#[derive(Clone)]
pub struct HavingNode {
    prev_node: PrevNode,
    expr: ExprNode,
}

impl HavingNode {
    pub fn new<N: Into<PrevNode>, T: Into<ExprNode>>(prev_node: N, expr: T) -> Self {
        Self {
            prev_node: prev_node.into(),
            expr: expr.into(),
        }
    }

    pub fn offset<T: Into<ExprNode>>(self, expr: T) -> OffsetNode {
        OffsetNode::new(self, expr)
    }

    pub fn limit<T: Into<ExprNode>>(self, expr: T) -> LimitNode {
        LimitNode::new(self, expr)
    }

    pub fn order_by<T: Into<OrderByExprList>>(self, expr_list: T) -> OrderByNode {
        OrderByNode::new(self, expr_list)
    }

    pub fn project<T: Into<SelectItemList>>(self, select_items: T) -> ProjectNode {
        ProjectNode::new(self, select_items)
    }

    pub fn build(self) -> Result<Statement> {
        self.prebuild().map(NodeData::build_stmt)
    }
}

impl Prebuild for HavingNode {
    fn prebuild(self) -> Result<NodeData> {
        let mut select_data = self.prev_node.prebuild()?;
        select_data.having = Some(self.expr.try_into()?);

        Ok(select_data)
    }
}

#[cfg(test)]
mod tests {
    use crate::ast_builder::{table, test};

    #[test]
    fn having() {
        let actual = table("Bar")
            .select()
            .filter("id IS NULL")
            .group_by("id, (a + name)")
            .having("COUNT(id) > 10")
            .build();
        let expected = "
            SELECT * FROM Bar
            WHERE id IS NULL
            GROUP BY id, (a + name)
            HAVING COUNT(id) > 10
        ";
        test(actual, expected);
    }
}
