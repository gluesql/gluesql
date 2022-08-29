use {
    super::{NodeData, Prebuild},
    crate::{
        ast::Statement,
        ast_builder::{
            ExprList, ExprNode, GroupByNode, JoinConstraintNode, JoinNode, LimitNode, OffsetNode,
            ProjectNode, SelectItemList,
        },
        result::Result,
    },
};

#[derive(Clone)]
pub enum PrevNode {
    Join(JoinNode),
    JoinConstraint(JoinConstraintNode),
    Filter(Box<FilterNode>),
}

impl Prebuild for PrevNode {
    fn prebuild(self) -> Result<NodeData> {
        match self {
            Self::Join(node) => node.prebuild(),
            Self::JoinConstraint(node) => node.prebuild(),
            Self::Filter(node) => node.prebuild(),
        }
    }
}

impl From<JoinNode> for PrevNode {
    fn from(node: JoinNode) -> Self {
        PrevNode::Join(node)
    }
}

impl From<JoinConstraintNode> for PrevNode {
    fn from(node: JoinConstraintNode) -> Self {
        PrevNode::JoinConstraint(node)
    }
}

impl From<FilterNode> for PrevNode {
    fn from(node: FilterNode) -> Self {
        PrevNode::Filter(Box::new(node))
    }
}

#[derive(Clone)]
pub struct FilterNode {
    prev_node: PrevNode,
    filter_expr: ExprNode,
}

impl FilterNode {
    pub fn new<N: Into<PrevNode>, T: Into<ExprNode>>(prev_node: N, expr: T) -> Self {
        Self {
            prev_node: prev_node.into(),
            filter_expr: expr.into(),
        }
    }

    pub fn filter<N: Into<PrevNode>, T: Into<ExprNode>>(prev_node: N, expr: T) -> Self {
        FilterNode::new(prev_node, expr)
    }

    pub fn offset<T: Into<ExprNode>>(self, expr: T) -> OffsetNode {
        OffsetNode::new(self, expr)
    }

    pub fn limit<T: Into<ExprNode>>(self, expr: T) -> LimitNode {
        LimitNode::new(self, expr)
    }

    pub fn project<T: Into<SelectItemList>>(self, select_items: T) -> ProjectNode {
        ProjectNode::new(self, select_items)
    }

    pub fn group_by<T: Into<ExprList>>(self, expr_list: T) -> GroupByNode {
        GroupByNode::new(self, expr_list)
    }

    pub fn build(self) -> Result<Statement> {
        self.prebuild().map(NodeData::build_stmt)
    }
}

impl Prebuild for FilterNode {
    fn prebuild(self) -> Result<NodeData> {
        let mut select_data = self.prev_node.prebuild()?;
        select_data.selection = Some(self.filter_expr.try_into()?);
        Ok(select_data)
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        ast::{BinaryOperator, Expr},
        ast_builder::{table, test},
    };

    #[test]
    fn filter() {
        let actual = table("Bar").select().filter("id IS NULL").build();
        let expected = "SELECT * FROM Bar WHERE id IS NULL";
        test(actual, expected);

        let actual = table("Bar")
            .select()
            .filter("id IS NULL")
            .filter("id > 10")
            .filter("id < 20")
            .build();
        let expected = "SELECT * FROM Bar WHERE id IS NULL AND id > 10 AND id < 20";
        test(actual, expected);

        let actual = table("Foo")
            .select()
            .filter(Expr::BinaryOp {
                left: Box::new(Expr::Identifier("col1".to_owned())),
                op: BinaryOperator::Gt,
                right: Box::new(Expr::Identifier("col2".to_owned())),
            })
            .build();
        let expected = "SELECT * FROM Foo WHERE col1 > col2";
        test(actual, expected);
    }
}
