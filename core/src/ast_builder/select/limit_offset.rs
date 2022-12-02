use {
    super::{NodeData, Prebuild},
    crate::{
        ast_builder::{ExprNode, LimitNode, ProjectNode, SelectItemList},
        result::Result,
    },
};

#[derive(Clone, Debug)]
pub enum PrevNode<'a> {
    Limit(LimitNode<'a>),
}

impl<'a> Prebuild for PrevNode<'a> {
    fn prebuild(self) -> Result<NodeData> {
        match self {
            Self::Limit(node) => node.prebuild(),
        }
    }
}

impl<'a> From<LimitNode<'a>> for PrevNode<'a> {
    fn from(node: LimitNode<'a>) -> Self {
        PrevNode::Limit(node)
    }
}

#[derive(Clone, Debug)]
pub struct LimitOffsetNode<'a> {
    prev_node: PrevNode<'a>,
    expr: ExprNode<'a>,
}

impl<'a> LimitOffsetNode<'a> {
    pub fn new<N: Into<PrevNode<'a>>, T: Into<ExprNode<'a>>>(prev_node: N, expr: T) -> Self {
        Self {
            prev_node: prev_node.into(),
            expr: expr.into(),
        }
    }

    pub fn project<T: Into<SelectItemList<'a>>>(self, select_items: T) -> ProjectNode<'a> {
        ProjectNode::new(self, select_items)
    }
}

impl<'a> Prebuild for LimitOffsetNode<'a> {
    fn prebuild(self) -> Result<NodeData> {
        let mut select_data = self.prev_node.prebuild()?;
        select_data.offset = Some(self.expr.try_into()?);

        Ok(select_data)
    }
}

#[cfg(test)]
mod tests {
    use crate::ast_builder::{table, test, Build};

    #[test]
    fn limit_offset() {
        // limit node -> offset node -> build
        let actual = table("World")
            .select()
            .filter("id > 2")
            .limit(100)
            .offset(3)
            .build();
        let expected = "SELECT * FROM World WHERE id > 2 OFFSET 3 LIMIT 100";
        test(actual, expected);

        // limit node -> offset node -> project node
        let actual = table("World")
            .select()
            .filter("id > 2")
            .limit(100)
            .offset(3)
            .project("id")
            .build();
        let expected = "SELECT id FROM World WHERE id > 2 OFFSET 3 LIMIT 100";
        test(actual, expected);
    }
}
