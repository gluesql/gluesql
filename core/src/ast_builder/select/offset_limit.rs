use {
    super::{NodeData, Prebuild},
    crate::{
        ast_builder::{ExprNode, OffsetNode, ProjectNode, SelectItemList},
        result::Result,
    },
};

#[derive(Clone)]
pub enum PrevNode<'a> {
    Offset(OffsetNode<'a>),
}

impl<'a> Prebuild for PrevNode<'a> {
    fn prebuild(self) -> Result<NodeData> {
        match self {
            Self::Offset(node) => node.prebuild(),
        }
    }
}

impl<'a> From<OffsetNode<'a>> for PrevNode<'a> {
    fn from(node: OffsetNode<'a>) -> Self {
        PrevNode::Offset(node)
    }
}

#[derive(Clone)]
pub struct OffsetLimitNode<'a> {
    prev_node: PrevNode<'a>,
    expr: ExprNode<'a>,
}

impl<'a> OffsetLimitNode<'a> {
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

impl<'a> Prebuild for OffsetLimitNode<'a> {
    fn prebuild(self) -> Result<NodeData> {
        let mut select_data = self.prev_node.prebuild()?;
        select_data.limit = Some(self.expr.try_into()?);

        Ok(select_data)
    }
}

#[cfg(test)]
mod tests {
    use crate::ast_builder::{table, test, Build};

    #[test]
    fn offset_limit() {
        // offset node -> limit node -> build node
        let actual = table("Bar")
            .select()
            .group_by("city")
            .having("COUNT(name) < 100")
            .offset(1)
            .limit(3)
            .build();
        let expected = "
            SELECT * FROM Bar
            GROUP BY city
            HAVING COUNT(name) < 100
            OFFSET 1
            LIMIT 3;
        ";
        test(actual, expected);

        // offset node -> limit node -> project node
        let actual = table("Bar")
            .select()
            .group_by("city")
            .having("COUNT(name) < 100")
            .offset(1)
            .limit(3)
            .project("city")
            .build();
        let expected = "
            SELECT city FROM Bar
            GROUP BY city
            HAVING COUNT(name) < 100
            OFFSET 1
            LIMIT 3;
        ";
        test(actual, expected);
    }
}
