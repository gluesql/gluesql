use {
    super::{NodeData, Prebuild},
    crate::{
        ast::Statement,
        ast_builder::{
            ExprList, ExprNode, HavingNode, LimitNode, OffsetNode, ProjectNode, SelectItemList,
            SelectNode,
        },
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
    pub fn new<N: Into<PrevNode>, T: Into<ExprList>>(prev_node: N, expr_list: T) -> Self {
        Self {
            prev_node: prev_node.into(),
            expr_list: expr_list.into(),
        }
    }

    pub fn having<T: Into<ExprNode>>(self, expr: T) -> HavingNode {
        HavingNode::new(self, expr)
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

    pub fn build(self) -> Result<Statement> {
        self.prebuild().map(NodeData::build_stmt)
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
    use crate::ast_builder::{col, table, test};

    #[test]
    fn group_by() {
        let actual = table("Bar")
            .select()
            .filter(col("id").is_null())
            .group_by("id, (a + name)")
            .build();
        let expected = "
            SELECT * FROM Bar
            WHERE id IS NULL
            GROUP BY id, (a + name)
        ";
        test(actual, expected);

        let actual = table("Foo")
            .select()
            .filter("name IS NOT NULL")
            .group_by(vec![col("id"), col("a").add(col("name"))])
            .build();
        let expected = "
            SELECT * FROM Foo
            WHERE name IS NOT NULL
            GROUP BY id, a + name
        ";
        test(actual, expected);

        let actual = table("Foo")
            .select()
            .group_by(vec!["id", "a + name"])
            .build();
        let expected = "
            SELECT * FROM Foo
            GROUP BY id, a + name
        ";
        test(actual, expected);
    }
}
