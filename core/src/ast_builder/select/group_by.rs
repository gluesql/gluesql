use {
    super::{NodeData, Prebuild},
    crate::{
        ast::Statement,
        ast_builder::{
            ExprList, ExprNode, FilterNode, HavingNode, JoinConstraintNode, JoinNode, LimitNode,
            OffsetNode, ProjectNode, SelectItemList, SelectNode,
        },
        result::Result,
    },
};

#[derive(Clone)]
pub enum PrevNode {
    Select(SelectNode),
    Join(JoinNode),
    JoinConstraint(JoinConstraintNode),
    Filter(FilterNode),
}

impl Prebuild for PrevNode {
    fn prebuild(self) -> Result<NodeData> {
        match self {
            Self::Select(node) => node.prebuild(),
            Self::Join(node) => node.prebuild(),
            Self::JoinConstraint(node) => node.prebuild(),
            Self::Filter(node) => node.prebuild(),
        }
    }
}

impl From<SelectNode> for PrevNode {
    fn from(node: SelectNode) -> Self {
        PrevNode::Select(node)
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
        PrevNode::Filter(node)
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
        // select node -> group by node -> having node
        let actual = table("Foo")
            .select()
            .group_by("a")
            .having("COUNT(id) > 10")
            .build();
        let expected = "SELECT * FROM Foo GROUP BY a HAVING COUNT(id) > 10";
        test(actual, expected);

        // select node -> group by node -> offset node
        let actual = table("Foo").select().group_by("a").offset(10).build();
        let expected = "SELECT * FROM Foo GROUP BY a OFFSET 10";
        test(actual, expected);

        // select node -> group by node -> limit node
        let actual = table("Foo").select().group_by("a").limit(10).build();
        let expected = "SELECT * FROM Foo GROUP BY a LIMIT 10";
        test(actual, expected);

        // select node -> group by node -> project node
        let actual = table("Foo")
            .select()
            .group_by("a")
            .project(col("b"))
            .build();
        let expected = "SELECT b FROM Foo GROUP BY a";
        test(actual, expected);

        // select node -> group by node -> build
        let acutal = table("Foo").select().group_by("a").build();
        let expected = "SELECT * FROM Foo GROUP BY a";
        test(acutal, expected);

        // join node -> group by node -> having node
        let actual = table("Foo")
            .select()
            .join("Bar")
            .group_by("b")
            .having("COUNT(id) > 10")
            .build();
        let expected = "SELECT * FROM Foo JOIN Bar GROUP BY b HAVING COUNT(id) > 10";
        test(actual, expected);

        // join node -> group by node -> offset node
        let actual = table("Foo")
            .select()
            .join("Bar")
            .group_by("b")
            .offset(10)
            .build();
        let expected = "SELECT * FROM Foo JOIN Bar GROUP BY b OFFSET 10";
        test(actual, expected);

        // join node -> group by node -> limit node
        let actual = table("Foo")
            .select()
            .join("Bar")
            .group_by("b")
            .limit(10)
            .build();
        let expected = "SELECT * FROM Foo JOIN Bar GROUP BY b LIMIT 10";
        test(actual, expected);

        // join node -> group by node -> project node
        let actual = table("Foo")
            .select()
            .join("Bar")
            .group_by("b")
            .project(col("c"))
            .build();
        let expected = "SELECT c FROM Foo JOIN Bar GROUP BY b";
        test(actual, expected);

        // join node -> group by node -> build
        let actual = table("Foo").select().join("Bar").group_by("b").build();
        let expected = "SELECT * FROM Foo JOIN Bar GROUP BY b";
        test(actual, expected);

        // join constraint node -> group by node -> having node
        let actual = table("Foo")
            .select()
            .join("Bar")
            .on("Foo.id = Bar.id")
            .group_by("b")
            .having("id > 10")
            .build();
        let expected = "SELECT * FROM Foo JOIN Bar ON Foo.id = Bar.id GROUP BY b HAVING id > 10";
        test(actual, expected);

        // join constraint node -> group by node -> offset node
        let actual = table("Foo")
            .select()
            .join("Bar")
            .on("Foo.id = Bar.id")
            .group_by("b")
            .offset(10)
            .build();
        let expected = "SELECT * FROM Foo JOIN Bar ON Foo.id = Bar.id GROUP BY b OFFSET 10";
        test(actual, expected);

        // join constraint node -> group by node -> limit node
        let actual = table("Foo")
            .select()
            .join("Bar")
            .on("Foo.id = Bar.id")
            .group_by("b")
            .limit(10)
            .build();
        let expected = "SELECT * FROM Foo JOIN Bar ON Foo.id = Bar.id GROUP BY b LIMIT 10";
        test(actual, expected);

        // join constraint node -> group by node -> project node
        let actual = table("Foo")
            .select()
            .join("Bar")
            .on("Foo.id = Bar.id")
            .group_by("b")
            .project(col("c"))
            .build();
        let expected = "SELECT c FROM Foo JOIN Bar ON Foo.id = Bar.id GROUP BY b";
        test(actual, expected);

        // join constraint node -> group by node -> build
        let actual = table("Foo")
            .select()
            .join("Bar")
            .on("Foo.id = Bar.id")
            .group_by("b")
            .build();
        let expected = "SELECT * FROM Foo JOIN Bar ON Foo.id = Bar.id GROUP BY b";
        test(actual, expected);

        // filter node -> group by node -> having node
        let actual = table("Foo")
            .select()
            .group_by("a")
            .having("COUNT(id) > 10")
            .build();
        let expected = "SELECT * FROM Foo GROUP BY a HAVING COUNT(id) > 10";
        test(actual, expected);

        // filter node -> group by node -> offset node
        let actual = table("Foo").select().group_by("a").offset(10).build();
        let expected = "SELECT * FROM Foo GROUP BY a OFFSET 10";
        test(actual, expected);

        // filter node -> group by node -> limit node
        let actual = table("Foo").select().group_by("a").limit(10).build();
        let expected = "SELECT * FROM Foo GROUP BY a LIMIT 10";
        test(actual, expected);

        // filter node -> group by node -> project node
        let actual = table("Foo")
            .select()
            .group_by("a")
            .project(col("c"))
            .build();
        let expected = "SELECT c FROM Foo GROUP BY a";
        test(actual, expected);

        // filter node -> group by node -> build
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

        // filter node -> group by node -> build
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

        // filter node -> group by node -> build
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
