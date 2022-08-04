use {
    super::{NodeData, Prebuild},
    crate::{ast::Statement, ast_builder::*, result::Result},
};

#[derive(Clone)]
pub enum PrevNode {
    Select(SelectNode),
    Having(HavingNode),
    GroupBy(GroupByNode),
}

impl Prebuild for PrevNode {
    fn prebuild(self) -> Result<NodeData> {
        match self {
            Self::Select(node) => node.prebuild(),
            Self::Having(node) => node.prebuild(),
            Self::GroupBy(node) => node.prebuild(),
        }
    }
}

impl From<SelectNode> for PrevNode {
    fn from(node: SelectNode) -> Self {
        PrevNode::Select(node)
    }
}

impl From<HavingNode> for PrevNode {
    fn from(node: HavingNode) -> Self {
        PrevNode::Having(node)
    }
}

impl From<GroupByNode> for PrevNode {
    fn from(node: GroupByNode) -> Self {
        PrevNode::GroupBy(node)
    }
}

#[derive(Clone)]
pub struct OrderByNode {
    prev_node: PrevNode,
    expr_list: OrderByExprList,
}

impl OrderByNode {
    pub fn new<N: Into<PrevNode>, T: Into<OrderByExprList>>(prev_node: N, expr_list: T) -> Self {
        Self {
            prev_node: prev_node.into(),
            expr_list: expr_list.into(),
        }
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

impl Prebuild for OrderByNode {
    fn prebuild(self) -> Result<NodeData> {
        let mut select_data = self.prev_node.prebuild()?;
        select_data.order_by = self.expr_list.try_into()?;

        Ok(select_data)
    }
}

#[cfg(test)]
mod tests {
    use crate::ast_builder::{table, test};

    #[test]
    fn order_by() {
        let actual = table("Foo").select().order_by(vec!["name asc"]).build();
        let expected = "
            SELECT * FROM Foo
            ORDER BY name ASC
        ";
        test(actual, expected);

        let actual = table("Foo")
            .select()
            .group_by("city")
            .having("COUNT(name) < 100")
            .order_by(vec!["name asc"])
            .limit(3)
            .offset(2)
            .build();
        let expected = "
            SELECT * FROM Foo
            GROUP BY city
            HAVING COUNT(name) < 100
            ORDER BY name ASC
            LIMIT 3
            OFFSET 2
        ";
        test(actual, expected);

        let actual = table("Bar")
            .select()
            .order_by(vec!["name asc", "id desc"])
            .build();
        let expected = "SELECT * FROM Bar ORDER BY name asc, id desc";
        test(actual, expected);
    }
}
