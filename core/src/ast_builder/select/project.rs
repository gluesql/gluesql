use {
    super::{build_stmt, NodeData, Prebuild},
    crate::{
        ast::Statement,
        ast_builder::{
            GroupByNode, HavingNode, LimitNode, LimitOffsetNode, OffsetLimitNode, OffsetNode,
            SelectItemList, SelectNode,
        },
        result::Result,
    },
};

#[derive(Clone)]
pub enum PrevNode {
    Select(SelectNode),
    GroupBy(GroupByNode),
    Having(HavingNode),
    Limit(LimitNode),
    LimitOffset(LimitOffsetNode),
    Offset(OffsetNode),
    OffsetLimit(OffsetLimitNode),
}

impl Prebuild for PrevNode {
    fn prebuild(self) -> Result<NodeData> {
        match self {
            Self::Select(node) => node.prebuild(),
            Self::GroupBy(node) => node.prebuild(),
            Self::Having(node) => node.prebuild(),
            Self::Limit(node) => node.prebuild(),
            Self::LimitOffset(node) => node.prebuild(),
            Self::Offset(node) => node.prebuild(),
            Self::OffsetLimit(node) => node.prebuild(),
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

impl From<LimitOffsetNode> for PrevNode {
    fn from(node: LimitOffsetNode) -> Self {
        PrevNode::LimitOffset(node)
    }
}

impl From<OffsetNode> for PrevNode {
    fn from(node: OffsetNode) -> Self {
        PrevNode::Offset(node)
    }
}

impl From<OffsetLimitNode> for PrevNode {
    fn from(node: OffsetLimitNode) -> Self {
        PrevNode::OffsetLimit(node)
    }
}

#[derive(Clone)]
pub struct ProjectNode {
    prev_node: PrevNode,
    select_items_list: Vec<SelectItemList>,
}

impl ProjectNode {
    pub fn new<N: Into<PrevNode>, T: Into<SelectItemList>>(prev_node: N, select_items: T) -> Self {
        Self {
            prev_node: prev_node.into(),
            select_items_list: vec![select_items.into()],
        }
    }

    pub fn project<T: Into<SelectItemList>>(mut self, select_items: T) -> Self {
        self.select_items_list.push(select_items.into());

        self
    }

    pub fn build(self) -> Result<Statement> {
        let select_data = self.prebuild()?;

        Ok(build_stmt(select_data))
    }
}

impl Prebuild for ProjectNode {
    fn prebuild(self) -> Result<NodeData> {
        let mut select_data = self.prev_node.prebuild()?;
        select_data.projection = self
            .select_items_list
            .into_iter()
            .map(TryInto::try_into)
            .collect::<Result<Vec<Vec<_>>>>()?
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();

        Ok(select_data)
    }
}

#[cfg(test)]
mod tests {
    use crate::ast_builder::{test, Builder};

    #[test]
    fn project() {
        let actual = Builder::table("Good").select().project("id").build();
        let expected = "SELECT id FROM Good";
        test(actual, expected);

        let actual = Builder::table("Group")
            .select()
            .project("*, Group.*, name")
            .build();
        let expected = "SELECT *, Group.*, name FROM Group";
        test(actual, expected);

        let actual = Builder::table("Foo")
            .select()
            .project("col1, col2")
            .project("col3")
            .project("col4, col5, col6")
            .build();
        let expected = "
            SELECT
                col1, col2, col3,
                col4, col5, col6
            FROM
                Foo
        ";
        test(actual, expected);

        let actual = Builder::table("Aliased")
            .select()
            .project("1 + 1 as col1, col2")
            .build();
        let expected = "SELECT 1 + 1 as col1, col2 FROM Aliased";
        test(actual, expected);
    }

    #[test]
    fn prev_nodes() {
        // Select
        let actual = Builder::table("Foo").select().project("*").build();
        let expected = "SELECT * FROM Foo";
        test(actual, expected);

        // GroupBy
        let actual = Builder::table("Bar")
            .select()
            .group_by("city")
            .project("city, COUNT(name) as num")
            .build();
        let expected = "
            SELECT
              city, COUNT(name) as num
            FROM Bar
            GROUP BY city
        ";
        test(actual, expected);

        // Having
        let actual = Builder::table("Cat")
            .select()
            .filter(r#"type = "cute""#)
            .group_by("age")
            .having("SUM(length) < 1000")
            .project("age")
            .project("SUM(length)")
            .build();
        let expected = r#"
            SELECT age, SUM(length)
            FROM Cat
            WHERE type = "cute"
            GROUP BY age
            HAVING SUM(length) < 1000;
        "#;
        test(actual, expected);

        // Limit
        let actual = Builder::table("Item")
            .select()
            .limit(10)
            .project("*")
            .build();
        let expected = "SELECT * FROM Item LIMIT 10";
        test(actual, expected);

        // LimitOffset
        let actual = Builder::table("Operator")
            .select()
            .limit(100)
            .offset(50)
            .project("name")
            .build();
        let expected = "SELECT name FROM Operator LIMIT 100 OFFSET 50";
        test(actual, expected);

        // Offset
        let actual = Builder::table("Item")
            .select()
            .offset(10)
            .project("*")
            .build();
        let expected = "SELECT * FROM Item OFFSET 10";
        test(actual, expected);

        // OffsetLimit
        let actual = Builder::table("Operator")
            .select()
            .offset(3)
            .limit(10)
            .project("name")
            .build();
        let expected = "SELECT name FROM Operator LIMIT 10 OFFSET 3";
        test(actual, expected);
    }
}
