use {
    super::{NodeData, Prebuild},
    crate::{
        ast::{
            Join, JoinConstraint, JoinExecutor, JoinOperator, ObjectName, Statement, TableAlias,
            TableFactor,
        },
        ast_builder::{
            ExprList, ExprNode, FilterNode, GroupByNode, JoinConstraintNode, LimitNode, OffsetNode,
            ProjectNode, SelectItemList, SelectNode,
        },
        result::Result,
    },
};

#[derive(Clone)]
pub enum JoinType {
    Inner,
    Left,
}

#[derive(Clone)]
pub enum PrevNode {
    Select(SelectNode),
    Join(Box<JoinNode>),
    JoinConstraint(Box<JoinConstraintNode>),
}

impl Prebuild for PrevNode {
    fn prebuild(self) -> Result<NodeData> {
        match self {
            Self::Select(node) => node.prebuild(),
            Self::Join(node) => node.prebuild(),
            Self::JoinConstraint(node) => node.prebuild(),
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
        PrevNode::Join(Box::new(node))
    }
}

impl From<JoinConstraintNode> for PrevNode {
    fn from(node: JoinConstraintNode) -> Self {
        PrevNode::JoinConstraint(Box::new(node))
    }
}

#[derive(Clone)]
pub struct JoinNode {
    prev_node: PrevNode,
    relation: TableFactor,
    join_operator: JoinOperator,
    join_operator_type: JoinType,
}

impl JoinNode {
    pub fn new<N: Into<PrevNode>>(
        prev_node: N,
        table_name: String,
        alias: Option<String>,
        join_operator_type: JoinType,
    ) -> Self {
        Self {
            prev_node: prev_node.into(),
            join_operator_type: join_operator_type.clone(),
            relation: match alias {
                Some(alias) => TableFactor::Table {
                    name: ObjectName(vec![table_name]),
                    alias: Some(TableAlias {
                        name: alias,
                        columns: vec![],
                    }),
                    index: None,
                },
                None => TableFactor::Table {
                    name: ObjectName(vec![table_name]),
                    alias: None,
                    index: None,
                },
            },
            join_operator: match join_operator_type {
                JoinType::Inner => JoinOperator::Inner(JoinConstraint::None),
                JoinType::Left => JoinOperator::LeftOuter(JoinConstraint::None),
            },
        }
    }

    pub fn on<T: Into<ExprNode>>(self, expr: T) -> JoinConstraintNode {
        JoinConstraintNode::new(self, expr)
    }

    pub fn join(self, table_name: &str) -> JoinNode {
        JoinNode::new(self, table_name.to_string(), None, JoinType::Inner)
    }

    pub fn join_as(self, table_name: &str, alias: &str) -> JoinNode {
        JoinNode::new(
            self,
            table_name.to_string(),
            Some(alias.to_string()),
            JoinType::Inner,
        )
    }

    pub fn left_join(self, table_name: &str) -> JoinNode {
        JoinNode::new(self, table_name.to_string(), None, JoinType::Left)
    }

    pub fn left_join_as(self, table_name: &str, alias: &str) -> JoinNode {
        JoinNode::new(
            self,
            table_name.to_string(),
            Some(alias.to_string()),
            JoinType::Left,
        )
    }

    pub fn project<T: Into<SelectItemList>>(self, select_items: T) -> ProjectNode {
        ProjectNode::new(self, select_items)
    }

    pub fn group_by<T: Into<ExprList>>(self, expr_list: T) -> GroupByNode {
        GroupByNode::new(self, expr_list)
    }

    pub fn offset<T: Into<ExprNode>>(self, expr: T) -> OffsetNode {
        OffsetNode::new(self, expr)
    }

    pub fn limit<T: Into<ExprNode>>(self, expr: T) -> LimitNode {
        LimitNode::new(self, expr)
    }

    pub fn filter<T: Into<ExprNode>>(self, expr: T) -> FilterNode {
        FilterNode::new(self, expr)
    }

    pub fn build(self) -> Result<Statement> {
        self.prebuild().map(NodeData::build_stmt)
    }

    pub fn prebuild_for_constraint(self) -> Result<(NodeData, TableFactor, JoinType)> {
        let select_data = self.prev_node.prebuild()?;
        Ok((select_data, self.relation, self.join_operator_type))
    }
}

impl Prebuild for JoinNode {
    fn prebuild(self) -> Result<NodeData> {
        let mut select_data = self.prev_node.prebuild()?;
        select_data.joins.push(Join {
            relation: self.relation,
            join_operator: self.join_operator,
            join_executor: JoinExecutor::NestedLoop,
        });
        Ok(select_data)
    }
}
#[cfg(test)]
mod tests {
    use crate::ast_builder::{table, test};

    #[test]
    fn inner_join() {
        // select node -> join node -> join constraint node
        let actual = table("Item")
            .select()
            .join_as("Player", "p")
            .on("p.id = Item.player_id")
            .filter("p.id = 1")
            .build();
        let expected = "
        SELECT * FROM Item INNER JOIN Player AS p ON p.id = Item.player_id WHERE p.id = 1;
        ";
        test(actual, expected);

        // select node -> join node ->  join constraint node
        let actual = table("Item")
            .select()
            .join_as("Player", "p")
            .on("p.id = Item.player_id")
            .filter("p.id = 1")
            .project(vec!["p.id", "p.name", "Item.id"])
            .build();
        let expected = "
        SELECT p.id, p.name, Item.id FROM Item INNER JOIN Player AS p ON p.id = Item.player_id WHERE p.id = 1;
        ";
        test(actual, expected);

        // select node -> join node ->  build
        let actual = table("Item").select().join_as("Player", "p").build();
        let expected = "
        SELECT * FROM Item INNER JOIN Player AS p;
        ";
        test(actual, expected);

        // join node -> join constraint node -> join node -> join constraint node
        let actual = table("students")
            .select()
            .join("marks")
            .on("students.id = marks.id")
            .join("attendance")
            .on("marks.id = attendance.id")
            .filter("attendance.attendance >= 75")
            .project(vec![
                "students.id",
                "students.name",
                "marks.rank",
                "attendance.attendance",
            ])
            .build();
        let expected = "
            SELECT students.id, students.name, marks.rank, attendance.attendance
            FROM students
            INNER JOIN marks ON students.id=marks.id
            INNER JOIN attendance on marks.id=attendance.id
            WHERE attendance.attendance >= 75;
        ";
        test(actual, expected);

        // select node -> join node -> project node
        let acutal = table("Orders")
            .select()
            .join("Customers")
            .project(vec![
                "Orders.OrderID",
                "Customers.CustomerName",
                "Orders.OrderDate",
            ])
            .build();
        let expected = "
            SELECT Orders.OrderID, Customers.CustomerName, Orders.OrderDate 
            FROM Orders INNER JOIN Customers
        ";
        test(acutal, expected);
    }

    #[test]
    fn left_join() {
        // select node -> left join node -> join constraint node
        let actual = table("player")
            .select()
            .left_join("item")
            .on("player.id = item.id")
            .project(vec!["player.id", "item.id"])
            .build();
        let expected = "
            SELECT player.id, item.id
            FROM player
            LEFT JOIN item
            ON player.id = item.id
        ";
        test(actual, expected);

        // select node -> left join node -> join constraint node -> left join node
        let actual = table("Item")
            .select()
            .left_join("Player")
            .on("Player.id = Item.player_id")
            .left_join_as("Player", "p1")
            .on("p1.id = Item.player_id")
            .left_join_as("Player", "p2")
            .on("p2.id = Item.player_id")
            .left_join_as("Player", "p3")
            .on("p3.id = Item.player_id")
            .left_join_as("Player", "p4")
            .on("p4.id = Item.player_id")
            .left_join_as("Player", "p5")
            .on("p5.id = Item.player_id")
            .left_join_as("Player", "p6")
            .on("p6.id = Item.player_id")
            .left_join_as("Player", "p7")
            .on("p7.id = Item.player_id")
            .left_join_as("Player", "p8")
            .on("p8.id = Item.player_id")
            .left_join_as("Player", "p9")
            .on("p9.id = Item.player_id")
            .filter("Player.id = 1")
            .build();
        let expected = "
            SELECT * FROM Item
            LEFT JOIN Player ON Player.id = Item.player_id
            LEFT JOIN Player p1 ON p1.id = Item.player_id
            LEFT JOIN Player p2 ON p2.id = Item.player_id
            LEFT JOIN Player p3 ON p3.id = Item.player_id
            LEFT JOIN Player p4 ON p4.id = Item.player_id
            LEFT JOIN Player p5 ON p5.id = Item.player_id
            LEFT JOIN Player p6 ON p6.id = Item.player_id
            LEFT JOIN Player p7 ON p7.id = Item.player_id
            LEFT JOIN Player p8 ON p8.id = Item.player_id
            LEFT JOIN Player p9 ON p9.id = Item.player_id
            WHERE Player.id = 1;
        ";
        test(actual, expected);

        // select node -> left join node -> join constraint node -> left join node
        let actual = table("Item")
            .select()
            .left_join("Player")
            .on("Player.id = Item.player_id")
            .left_join("Player")
            .on("p1.id = Item.player_id")
            .build();
        let expected = "
            SELECT * FROM Item
            LEFT JOIN Player ON Player.id = Item.player_id
            LEFT JOIN Player ON p1.id = Item.player_id";
        test(actual, expected);

        let actual = table("Item")
            .select()
            .left_join("Player")
            .on("Player.id = Item.player_id")
            .left_join_as("Player", "p1")
            .on("p1.id = Item.player_id")
            .left_join_as("Player", "p2")
            .on("p2.id = Item.player_id")
            .left_join_as("Player", "p3")
            .on("p3.id = Item.player_id")
            .join_as("Player", "p4")
            .on("p4.id = Item.player_id AND Item.id > 101")
            .filter("Player.id = 1")
            .build();
        let expected = "
            SELECT * FROM Item
            LEFT JOIN Player ON Player.id = Item.player_id
            LEFT JOIN Player p1 ON p1.id = Item.player_id
            LEFT JOIN Player p2 ON p2.id = Item.player_id
            LEFT JOIN Player p3 ON p3.id = Item.player_id
            INNER JOIN Player p4 ON p4.id = Item.player_id AND Item.id > 101
            WHERE Player.id = 1;
        ";
        test(actual, expected);
    }

    #[test]
    fn join_join() {
        // join - join
        let actual = table("Foo").select().join("Bar").join("Baz").build();
        let expected = "
            SELECT * FROM Foo
            INNER JOIN Bar
            INNER JOIN Baz
            ";
        test(actual, expected);

        // join - join as
        let actual = table("Foo")
            .select()
            .join("Bar")
            .join_as("Baz", "B")
            .build();
        let expected = "
            SELECT * FROM Foo
            INNER JOIN Bar
            INNER JOIN Baz B
            ";
        test(actual, expected);

        // join - left join
        let actual = table("Foo").select().join("Bar").left_join("Baz").build();
        let expected = "
            SELECT * FROM Foo
            INNER JOIN Bar
            LEFT JOIN Baz
            ";
        test(actual, expected);

        // join - left join as
        let actual = table("Foo")
            .select()
            .join("Bar")
            .left_join_as("Baz", "B")
            .build();
        let expected = "
            SELECT * FROM Foo
            INNER JOIN Bar
            LEFT JOIN Baz B
            ";
        test(actual, expected);

        // join as - join
        let actual = table("Foo")
            .select()
            .join_as("Bar", "B")
            .join("Baz")
            .build();
        let expected = "
            SELECT * FROM Foo
            INNER JOIN Bar B
            INNER JOIN Baz
            ";
        test(actual, expected);

        // join as - join as
        let actual = table("Foo")
            .select()
            .join_as("Bar", "B")
            .join_as("Baz", "C")
            .build();
        let expected = "
            SELECT * FROM Foo
            INNER JOIN Bar B
            INNER JOIN Baz C
            ";
        test(actual, expected);

        // join as - left join
        let actual = table("Foo")
            .select()
            .join_as("Bar", "B")
            .left_join("Baz")
            .build();
        let expected = "
            SELECT * FROM Foo
            INNER JOIN Bar B
            LEFT JOIN Baz
            ";
        test(actual, expected);

        // join as - left join as
        let actual = table("Foo")
            .select()
            .join_as("Bar", "B")
            .left_join_as("Baz", "C")
            .build();
        let expected = "
            SELECT * FROM Foo
            INNER JOIN Bar B
            LEFT JOIN Baz C
            ";
        test(actual, expected);

        // left join - join
        let actual = table("Foo").select().left_join("Bar").join("Baz").build();
        let expected = "
            SELECT * FROM Foo
            LEFT JOIN Bar
            INNER JOIN Baz
            ";
        test(actual, expected);

        // left join - join as
        let actual = table("Foo")
            .select()
            .left_join("Bar")
            .join_as("Baz", "B")
            .build();
        let expected = "
            SELECT * FROM Foo
            LEFT JOIN Bar
            INNER JOIN Baz B
            ";
        test(actual, expected);

        // left join - left join
        let actual = table("Foo")
            .select()
            .left_join("Bar")
            .left_join("Baz")
            .build();
        let expected = "
            SELECT * FROM Foo
            LEFT JOIN Bar
            LEFT JOIN Baz
            ";
        test(actual, expected);

        // left join - left join as
        let actual = table("Foo")
            .select()
            .left_join("Bar")
            .left_join_as("Baz", "B")
            .build();
        let expected = "
            SELECT * FROM Foo
            LEFT JOIN Bar
            LEFT JOIN Baz B
            ";
        test(actual, expected);

        // left join as - join
        let actual = table("Foo")
            .select()
            .left_join_as("Bar", "B")
            .join("Baz")
            .build();
        let expected = "
            SELECT * FROM Foo
            LEFT JOIN Bar B
            INNER JOIN Baz
            ";
        test(actual, expected);

        // left join as - join as
        let actual = table("Foo")
            .select()
            .left_join_as("Bar", "B")
            .join_as("Baz", "C")
            .build();
        let expected = "
            SELECT * FROM Foo
            LEFT JOIN Bar B
            INNER JOIN Baz C
            ";
        test(actual, expected);

        // left join as - left join
        let actual = table("Foo")
            .select()
            .left_join_as("Bar", "B")
            .left_join("Baz")
            .build();
        let expected = "
            SELECT * FROM Foo
            LEFT JOIN Bar B
            LEFT JOIN Baz
            ";
        test(actual, expected);

        // left join as - left join as
        let actual = table("Foo")
            .select()
            .left_join_as("Bar", "B")
            .left_join_as("Baz", "C")
            .build();
        let expected = "
            SELECT * FROM Foo
            LEFT JOIN Bar B
            LEFT JOIN Baz C
            ";
        test(actual, expected);
    }
}
