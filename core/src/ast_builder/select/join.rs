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
pub struct JoinNode {
    prev_node: PrevNode,
}

impl JoinNode {
    pub fn new<N: Into<PrevNode>>(prev_node: N) -> Self {
        Self {
            prev_node: prev_node.into(),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::ast_builder::{col, table, test};

    #[test]
    fn inner_join() {
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

        let actual = table("student")
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
            SELECT s.id, s.name, m.rank, a.attendance
            FROM students AS s
            INNER JOIN marks AS m ON s.id=m.id
            INNER JOIN attendance AS a on m.id=a.id
            WHERE a.attendance>=75;
        ";
        test(actual, expected);

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
    }

    fn left_join() {
        let actual = table("player")
            .select()
            .projection("player.id", "item.id")
            .left_join("item")
            .on("player.id = item.id")
            .build();
        let expected = "
            SELECT p.id, i.id
            FROM Player p
            LEFT JOIN Item i
            ON p.id = i.player_id
        ";
        test(actual, expected);

        let actual = table("Item")
            .select()
            .left_join("Player")
            .on("Item.player_id = Player.id")
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

        let actual = table("Item")
            .select()
            .left_join("Player")
            .on("Item.player_id = Player.id")
            .left_join_as("Player", "p1")
            .on("p1.id = Item.player_id")
            .left_join_as("Player", "p2")
            .on("p2.id = Item.player_id")
            .left_join_as("Player", "p3")
            .on("p3.id = Item.player_id")
            .inner_join_as("Player", "p4")
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
    }
}
