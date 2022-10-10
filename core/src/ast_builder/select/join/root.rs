use {
    super::{JoinConstraintData, JoinOperatorType},
    crate::{
        ast::{Join, JoinExecutor, JoinOperator, TableAlias, TableFactor},
        ast_builder::{
            select::{NodeData, Prebuild},
            ExprList, ExprNode, FilterNode, GroupByNode, HashJoinNode, JoinConstraintNode,
            LimitNode, OffsetNode, OrderByExprList, OrderByNode, ProjectNode, SelectItemList,
            SelectNode,
        },
        result::Result,
    },
};

#[derive(Clone)]
pub enum PrevNode {
    Select(SelectNode),
    Join(Box<JoinNode>),
    JoinConstraint(Box<JoinConstraintNode>),
    HashJoin(Box<HashJoinNode>),
}

impl Prebuild for PrevNode {
    fn prebuild(self) -> Result<NodeData> {
        match self {
            Self::Select(node) => node.prebuild(),
            Self::Join(node) => node.prebuild(),
            Self::JoinConstraint(node) => node.prebuild(),
            Self::HashJoin(node) => node.prebuild(),
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

impl From<HashJoinNode> for PrevNode {
    fn from(node: HashJoinNode) -> Self {
        PrevNode::HashJoin(Box::new(node))
    }
}

#[derive(Clone)]
pub struct JoinNode {
    prev_node: PrevNode,
    relation: TableFactor,
    join_operator_type: JoinOperatorType,
}

impl JoinNode {
    pub fn new<N: Into<PrevNode>>(
        prev_node: N,
        name: String,
        alias: Option<String>,
        join_operator_type: JoinOperatorType,
    ) -> Self {
        Self {
            prev_node: prev_node.into(),
            join_operator_type,
            relation: match alias {
                Some(alias) => TableFactor::Table {
                    name,
                    alias: Some(TableAlias {
                        name: alias,
                        columns: vec![],
                    }),
                    index: None,
                },
                None => TableFactor::Table {
                    name,
                    alias: None,
                    index: None,
                },
            },
        }
    }

    pub fn on<T: Into<ExprNode>>(self, expr: T) -> JoinConstraintNode {
        JoinConstraintNode::new(self, expr)
    }

    pub fn join(self, table_name: &str) -> JoinNode {
        JoinNode::new(self, table_name.to_owned(), None, JoinOperatorType::Inner)
    }

    pub fn join_as(self, table_name: &str, alias: &str) -> JoinNode {
        JoinNode::new(
            self,
            table_name.to_owned(),
            Some(alias.to_owned()),
            JoinOperatorType::Inner,
        )
    }

    pub fn left_join(self, table_name: &str) -> JoinNode {
        JoinNode::new(self, table_name.to_owned(), None, JoinOperatorType::Left)
    }

    pub fn left_join_as(self, table_name: &str, alias: &str) -> JoinNode {
        JoinNode::new(
            self,
            table_name.to_owned(),
            Some(alias.to_owned()),
            JoinOperatorType::Left,
        )
    }

    pub fn hash_executor<T: Into<ExprNode>>(self, key_expr: T, value_expr: T) -> HashJoinNode {
        HashJoinNode::new(self, key_expr, value_expr)
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

    pub fn order_by<T: Into<OrderByExprList>>(self, order_by_exprs: T) -> OrderByNode {
        OrderByNode::new(self, order_by_exprs)
    }

    pub fn prebuild_for_constraint(self) -> Result<JoinConstraintData> {
        Ok(JoinConstraintData {
            node_data: self.prev_node.prebuild()?,
            relation: self.relation,
            operator_type: self.join_operator_type,
            executor: JoinExecutor::NestedLoop,
        })
    }

    pub fn prebuild_for_hash_join(self) -> Result<(NodeData, TableFactor, JoinOperator)> {
        let select_data = self.prev_node.prebuild()?;
        let join_operator = JoinOperator::from(self.join_operator_type);

        Ok((select_data, self.relation, join_operator))
    }
}

impl Prebuild for JoinNode {
    fn prebuild(self) -> Result<NodeData> {
        let mut select_data = self.prev_node.prebuild()?;
        select_data.joins.push(Join {
            relation: self.relation,
            join_operator: JoinOperator::from(self.join_operator_type),
            join_executor: JoinExecutor::NestedLoop,
        });
        Ok(select_data)
    }
}

#[cfg(test)]
mod tests {
    use crate::ast_builder::{table, test, Build};

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

    #[test]
    fn hash_join() {
        use crate::{
            ast::{
                Join, JoinConstraint, JoinExecutor, JoinOperator, Query, Select, SetExpr,
                Statement, TableAlias, TableFactor, TableWithJoins,
            },
            ast_builder::{col, SelectItemList},
        };

        let gen_expected = |other_join| {
            let join = Join {
                relation: TableFactor::Table {
                    name: "PlayerItem".to_owned(),
                    alias: None,
                    index: None,
                },
                join_operator: JoinOperator::Inner(JoinConstraint::None),
                join_executor: JoinExecutor::Hash {
                    key_expr: col("PlayerItem.user_id").try_into().unwrap(),
                    value_expr: col("Player.id").try_into().unwrap(),
                    where_clause: None,
                },
            };
            let select = Select {
                projection: SelectItemList::from("*").try_into().unwrap(),
                from: TableWithJoins {
                    relation: TableFactor::Table {
                        name: "Player".to_owned(),
                        alias: None,
                        index: None,
                    },
                    joins: vec![join, other_join],
                },
                selection: None,
                group_by: Vec::new(),
                having: None,
            };

            Ok(Statement::Query(Query {
                body: SetExpr::Select(Box::new(select)),
                order_by: Vec::new(),
                limit: None,
                offset: None,
            }))
        };

        let actual = table("Player")
            .select()
            .join("PlayerItem")
            .hash_executor("PlayerItem.user_id", "Player.id")
            .join("OtherItem")
            .build();
        let expected = {
            let other_join = Join {
                relation: TableFactor::Table {
                    name: "OtherItem".to_owned(),
                    alias: None,
                    index: None,
                },
                join_operator: JoinOperator::Inner(JoinConstraint::None),
                join_executor: JoinExecutor::NestedLoop,
            };

            gen_expected(other_join)
        };
        assert_eq!(actual, expected, "inner join");

        let actual = table("Player")
            .select()
            .join("PlayerItem")
            .hash_executor("PlayerItem.user_id", "Player.id")
            .join_as("OtherItem", "Ot")
            .build();
        let expected = {
            let other_join = Join {
                relation: TableFactor::Table {
                    name: "OtherItem".to_owned(),
                    alias: Some(TableAlias {
                        name: "Ot".to_owned(),
                        columns: Vec::new(),
                    }),
                    index: None,
                },
                join_operator: JoinOperator::Inner(JoinConstraint::None),
                join_executor: JoinExecutor::NestedLoop,
            };

            gen_expected(other_join)
        };
        assert_eq!(actual, expected, "inner join with alias");

        let actual = table("Player")
            .select()
            .join("PlayerItem")
            .hash_executor("PlayerItem.user_id", "Player.id")
            .left_join("OtherItem")
            .build();
        let expected = {
            let other_join = Join {
                relation: TableFactor::Table {
                    name: "OtherItem".to_owned(),
                    alias: None,
                    index: None,
                },
                join_operator: JoinOperator::LeftOuter(JoinConstraint::None),
                join_executor: JoinExecutor::NestedLoop,
            };

            gen_expected(other_join)
        };
        assert_eq!(actual, expected, "left join");

        let actual = table("Player")
            .select()
            .join("PlayerItem")
            .hash_executor("PlayerItem.user_id", "Player.id")
            .left_join_as("OtherItem", "Ot")
            .build();
        let expected = {
            let other_join = Join {
                relation: TableFactor::Table {
                    name: "OtherItem".to_owned(),
                    alias: Some(TableAlias {
                        name: "Ot".to_owned(),
                        columns: Vec::new(),
                    }),
                    index: None,
                },
                join_operator: JoinOperator::LeftOuter(JoinConstraint::None),
                join_executor: JoinExecutor::NestedLoop,
            };

            gen_expected(other_join)
        };
        assert_eq!(actual, expected, "left join with alias");
    }
}
