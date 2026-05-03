use {
    super::{JoinOperatorType, join_operator_plan_with_constraint, join_operator_with_constraint},
    crate::{
        ast::{Expr, Join, Select, TableAlias, TableFactor},
        ast_builder::{
            ExprList, ExprNode, FilterNode, GroupByNode, HashJoinNode, JoinConstraintNode,
            LimitNode, OffsetNode, OrderByExprList, OrderByNode, ProjectNode, QueryNode,
            SelectItemList, SelectNode, TableFactorNode,
            select::{BuildSelect, BuildSelectPlan},
        },
        plan::{
            JoinConstraintPlan, JoinExecutorPlan, JoinOperatorPlan, JoinPlan, SelectPlan,
            TableAliasPlan, TableFactorPlan,
        },
        result::Result,
    },
};

#[derive(Clone, Debug)]
pub(in crate::ast_builder::select) enum PrevNode<'a> {
    Select(SelectNode<'a>),
    Join(Box<JoinNode<'a>>),
    JoinConstraint(Box<JoinConstraintNode<'a>>),
    HashJoin(Box<HashJoinNode<'a>>),
}

impl BuildSelectPlan for PrevNode<'_> {
    fn build_select_plan(self) -> Result<SelectPlan> {
        match self {
            Self::Select(node) => node.build_select_plan(),
            Self::Join(node) => node.build_select_plan(),
            Self::JoinConstraint(node) => node.build_select_plan(),
            Self::HashJoin(node) => node.build_select_plan(),
        }
    }
}

impl BuildSelect for PrevNode<'_> {
    fn build_select(self) -> Result<Select> {
        match self {
            Self::Select(node) => node.build_select(),
            Self::Join(node) => node.build_select(),
            Self::JoinConstraint(node) => node.build_select(),
            Self::HashJoin(node) => node.build_select(),
        }
    }
}

impl<'a> From<SelectNode<'a>> for PrevNode<'a> {
    fn from(node: SelectNode<'a>) -> Self {
        PrevNode::Select(node)
    }
}

impl<'a> From<JoinNode<'a>> for PrevNode<'a> {
    fn from(node: JoinNode<'a>) -> Self {
        PrevNode::Join(Box::new(node))
    }
}

impl<'a> From<JoinConstraintNode<'a>> for PrevNode<'a> {
    fn from(node: JoinConstraintNode<'a>) -> Self {
        PrevNode::JoinConstraint(Box::new(node))
    }
}

impl<'a> From<HashJoinNode<'a>> for PrevNode<'a> {
    fn from(node: HashJoinNode<'a>) -> Self {
        PrevNode::HashJoin(Box::new(node))
    }
}

#[derive(Clone, Debug)]
pub struct JoinNode<'a> {
    prev_node: PrevNode<'a>,
    relation_name: String,
    relation_alias: Option<String>,
    join_operator_type: JoinOperatorType,
}

impl<'a> JoinNode<'a> {
    pub(in crate::ast_builder::select) fn new<N: Into<PrevNode<'a>>>(
        prev_node: N,
        name: String,
        alias: Option<String>,
        join_operator_type: JoinOperatorType,
    ) -> Self {
        Self {
            prev_node: prev_node.into(),
            join_operator_type,
            relation_name: name,
            relation_alias: alias,
        }
    }

    fn relation_plan(&self) -> TableFactorPlan {
        TableFactorPlan::Table {
            name: self.relation_name.clone(),
            alias: self.relation_alias.as_ref().map(|name| TableAliasPlan {
                name: name.clone(),
                columns: vec![],
            }),
            index: None,
        }
    }

    fn relation_ast(&self) -> TableFactor {
        TableFactor::Table {
            name: self.relation_name.clone(),
            alias: self.relation_alias.as_ref().map(|name| TableAlias {
                name: name.clone(),
                columns: vec![],
            }),
            index: None,
        }
    }

    pub fn on<T: Into<ExprNode<'a>>>(self, expr: T) -> JoinConstraintNode<'a> {
        JoinConstraintNode::new(self, expr)
    }

    #[must_use]
    pub fn join(self, table_name: &str) -> JoinNode<'a> {
        JoinNode::new(self, table_name.to_owned(), None, JoinOperatorType::Inner)
    }

    #[must_use]
    pub fn join_as(self, table_name: &str, alias: &str) -> JoinNode<'a> {
        JoinNode::new(
            self,
            table_name.to_owned(),
            Some(alias.to_owned()),
            JoinOperatorType::Inner,
        )
    }

    #[must_use]
    pub fn left_join(self, table_name: &str) -> JoinNode<'a> {
        JoinNode::new(self, table_name.to_owned(), None, JoinOperatorType::Left)
    }

    #[must_use]
    pub fn left_join_as(self, table_name: &str, alias: &str) -> JoinNode<'a> {
        JoinNode::new(
            self,
            table_name.to_owned(),
            Some(alias.to_owned()),
            JoinOperatorType::Left,
        )
    }

    pub fn hash_executor<T: Into<ExprNode<'a>>, U: Into<ExprNode<'a>>>(
        self,
        key_expr: T,
        value_expr: U,
    ) -> HashJoinNode<'a> {
        HashJoinNode::new(self, key_expr, value_expr)
    }

    pub fn project<T: Into<SelectItemList<'a>>>(self, select_items: T) -> ProjectNode<'a> {
        ProjectNode::new(self, select_items)
    }

    pub fn group_by<T: Into<ExprList<'a>>>(self, expr_list: T) -> GroupByNode<'a> {
        GroupByNode::new(self, expr_list)
    }

    pub fn offset<T: Into<ExprNode<'a>>>(self, expr: T) -> OffsetNode<'a> {
        OffsetNode::new(self, expr)
    }

    pub fn limit<T: Into<ExprNode<'a>>>(self, expr: T) -> LimitNode<'a> {
        LimitNode::new(self, expr)
    }

    pub fn filter<T: Into<ExprNode<'a>>>(self, expr: T) -> FilterNode<'a> {
        FilterNode::new(self, expr)
    }

    pub fn order_by<T: Into<OrderByExprList<'a>>>(self, order_by_exprs: T) -> OrderByNode<'a> {
        OrderByNode::new(self, order_by_exprs)
    }

    pub fn alias_as(self, table_alias: &'a str) -> TableFactorNode<'a> {
        QueryNode::JoinNode(self).alias_as(table_alias)
    }

    pub(super) fn build_select_plan_with_constraint(
        self,
        constraint: JoinConstraintPlan,
    ) -> Result<SelectPlan> {
        let relation = self.relation_plan();
        let mut select = self.prev_node.build_select_plan()?;

        select.from.joins.push(JoinPlan {
            relation,
            join_operator: join_operator_plan_with_constraint(self.join_operator_type, constraint),
            join_executor: JoinExecutorPlan::NestedLoop,
        });

        Ok(select)
    }

    pub(super) fn build_select_with_constraint(self, constraint: Expr) -> Result<Select> {
        let relation = self.relation_ast();
        let mut select = self.prev_node.build_select()?;

        select.from.joins.push(Join {
            relation,
            join_operator: join_operator_with_constraint(self.join_operator_type, Some(constraint)),
        });

        Ok(select)
    }

    pub(super) fn build_hash_join_data_plan_with_constraint(
        self,
        constraint: JoinConstraintPlan,
    ) -> Result<(SelectPlan, TableFactorPlan, JoinOperatorPlan)> {
        let relation = self.relation_plan();
        let select_data = self.prev_node.build_select_plan()?;
        let join_operator = join_operator_plan_with_constraint(self.join_operator_type, constraint);

        Ok((select_data, relation, join_operator))
    }
}

impl BuildSelectPlan for JoinNode<'_> {
    fn build_select_plan(self) -> Result<SelectPlan> {
        let relation = self.relation_plan();
        let mut select = self.prev_node.build_select_plan()?;

        select.from.joins.push(JoinPlan {
            relation,
            join_operator: join_operator_plan_with_constraint(
                self.join_operator_type,
                JoinConstraintPlan::None,
            ),
            join_executor: JoinExecutorPlan::NestedLoop,
        });

        Ok(select)
    }
}

impl BuildSelect for JoinNode<'_> {
    fn build_select(self) -> Result<Select> {
        let relation = self.relation_ast();
        let mut select = self.prev_node.build_select()?;

        select.from.joins.push(Join {
            relation,
            join_operator: join_operator_with_constraint(self.join_operator_type, None),
        });

        Ok(select)
    }
}

#[cfg(test)]
mod tests {
    use {
        crate::ast_builder::{Build, table, test},
        pretty_assertions::assert_eq,
    };

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
        test(&actual, expected);

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
        test(&actual, expected);

        // select node -> join node ->  build
        let actual = table("Item").select().join_as("Player", "p").build();
        let expected = "
        SELECT * FROM Item INNER JOIN Player AS p;
        ";
        test(&actual, expected);

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
        test(&actual, expected);

        // select node -> join node -> project node
        let actual = table("Orders")
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
        test(&actual, expected);
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
        test(&actual, expected);

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
        test(&actual, expected);

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
        test(&actual, expected);

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
        test(&actual, expected);
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
        test(&actual, expected);

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
        test(&actual, expected);

        // join - left join
        let actual = table("Foo").select().join("Bar").left_join("Baz").build();
        let expected = "
            SELECT * FROM Foo
            INNER JOIN Bar
            LEFT JOIN Baz
            ";
        test(&actual, expected);

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
        test(&actual, expected);

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
        test(&actual, expected);

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
        test(&actual, expected);

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
        test(&actual, expected);

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
        test(&actual, expected);

        // left join - join
        let actual = table("Foo").select().left_join("Bar").join("Baz").build();
        let expected = "
            SELECT * FROM Foo
            LEFT JOIN Bar
            INNER JOIN Baz
            ";
        test(&actual, expected);

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
        test(&actual, expected);

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
        test(&actual, expected);

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
        test(&actual, expected);

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
        test(&actual, expected);

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
        test(&actual, expected);

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
        test(&actual, expected);

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
        test(&actual, expected);
    }

    #[test]
    fn hash_join() {
        use crate::{
            ast_builder::{SelectItemList, col},
            plan::{
                JoinConstraintPlan, JoinExecutorPlan, JoinOperatorPlan, JoinPlan, ProjectionPlan,
                QueryPlan, SelectPlan, SetExprPlan, StatementPlan, TableAliasPlan, TableFactorPlan,
                TableWithJoinsPlan,
            },
        };

        let gen_expected = |other_join| {
            let join = JoinPlan {
                relation: TableFactorPlan::Table {
                    name: "PlayerItem".to_owned(),
                    alias: None,
                    index: None,
                },
                join_operator: JoinOperatorPlan::Inner(JoinConstraintPlan::None),
                join_executor: JoinExecutorPlan::Hash {
                    key_expr: col("PlayerItem.user_id").build_expr_plan().unwrap(),
                    value_expr: col("Player.id").build_expr_plan().unwrap(),
                    where_clause: None,
                },
            };
            let select = SelectPlan {
                distinct: false,
                projection: ProjectionPlan::SelectItems(
                    SelectItemList::from("*").build_select_items_plan().unwrap(),
                ),
                from: TableWithJoinsPlan {
                    relation: TableFactorPlan::Table {
                        name: "Player".to_owned(),
                        alias: None,
                        index: None,
                    },
                    joins: vec![join, other_join],
                },
                selection: None,
                group_by: Vec::new(),
                having: None,
                aggregate_slots: None,
            };

            Ok(StatementPlan::Query(QueryPlan {
                body: SetExprPlan::Select(Box::new(select)),
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
            let other_join = JoinPlan {
                relation: TableFactorPlan::Table {
                    name: "OtherItem".to_owned(),
                    alias: None,
                    index: None,
                },
                join_operator: JoinOperatorPlan::Inner(JoinConstraintPlan::None),
                join_executor: JoinExecutorPlan::NestedLoop,
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
            let other_join = JoinPlan {
                relation: TableFactorPlan::Table {
                    name: "OtherItem".to_owned(),
                    alias: Some(TableAliasPlan {
                        name: "Ot".to_owned(),
                        columns: Vec::new(),
                    }),
                    index: None,
                },
                join_operator: JoinOperatorPlan::Inner(JoinConstraintPlan::None),
                join_executor: JoinExecutorPlan::NestedLoop,
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
            let other_join = JoinPlan {
                relation: TableFactorPlan::Table {
                    name: "OtherItem".to_owned(),
                    alias: None,
                    index: None,
                },
                join_operator: JoinOperatorPlan::LeftOuter(JoinConstraintPlan::None),
                join_executor: JoinExecutorPlan::NestedLoop,
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
            let other_join = JoinPlan {
                relation: TableFactorPlan::Table {
                    name: "OtherItem".to_owned(),
                    alias: Some(TableAliasPlan {
                        name: "Ot".to_owned(),
                        columns: Vec::new(),
                    }),
                    index: None,
                },
                join_operator: JoinOperatorPlan::LeftOuter(JoinConstraintPlan::None),
                join_executor: JoinExecutorPlan::NestedLoop,
            };

            gen_expected(other_join)
        };
        assert_eq!(actual, expected, "left join with alias");

        let actual = table("App").select().alias_as("Sub").select().build();
        let expected = "SELECT * FROM (SELECT * FROM App) Sub";
        test(&actual, expected);

        // join -> derived subquery
        let actual = table("Foo")
            .select()
            .join("Bar")
            .alias_as("Sub")
            .select()
            .build();
        let expected = "
            SELECT * FROM (
                SELECT * FROM Foo
                INNER JOIN Bar
            ) Sub
            ";
        test(&actual, expected);
    }
}
