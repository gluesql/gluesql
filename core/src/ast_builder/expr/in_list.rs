use {
    super::ExprNode,
    crate::ast_builder::{
        GroupByNode, HashJoinNode, HavingNode, JoinConstraintNode, JoinNode, LimitNode,
        LimitOffsetNode, OffsetLimitNode, OffsetNode, OrderByNode, ProjectNode, QueryNode,
        SelectNode,
    },
};

#[derive(Clone)]
pub enum InListNode {
    InList(Vec<ExprNode>),
    Query(Box<QueryNode>),
    Text(String),
}

impl From<Vec<ExprNode>> for InListNode {
    fn from(list: Vec<ExprNode>) -> Self {
        InListNode::InList(list)
    }
}

impl From<&str> for InListNode {
    fn from(query: &str) -> Self {
        InListNode::Text(query.to_owned())
    }
}

impl From<QueryNode> for InListNode {
    fn from(node: QueryNode) -> Self {
        InListNode::Query(Box::new(node))
    }
}

macro_rules! impl_from_select_nodes {
    ($type: path) => {
        impl From<$type> for InListNode {
            fn from(list: $type) -> Self {
                InListNode::Query(Box::new(list.into()))
            }
        }
    };
}

impl_from_select_nodes!(SelectNode);
impl_from_select_nodes!(JoinNode);
impl_from_select_nodes!(JoinConstraintNode);
impl_from_select_nodes!(HashJoinNode);
impl_from_select_nodes!(GroupByNode);
impl_from_select_nodes!(HavingNode);
impl_from_select_nodes!(LimitNode);
impl_from_select_nodes!(LimitOffsetNode);
impl_from_select_nodes!(OffsetNode);
impl_from_select_nodes!(OffsetLimitNode);
impl_from_select_nodes!(ProjectNode);
impl_from_select_nodes!(OrderByNode);

impl ExprNode {
    pub fn in_list<T: Into<InListNode>>(self, value: T) -> Self {
        Self::InList {
            expr: Box::new(self),
            list: Box::new(value.into()),
            negated: false,
        }
    }

    pub fn not_in_list<T: Into<InListNode>>(self, value: T) -> Self {
        Self::InList {
            expr: Box::new(self),
            list: Box::new(value.into()),
            negated: true,
        }
    }
}

#[cfg(test)]
mod test {
    use crate::{
        ast::{
            Expr, Join, JoinConstraint, JoinExecutor, JoinOperator, Query, Select, SetExpr,
            TableFactor, TableWithJoins,
        },
        ast_builder::{col, table, test_expr, text, QueryNode, SelectItemList},
    };

    #[test]
    fn in_list() {
        let actual = col("id").in_list(vec![text("a"), text("b"), text("c")]);
        let expected = "id IN ('a', 'b', 'c')";
        test_expr(actual, expected);

        let actual = col("id").not_in_list("opt1, opt2, opt3");
        let expected = "id NOT IN (opt1, opt2, opt3)";
        test_expr(actual, expected);
    }

    #[test]
    fn from_nodes() {
        // from Vec<ExprNode>
        let actual = col("id").not_in_list(vec![text("a"), text("b"), text("c")]);
        let expected = "id NOT IN ('a', 'b', 'c')";
        test_expr(actual, expected);

        // from &str
        let actual = col("id").in_list("1, 2, 3, 4, 5");
        let expected = "id IN (1, 2, 3, 4, 5)";
        test_expr(actual, expected);

        let actual = col("id").in_list("SELECT id FROM FOO");
        let expected = "id IN (SELECT id FROM FOO)";
        test_expr(actual, expected);

        // from QueryNode
        let query_node = QueryNode::from("SELECT name FROM ItemList");
        let actual = col("id").in_list(query_node);
        let expected = "id IN (SELECT name FROM ItemList)";
        test_expr(actual, expected);

        // from SelectNode
        let actual = col("id").in_list(table("FOO").select());
        let expected = "id IN (SELECT * FROM FOO)";
        test_expr(actual, expected);

        // from JoinNode
        let actual = col("id").in_list(table("Bar").select().join("Foo"));
        let expected = "id IN (SELECT * FROM Bar JOIN Foo)";
        test_expr(actual, expected);

        // from JoinConstraintNode
        let actual = col("id").in_list(table("Bar").select().join("Foo").on("Foo.id = Bar.foo_id"));
        let expected = "id IN (SELECT * FROM Bar JOIN Foo ON Foo.id = Bar.foo_id)";
        test_expr(actual, expected);

        // from HashJoinNode
        let actual = col("id").in_list(
            table("Player")
                .select()
                .join("PlayerItem")
                .hash_executor("PlayerItem.user_id", "Player.id"),
        );
        let expected = {
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
                    joins: vec![join],
                },
                selection: None,
                group_by: Vec::new(),
                having: None,
            };

            let query = Query {
                body: SetExpr::Select(Box::new(select)),
                order_by: Vec::new(),
                limit: None,
                offset: None,
            };

            Expr::InSubquery {
                expr: Box::new(Expr::Identifier("id".to_owned())),
                subquery: Box::new(query),
                negated: false,
            }
        };
        assert_eq!(Expr::try_from(actual).unwrap(), expected);

        // from GroupByNode
        let actual = col("id").not_in_list(
            table("Bar")
                .select()
                .filter(col("id").is_null())
                .group_by("id, (a + name)"),
        );
        let expected = "id NOT IN (SELECT * FROM Bar WHERE id IS NULL GROUP BY id, (a + name))";
        test_expr(actual, expected);

        // from HavingNode
        let actual = col("id").in_list(
            table("Bar")
                .select()
                .filter("id IS NULL")
                .group_by("id, (a + name)")
                .having("COUNT(id) > 10"),
        );
        let expected = "
            id IN (
                SELECT * FROM Bar
                WHERE id IS NULL
                GROUP BY id, (a + name)
                HAVING COUNT(id) > 10
            )
        ";
        test_expr(actual, expected);

        // from LimitNode
        let actual = col("id").in_list(table("FOO").select().filter("id IS NULL").limit(10));
        let expected = "id IN (SELECT * FROM FOO WHERE id IS NULL LIMIT 10)";
        test_expr(actual, expected);

        // from LimitOffsetNode
        let actual = col("id").in_list(
            table("World")
                .select()
                .filter("id > 2")
                .limit(100)
                .offset(3),
        );
        let expected = "id IN (SELECT * FROM World WHERE id > 2 OFFSET 3 LIMIT 100)";
        test_expr(actual, expected);

        // from OffsetNode
        let actual = col("id").not_in_list(table("Hello").select().offset(10));
        let expected = "id NOT IN (SELECT * FROM Hello OFFSET 10)";
        test_expr(actual, expected);

        // from OffsetLimitNode
        let actual = col("id").in_list(table("Bar").select().offset(1).limit(3));
        let expected = "id IN (SELECT * FROM Bar OFFSET 1 LIMIT 3)";
        test_expr(actual, expected);

        // from ProjectNode
        let actual = col("name").in_list(table("Item").select().project("name"));
        let expected = "name IN (SELECT name FROM Item)";
        test_expr(actual, expected);

        // from OrderByNode
        let actual = col("id").in_list(table("Item").select().order_by("score ASC"));
        let expected = "id IN (SELECT * FROM Item ORDER BY score ASC)";
        test_expr(actual, expected);
    }
}
