use {
    super::ExprNode,
    crate::query_builder::{
        DistinctNode, FilterNode, GroupByNode, HashJoinNode, HavingNode, JoinConstraintNode,
        JoinNode, LimitNode, OffsetLimitNode, OffsetNode, ProjectNode, QueryNode, SelectNode,
        SelectOrderByNode, ValuesOrderByNode,
    },
};

#[derive(Clone, Debug)]
pub enum InListNode<'a> {
    InList(Vec<ExprNode<'a>>),
    Query(Box<QueryNode<'a>>),
    Text(String),
}

impl<'a> From<Vec<ExprNode<'a>>> for InListNode<'a> {
    fn from(list: Vec<ExprNode<'a>>) -> Self {
        InListNode::InList(list)
    }
}

impl From<&str> for InListNode<'_> {
    fn from(query: &str) -> Self {
        InListNode::Text(query.to_owned())
    }
}

impl<'a> From<QueryNode<'a>> for InListNode<'a> {
    fn from(node: QueryNode<'a>) -> Self {
        InListNode::Query(Box::new(node))
    }
}

macro_rules! impl_from_select_nodes {
    ($type: path) => {
        impl<'a> From<$type> for InListNode<'a> {
            fn from(list: $type) -> Self {
                InListNode::Query(Box::new(list.into()))
            }
        }
    };
}

impl_from_select_nodes!(SelectNode<'a>);
impl_from_select_nodes!(JoinNode<'a>);
impl_from_select_nodes!(JoinConstraintNode<'a>);
impl_from_select_nodes!(HashJoinNode<'a>);
impl_from_select_nodes!(GroupByNode<'a>);
impl_from_select_nodes!(HavingNode<'a>);
impl_from_select_nodes!(FilterNode<'a>);
impl_from_select_nodes!(LimitNode<'a>);
impl_from_select_nodes!(OffsetNode<'a>);
impl_from_select_nodes!(OffsetLimitNode<'a>);
impl_from_select_nodes!(ProjectNode<'a>);
impl_from_select_nodes!(SelectOrderByNode<'a>);
impl_from_select_nodes!(ValuesOrderByNode<'a>);
impl_from_select_nodes!(DistinctNode<'a>);

impl<'a> ExprNode<'a> {
    #[must_use]
    pub fn in_list<T: Into<InListNode<'a>>>(self, value: T) -> Self {
        Self::InList {
            expr: Box::new(self),
            list: Box::new(value.into()),
            negated: false,
        }
    }

    #[must_use]
    pub fn not_in_list<T: Into<InListNode<'a>>>(self, value: T) -> Self {
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
        plan::{
            ExprPlan, JoinConstraintPlan, JoinExecutorPlan, JoinInputPlan, JoinOperatorPlan,
            JoinPlan, ProjectInputPlan, ProjectPlan, ProjectionPlan, QueryPlan, SourcePlan,
            TableAccessPlan, TableSourcePlan,
        },
        query_builder::{QueryNode, SelectItemList, col, table, test_expr, text, values},
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

        // from DistinctNode
        let actual = col("id").in_list(table("FOO").select().project("id").distinct());
        let expected = "id IN (SELECT DISTINCT id FROM FOO)";
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
            let join = JoinPlan {
                input: JoinInputPlan::Source(SourcePlan::Table(TableSourcePlan {
                    name: "Player".to_owned(),
                    alias: None,
                    access: TableAccessPlan::FullScan,
                })),
                right: SourcePlan::Table(TableSourcePlan {
                    name: "PlayerItem".to_owned(),
                    alias: None,
                    access: TableAccessPlan::FullScan,
                }),
                join_operator: JoinOperatorPlan::Inner(JoinConstraintPlan::None),
                join_executor: JoinExecutorPlan::Hash {
                    key_expr: col("PlayerItem.user_id").build_expr_plan().unwrap(),
                    value_expr: col("Player.id").build_expr_plan().unwrap(),
                    where_clause: None,
                },
            };
            let query = QueryPlan::Project(ProjectPlan {
                input: ProjectInputPlan::Join(Box::new(join)),
                projection: ProjectionPlan::SelectItems(
                    SelectItemList::from("*").build_select_items_plan().unwrap(),
                ),
            });

            ExprPlan::InSubquery {
                expr: Box::new(ExprPlan::Identifier("id".to_owned())),
                subquery: Box::new(query),
                negated: false,
            }
        };
        assert_eq!(actual.build_expr_plan().unwrap(), expected);

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

        // from FilterNode
        let actual = col("id").in_list(table("Bar").select().filter("num > 10"));
        let expected = "id IN (SELECT * FROM Bar WHERE num > 10)";
        test_expr(actual, expected);

        // from LimitNode
        let actual = col("id").in_list(table("FOO").select().filter("id IS NULL").limit(10));
        let expected = "id IN (SELECT * FROM FOO WHERE id IS NULL LIMIT 10)";
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

        // from SelectOrderByNode
        let actual = col("id").in_list(table("Item").select().order_by("score ASC"));
        let expected = "id IN (SELECT * FROM Item ORDER BY score ASC)";
        test_expr(actual, expected);

        let actual = col("id")
            .in_list(values(vec!["1", "2"]).order_by("column1 DESC"))
            .build_expr_plan()
            .unwrap();
        assert!(matches!(
            actual,
            ExprPlan::InSubquery {
                subquery,
                ..
            } if matches!(*subquery, QueryPlan::ValuesOrderBy(_))
        ));
    }
}
