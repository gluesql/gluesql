use crate::{
    ast::Select,
    plan::{
        AggregationInputPlan, FilterInputPlan, JoinConditionInputPlan, JoinConditionPlan,
        LeftOuterJoinInputPlan, LeftOuterJoinPlan, ProjectInputPlan,
    },
    query_builder::{
        DistinctNode, ExprList, ExprNode, FilterNode, GroupByNode, HavingNode,
        InnerNestedLoopJoinNode, LeftOuterHashJoinNode, LeftOuterNestedLoopJoinNode, LimitNode,
        OffsetNode, OrderByExprList, ProjectNode, QueryBuilderError, QueryNode, SelectItemList,
        SelectOrderByNode, SourceNode,
        select::{
            BuildAggregationInputPlan, BuildFilterInputPlan, BuildProjectInputPlan, BuildSelect,
        },
    },
    result::Result,
};

#[derive(Clone, Debug)]
pub(super) enum PrevNode<'a> {
    NestedLoop(Box<LeftOuterNestedLoopJoinNode<'a>>),
    Hash(Box<LeftOuterHashJoinNode<'a>>),
}

impl<'a> From<LeftOuterNestedLoopJoinNode<'a>> for PrevNode<'a> {
    fn from(node: LeftOuterNestedLoopJoinNode<'a>) -> Self {
        Self::NestedLoop(Box::new(node))
    }
}

impl<'a> From<LeftOuterHashJoinNode<'a>> for PrevNode<'a> {
    fn from(node: LeftOuterHashJoinNode<'a>) -> Self {
        Self::Hash(Box::new(node))
    }
}

#[derive(Clone, Debug)]
pub struct LeftOuterJoinConditionNode<'a> {
    prev_node: PrevNode<'a>,
    expr: ExprNode<'a>,
}

impl<'a> LeftOuterJoinConditionNode<'a> {
    pub(super) fn new<N: Into<PrevNode<'a>>, T: Into<ExprNode<'a>>>(prev_node: N, expr: T) -> Self {
        Self {
            prev_node: prev_node.into(),
            expr: expr.into(),
        }
    }

    pub(super) fn build_left_outer_join_plan(self) -> Result<LeftOuterJoinPlan> {
        let input = match self.prev_node {
            PrevNode::NestedLoop(node) => node
                .build_nested_loop_join_plan()
                .map(JoinConditionInputPlan::NestedLoop)?,
            PrevNode::Hash(node) => node
                .build_hash_join_plan()
                .map(JoinConditionInputPlan::Hash)?,
        };
        let condition = JoinConditionPlan {
            input,
            expr: self.expr.build_expr_plan()?,
        };

        Ok(LeftOuterJoinPlan {
            input: LeftOuterJoinInputPlan::Condition(condition),
        })
    }

    pub fn join(self, table_name: &str) -> InnerNestedLoopJoinNode<'a> {
        InnerNestedLoopJoinNode::new(self, table_name.to_owned(), None)
    }

    pub fn join_as(self, table_name: &str, alias: &str) -> InnerNestedLoopJoinNode<'a> {
        InnerNestedLoopJoinNode::new(self, table_name.to_owned(), Some(alias.to_owned()))
    }

    pub fn left_join(self, table_name: &str) -> LeftOuterNestedLoopJoinNode<'a> {
        LeftOuterNestedLoopJoinNode::new(self, table_name.to_owned(), None)
    }

    pub fn left_join_as(self, table_name: &str, alias: &str) -> LeftOuterNestedLoopJoinNode<'a> {
        LeftOuterNestedLoopJoinNode::new(self, table_name.to_owned(), Some(alias.to_owned()))
    }

    pub fn project<T: Into<SelectItemList<'a>>>(self, select_items: T) -> ProjectNode<'a> {
        ProjectNode::new(self, select_items)
    }

    pub fn group_by<T: Into<ExprList<'a>>>(self, expr_list: T) -> GroupByNode<'a> {
        GroupByNode::new(self, expr_list)
    }

    pub fn having<T: Into<ExprNode<'a>>>(self, expr: T) -> HavingNode<'a> {
        HavingNode::new(self, expr)
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

    pub fn order_by<T: Into<OrderByExprList<'a>>>(
        self,
        order_by_exprs: T,
    ) -> SelectOrderByNode<'a> {
        SelectOrderByNode::new(self, order_by_exprs)
    }

    pub fn distinct(self) -> DistinctNode<'a> {
        DistinctNode::new(self)
    }

    pub fn alias_as(self, table_alias: &'a str) -> SourceNode<'a> {
        QueryNode::LeftOuterJoinConditionNode(self).alias_as(table_alias)
    }
}

impl BuildFilterInputPlan for LeftOuterJoinConditionNode<'_> {
    fn build_filter_input_plan(self) -> Result<FilterInputPlan> {
        self.build_left_outer_join_plan()
            .map(Box::new)
            .map(FilterInputPlan::LeftOuterJoin)
    }
}

impl BuildAggregationInputPlan for LeftOuterJoinConditionNode<'_> {
    fn build_aggregation_input_plan(self) -> Result<AggregationInputPlan> {
        self.build_left_outer_join_plan()
            .map(Box::new)
            .map(AggregationInputPlan::LeftOuterJoin)
    }
}

impl BuildProjectInputPlan for LeftOuterJoinConditionNode<'_> {
    fn build_project_input_plan(self) -> Result<ProjectInputPlan> {
        self.build_left_outer_join_plan()
            .map(Box::new)
            .map(ProjectInputPlan::LeftOuterJoin)
    }
}

impl BuildSelect for LeftOuterJoinConditionNode<'_> {
    fn build_select(self) -> Result<Select> {
        let expr = self.expr.build_expr()?;

        match self.prev_node {
            PrevNode::NestedLoop(node) => node.build_select_with_condition(Some(expr)),
            PrevNode::Hash(_) => Err(QueryBuilderError::HashJoinExecutorRequiresPlan.into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use {
        crate::{
            plan::{
                HashJoinInputPlan, HashJoinPlan, JoinConditionInputPlan, JoinConditionPlan,
                LeftOuterJoinInputPlan, LeftOuterJoinPlan, NestedLoopJoinInputPlan,
                NestedLoopJoinPlan, SourcePlan, TableAccessPlan, TableSourcePlan,
            },
            query_builder::{
                QueryBuilderError, col, expr, select::BuildQuery, table, test_query_builder,
            },
            result::Error,
        },
        pretty_assertions::assert_eq,
    };

    #[test]
    fn plans() {
        let actual = table("A")
            .select()
            .left_join("B")
            .on("A.id = B.id")
            .build_left_outer_join_plan()
            .unwrap();
        let expected = LeftOuterJoinPlan {
            input: LeftOuterJoinInputPlan::Condition(JoinConditionPlan {
                input: JoinConditionInputPlan::NestedLoop(NestedLoopJoinPlan {
                    input: NestedLoopJoinInputPlan::Source(SourcePlan::Table(TableSourcePlan {
                        name: "A".to_owned(),
                        alias: None,
                        access: TableAccessPlan::FullScan,
                    })),
                    right: SourcePlan::Table(TableSourcePlan {
                        name: "B".to_owned(),
                        alias: None,
                        access: TableAccessPlan::FullScan,
                    }),
                }),
                expr: expr("A.id = B.id").build_expr_plan().unwrap(),
            }),
        };
        assert_eq!(actual, expected);

        let actual = table("A")
            .select()
            .left_join("B")
            .hash_executor("B.id", "A.id")
            .on("A.active")
            .build_left_outer_join_plan()
            .unwrap();
        let expected = LeftOuterJoinPlan {
            input: LeftOuterJoinInputPlan::Condition(JoinConditionPlan {
                input: JoinConditionInputPlan::Hash(HashJoinPlan {
                    input: HashJoinInputPlan::Source(SourcePlan::Table(TableSourcePlan {
                        name: "A".to_owned(),
                        alias: None,
                        access: TableAccessPlan::FullScan,
                    })),
                    right: SourcePlan::Table(TableSourcePlan {
                        name: "B".to_owned(),
                        alias: None,
                        access: TableAccessPlan::FullScan,
                    }),
                    input_key: col("A.id").build_expr_plan().unwrap(),
                    right_key: col("B.id").build_expr_plan().unwrap(),
                    right_filter: None,
                }),
                expr: col("A.active").build_expr_plan().unwrap(),
            }),
        };
        assert_eq!(actual, expected);
    }

    #[test]
    fn successors() {
        let actual = table("A")
            .select()
            .left_join("B")
            .on("A.id = B.id")
            .join("C");
        let expected = "SELECT * FROM A LEFT JOIN B ON A.id = B.id JOIN C";
        test_query_builder(actual, expected);

        let actual = table("A")
            .select()
            .left_join("B")
            .on("A.id = B.id")
            .join_as("C", "c");
        let expected = "SELECT * FROM A LEFT JOIN B ON A.id = B.id JOIN C AS c";
        test_query_builder(actual, expected);

        let actual = table("A")
            .select()
            .left_join("B")
            .on("A.id = B.id")
            .left_join("C");
        let expected = "SELECT * FROM A LEFT JOIN B ON A.id = B.id LEFT JOIN C";
        test_query_builder(actual, expected);

        let actual = table("A")
            .select()
            .left_join("B")
            .on("A.id = B.id")
            .left_join_as("C", "c");
        let expected = "SELECT * FROM A LEFT JOIN B ON A.id = B.id LEFT JOIN C AS c";
        test_query_builder(actual, expected);

        let actual = table("A")
            .select()
            .left_join("B")
            .on("A.id = B.id")
            .project("A.id");
        let expected = "SELECT A.id FROM A LEFT JOIN B ON A.id = B.id";
        test_query_builder(actual, expected);

        let actual = table("A")
            .select()
            .left_join("B")
            .on("A.id = B.id")
            .group_by("A.id");
        let expected = "SELECT * FROM A LEFT JOIN B ON A.id = B.id GROUP BY A.id";
        test_query_builder(actual, expected);

        let actual = table("A")
            .select()
            .left_join("B")
            .on("A.id = B.id")
            .having("TRUE");
        let expected = "SELECT * FROM A LEFT JOIN B ON A.id = B.id HAVING TRUE";
        test_query_builder(actual, expected);

        let actual = table("A")
            .select()
            .left_join("B")
            .on("A.id = B.id")
            .offset(1);
        let expected = "SELECT * FROM A LEFT JOIN B ON A.id = B.id OFFSET 1";
        test_query_builder(actual, expected);

        let actual = table("A")
            .select()
            .left_join("B")
            .on("A.id = B.id")
            .limit(1);
        let expected = "SELECT * FROM A LEFT JOIN B ON A.id = B.id LIMIT 1";
        test_query_builder(actual, expected);

        let actual = table("A")
            .select()
            .left_join("B")
            .on("A.id = B.id")
            .filter("A.active");
        let expected = "SELECT * FROM A LEFT JOIN B ON A.id = B.id WHERE A.active";
        test_query_builder(actual, expected);

        let actual = table("A")
            .select()
            .left_join("B")
            .on("A.id = B.id")
            .order_by("A.id");
        let expected = "SELECT * FROM A LEFT JOIN B ON A.id = B.id ORDER BY A.id";
        test_query_builder(actual, expected);

        let actual = table("A")
            .select()
            .left_join("B")
            .on("A.id = B.id")
            .distinct();
        let expected = "SELECT DISTINCT * FROM A LEFT JOIN B ON A.id = B.id";
        test_query_builder(actual, expected);

        let actual = table("A")
            .select()
            .left_join("B")
            .on("A.id = B.id")
            .alias_as("Joined")
            .select();
        let expected = "SELECT * FROM (SELECT * FROM A LEFT JOIN B ON A.id = B.id) Joined";
        test_query_builder(actual, expected);
    }

    #[test]
    fn hash_condition_requires_plan_for_ast() {
        let actual = table("A")
            .select()
            .left_join("B")
            .hash_executor("B.id", "A.id")
            .on("A.active")
            .build_query();
        let expected = Err(Error::QueryBuilder(
            QueryBuilderError::HashJoinExecutorRequiresPlan,
        ));
        assert_eq!(actual, expected);
    }
}
