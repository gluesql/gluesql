use {
    super::{
        BuildAggregationInputPlan, BuildFilterInputPlan, BuildFilterPlan, BuildProjectInputPlan,
        BuildSelect, DistinctNode,
    },
    crate::{
        ast::Select,
        plan::{AggregationInputPlan, FilterInputPlan, FilterPlan, ProjectInputPlan},
        query_builder::{
            ExprList, ExprNode, GroupByNode, HavingNode, InnerHashJoinNode, InnerJoinConditionNode,
            InnerNestedLoopJoinNode, LeftOuterHashJoinNode, LeftOuterJoinConditionNode,
            LeftOuterNestedLoopJoinNode, LimitNode, OffsetNode, OrderByExprList, ProjectNode,
            QueryNode, SelectItemList, SelectNode, SelectOrderByNode, SourceNode,
        },
        result::Result,
    },
};

#[derive(Clone, Debug)]
pub(super) enum PrevNode<'a> {
    Select(SelectNode<'a>),
    InnerNestedLoop(Box<InnerNestedLoopJoinNode<'a>>),
    LeftOuterNestedLoop(Box<LeftOuterNestedLoopJoinNode<'a>>),
    InnerHash(Box<InnerHashJoinNode<'a>>),
    LeftOuterHash(Box<LeftOuterHashJoinNode<'a>>),
    InnerCondition(Box<InnerJoinConditionNode<'a>>),
    LeftOuterCondition(Box<LeftOuterJoinConditionNode<'a>>),
}

impl BuildFilterInputPlan for PrevNode<'_> {
    fn build_filter_input_plan(self) -> Result<FilterInputPlan> {
        match self {
            Self::Select(node) => node.build_filter_input_plan(),
            Self::InnerNestedLoop(node) => node.build_filter_input_plan(),
            Self::LeftOuterNestedLoop(node) => node.build_filter_input_plan(),
            Self::InnerHash(node) => node.build_filter_input_plan(),
            Self::LeftOuterHash(node) => node.build_filter_input_plan(),
            Self::InnerCondition(node) => node.build_filter_input_plan(),
            Self::LeftOuterCondition(node) => node.build_filter_input_plan(),
        }
    }
}

impl BuildSelect for PrevNode<'_> {
    fn build_select(self) -> Result<Select> {
        match self {
            Self::Select(node) => node.build_select(),
            Self::InnerNestedLoop(node) => node.build_select(),
            Self::LeftOuterNestedLoop(node) => node.build_select(),
            Self::InnerHash(node) => node.build_select(),
            Self::LeftOuterHash(node) => node.build_select(),
            Self::InnerCondition(node) => node.build_select(),
            Self::LeftOuterCondition(node) => node.build_select(),
        }
    }
}

impl<'a> From<InnerNestedLoopJoinNode<'a>> for PrevNode<'a> {
    fn from(node: InnerNestedLoopJoinNode<'a>) -> Self {
        Self::InnerNestedLoop(Box::new(node))
    }
}

impl<'a> From<LeftOuterNestedLoopJoinNode<'a>> for PrevNode<'a> {
    fn from(node: LeftOuterNestedLoopJoinNode<'a>) -> Self {
        Self::LeftOuterNestedLoop(Box::new(node))
    }
}

impl<'a> From<InnerHashJoinNode<'a>> for PrevNode<'a> {
    fn from(node: InnerHashJoinNode<'a>) -> Self {
        Self::InnerHash(Box::new(node))
    }
}

impl<'a> From<LeftOuterHashJoinNode<'a>> for PrevNode<'a> {
    fn from(node: LeftOuterHashJoinNode<'a>) -> Self {
        Self::LeftOuterHash(Box::new(node))
    }
}

impl<'a> From<InnerJoinConditionNode<'a>> for PrevNode<'a> {
    fn from(node: InnerJoinConditionNode<'a>) -> Self {
        Self::InnerCondition(Box::new(node))
    }
}

impl<'a> From<LeftOuterJoinConditionNode<'a>> for PrevNode<'a> {
    fn from(node: LeftOuterJoinConditionNode<'a>) -> Self {
        Self::LeftOuterCondition(Box::new(node))
    }
}

impl<'a> From<SelectNode<'a>> for PrevNode<'a> {
    fn from(node: SelectNode<'a>) -> Self {
        PrevNode::Select(node)
    }
}

#[derive(Clone, Debug)]
pub struct FilterNode<'a> {
    prev_node: PrevNode<'a>,
    filter_expr: ExprNode<'a>,
}

impl<'a> FilterNode<'a> {
    pub(super) fn new<N: Into<PrevNode<'a>>, T: Into<ExprNode<'a>>>(prev_node: N, expr: T) -> Self {
        Self {
            prev_node: prev_node.into(),
            filter_expr: expr.into(),
        }
    }

    #[must_use]
    pub fn filter<T: Into<ExprNode<'a>>>(mut self, expr: T) -> Self {
        let exprs = self.filter_expr;
        self.filter_expr = exprs.and(expr);
        self
    }

    pub fn offset<T: Into<ExprNode<'a>>>(self, expr: T) -> OffsetNode<'a> {
        OffsetNode::new(self, expr)
    }

    pub fn limit<T: Into<ExprNode<'a>>>(self, expr: T) -> LimitNode<'a> {
        LimitNode::new(self, expr)
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
        QueryNode::FilterNode(self).alias_as(table_alias)
    }
}

impl BuildFilterPlan for FilterNode<'_> {
    fn build_filter_plan(self) -> Result<FilterPlan> {
        Ok(FilterPlan {
            input: self.prev_node.build_filter_input_plan()?,
            expr: self.filter_expr.build_expr_plan()?,
        })
    }
}

impl BuildAggregationInputPlan for FilterNode<'_> {
    fn build_aggregation_input_plan(self) -> Result<AggregationInputPlan> {
        self.build_filter_plan().map(AggregationInputPlan::Filter)
    }
}

impl BuildProjectInputPlan for FilterNode<'_> {
    fn build_project_input_plan(self) -> Result<ProjectInputPlan> {
        self.build_filter_plan().map(ProjectInputPlan::Filter)
    }
}

impl BuildSelect for FilterNode<'_> {
    fn build_select(self) -> Result<Select> {
        let mut select = self.prev_node.build_select()?;
        select.selection = Some(self.filter_expr.build_expr()?);

        Ok(select)
    }
}

#[cfg(test)]
mod tests {
    use {
        crate::{
            ast::{BinaryOperator, Expr},
            plan::{
                FilterInputPlan, FilterPlan, HashJoinInputPlan, HashJoinPlan, InnerJoinInputPlan,
                InnerJoinPlan, ProjectInputPlan, ProjectPlan, ProjectionPlan, QueryPlan,
                SourcePlan, StatementPlan, TableAccessPlan, TableSourcePlan,
            },
            query_builder::{Build, SelectItemList, col, expr, table, test_query_builder},
        },
        pretty_assertions::assert_eq,
    };

    #[test]
    fn filter() {
        // select node -> filter node -> build
        let actual = table("Bar").select().filter("id IS NULL");
        let expected = "SELECT * FROM Bar WHERE id IS NULL";
        test_query_builder(actual, expected);

        // select node -> filter node -> build
        let actual = table("Foo").select().filter(Expr::BinaryOp {
            left: Box::new(Expr::Identifier("col1".to_owned())),
            op: BinaryOperator::Gt,
            right: Box::new(Expr::Identifier("col2".to_owned())),
        });
        let expected = "SELECT * FROM Foo WHERE col1 > col2";
        test_query_builder(actual, expected);

        // filter node -> filter node -> build
        let actual = table("Bar")
            .select()
            .filter("id IS NULL")
            .filter("id > 10")
            .filter("id < 20");
        let expected = "SELECT * FROM Bar WHERE id IS NULL AND id > 10 AND id < 20";
        test_query_builder(actual, expected);

        // inner nested loop join node -> filter node -> build
        let actual = table("Foo").select().join("Bar").filter("id IS NULL");
        let expected = "SELECT * FROM Foo JOIN Bar WHERE id IS NULL";
        test_query_builder(actual, expected);

        // inner nested loop join node -> filter node -> build
        let actual = table("Foo")
            .select()
            .join_as("Bar", "b")
            .filter("id IS NULL");
        let expected = "SELECT * FROM Foo JOIN Bar AS b WHERE id IS NULL";
        test_query_builder(actual, expected);

        // left outer nested loop join node -> filter node -> build
        let actual = table("Foo").select().left_join("Bar").filter("id IS NULL");
        let expected = "SELECT * FROM Foo LEFT JOIN Bar WHERE id IS NULL";
        test_query_builder(actual, expected);

        // left outer nested loop join node -> filter node -> build
        let actual = table("Foo")
            .select()
            .left_join_as("Bar", "b")
            .filter("id IS NULL");
        let expected = "SELECT * FROM Foo LEFT JOIN Bar AS b WHERE id IS NULL";
        test_query_builder(actual, expected);

        // inner join condition node -> filter node -> build
        let actual = table("Foo")
            .select()
            .join("Bar")
            .on("Foo.id = Bar.id")
            .filter("id IS NULL");
        let expected = "SELECT * FROM Foo JOIN Bar ON Foo.id = Bar.id WHERE id IS NULL";
        test_query_builder(actual, expected);

        // inner hash join node -> filter node -> build
        let actual = table("Player")
            .select()
            .join("PlayerItem")
            .hash_executor("PlayerItem.user_id", "Player.id")
            .filter("PlayerItem.amount > 10")
            .build();
        let expected = {
            let join = InnerJoinPlan {
                input: InnerJoinInputPlan::Hash(HashJoinPlan {
                    input: HashJoinInputPlan::Source(SourcePlan::Table(TableSourcePlan {
                        name: "Player".to_owned(),
                        alias: None,
                        access: TableAccessPlan::FullScan,
                    })),
                    right: SourcePlan::Table(TableSourcePlan {
                        name: "PlayerItem".to_owned(),
                        alias: None,
                        access: TableAccessPlan::FullScan,
                    }),
                    input_key: col("Player.id").build_expr_plan().unwrap(),
                    right_key: col("PlayerItem.user_id").build_expr_plan().unwrap(),
                    right_filter: None,
                }),
            };
            let project = ProjectPlan {
                input: ProjectInputPlan::Filter(FilterPlan {
                    input: FilterInputPlan::InnerJoin(Box::new(join)),
                    expr: expr("PlayerItem.amount > 10").build_expr_plan().unwrap(),
                }),
                projection: ProjectionPlan::SelectItems(
                    SelectItemList::from("*").build_select_items_plan().unwrap(),
                ),
            };

            Ok(StatementPlan::Query(QueryPlan::Project(project)))
        };
        assert_eq!(actual, expected);

        // select node -> filter node -> derived subquery
        let actual = table("Bar")
            .select()
            .filter("id IS NULL")
            .alias_as("Sub")
            .select();
        let expected = "SELECT * FROM (SELECT * FROM Bar WHERE id IS NULL) Sub";
        test_query_builder(actual, expected);
    }
}
