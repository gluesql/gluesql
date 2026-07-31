use {
    super::{
        BuildAggregationInputPlan, BuildFilterInputPlan, BuildFilterPlan, BuildProjectInputPlan,
        BuildSelect, DistinctNode,
    },
    crate::{
        ast::Select,
        plan::{AggregationInputPlan, FilterPlan, ProjectInputPlan},
        query_builder::{
            ExprList, ExprNode, GroupByNode, HashJoinNode, HavingNode, JoinConstraintNode,
            JoinNode, LimitNode, OffsetNode, OrderByExprList, ProjectNode, QueryNode,
            SelectItemList, SelectNode, SelectOrderByNode, TableFactorNode,
        },
        result::Result,
    },
};

#[derive(Clone, Debug)]
pub(super) enum PrevNode<'a> {
    Select(SelectNode<'a>),
    Join(Box<JoinNode<'a>>),
    JoinConstraint(Box<JoinConstraintNode<'a>>),
    HashJoin(Box<HashJoinNode<'a>>),
}

impl BuildFilterInputPlan for PrevNode<'_> {
    fn build_filter_input_plan(self) -> Result<crate::plan::FilterInputPlan> {
        match self {
            Self::Select(node) => node.build_filter_input_plan(),
            Self::Join(node) => node.build_filter_input_plan(),
            Self::JoinConstraint(node) => node.build_filter_input_plan(),
            Self::HashJoin(node) => node.build_filter_input_plan(),
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

    pub fn alias_as(self, table_alias: &'a str) -> TableFactorNode<'a> {
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
                FilterInputPlan, FilterPlan, JoinConstraintPlan, JoinExecutorPlan, JoinInputPlan,
                JoinOperatorPlan, JoinPlan, ProjectInputPlan, ProjectPlan, ProjectionPlan,
                QueryPlan, StatementPlan, TableFactorPlan,
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

        // join node -> filter node -> build
        let actual = table("Foo").select().join("Bar").filter("id IS NULL");
        let expected = "SELECT * FROM Foo JOIN Bar WHERE id IS NULL";
        test_query_builder(actual, expected);

        // join node -> filter node -> build
        let actual = table("Foo")
            .select()
            .join_as("Bar", "b")
            .filter("id IS NULL");
        let expected = "SELECT * FROM Foo JOIN Bar AS b WHERE id IS NULL";
        test_query_builder(actual, expected);

        // join node -> filter node -> build
        let actual = table("Foo").select().left_join("Bar").filter("id IS NULL");
        let expected = "SELECT * FROM Foo LEFT JOIN Bar WHERE id IS NULL";
        test_query_builder(actual, expected);

        // join node -> filter node -> build
        let actual = table("Foo")
            .select()
            .left_join_as("Bar", "b")
            .filter("id IS NULL");
        let expected = "SELECT * FROM Foo LEFT JOIN Bar AS b WHERE id IS NULL";
        test_query_builder(actual, expected);

        // join constraint node -> filter node -> build
        let actual = table("Foo")
            .select()
            .join("Bar")
            .on("Foo.id = Bar.id")
            .filter("id IS NULL");
        let expected = "SELECT * FROM Foo JOIN Bar ON Foo.id = Bar.id WHERE id IS NULL";
        test_query_builder(actual, expected);

        // hash join node -> filter node -> build
        let actual = table("Player")
            .select()
            .join("PlayerItem")
            .hash_executor("PlayerItem.user_id", "Player.id")
            .filter("PlayerItem.amount > 10")
            .build();
        let expected = {
            let join = JoinPlan {
                input: JoinInputPlan::Relation(TableFactorPlan::Table {
                    name: "Player".to_owned(),
                    alias: None,
                    index: None,
                }),
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
            let project = ProjectPlan {
                input: ProjectInputPlan::Filter(FilterPlan {
                    input: FilterInputPlan::Join(Box::new(join)),
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
