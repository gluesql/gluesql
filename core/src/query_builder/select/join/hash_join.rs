use {
    super::JoinOperatorType,
    crate::{
        ast::Select,
        plan::{JoinConstraintPlan, JoinExecutorPlan, JoinInputPlan, JoinPlan},
        query_builder::{
            DistinctNode, ExprList, ExprNode, FilterNode, GroupByNode, HavingNode,
            JoinConstraintNode, JoinNode, LimitNode, OffsetNode, OrderByExprList, ProjectNode,
            QueryBuilderError, QueryNode, SelectItemList, SelectOrderByNode, TableFactorNode,
            select::{BuildJoinInputPlan, BuildJoinPlan, BuildSelect},
        },
        result::Result,
    },
};

#[derive(Clone, Debug)]
pub struct HashJoinNode<'a> {
    join_node: JoinNode<'a>,
    key_expr: ExprNode<'a>,
    value_expr: ExprNode<'a>,
    filter_expr: Option<ExprNode<'a>>,
}

impl<'a> HashJoinNode<'a> {
    pub(super) fn new<T: Into<ExprNode<'a>>, U: Into<ExprNode<'a>>>(
        join_node: JoinNode<'a>,
        key_expr: T,
        value_expr: U,
    ) -> Self {
        Self {
            join_node,
            key_expr: key_expr.into(),
            value_expr: value_expr.into(),
            filter_expr: None,
        }
    }

    #[must_use]
    pub fn hash_filter<T: Into<ExprNode<'a>>>(mut self, expr: T) -> Self {
        let expr = expr.into();
        let filter_expr = match self.filter_expr {
            Some(filter_expr) => filter_expr.and(expr),
            None => expr,
        };

        self.filter_expr = Some(filter_expr);
        self
    }

    pub fn on<T: Into<ExprNode<'a>>>(self, expr: T) -> JoinConstraintNode<'a> {
        JoinConstraintNode::new(self, expr)
    }

    pub fn join(self, table_name: &str) -> JoinNode<'a> {
        JoinNode::new(self, table_name.to_owned(), None, JoinOperatorType::Inner)
    }

    pub fn join_as(self, table_name: &str, alias: &str) -> JoinNode<'a> {
        JoinNode::new(
            self,
            table_name.to_owned(),
            Some(alias.to_owned()),
            JoinOperatorType::Inner,
        )
    }

    pub fn left_join(self, table_name: &str) -> JoinNode<'a> {
        JoinNode::new(self, table_name.to_owned(), None, JoinOperatorType::Left)
    }

    pub fn left_join_as(self, table_name: &str, alias: &str) -> JoinNode<'a> {
        JoinNode::new(
            self,
            table_name.to_owned(),
            Some(alias.to_owned()),
            JoinOperatorType::Left,
        )
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

    pub fn alias_as(self, table_alias: &'a str) -> TableFactorNode<'a> {
        QueryNode::HashJoinNode(self).alias_as(table_alias)
    }

    pub(super) fn build_join_plan_with_constraint(
        self,
        constraint: JoinConstraintPlan,
    ) -> Result<JoinPlan> {
        let (input, relation, join_operator) = self
            .join_node
            .build_join_plan_parts_with_constraint(constraint)?;
        let join_executor = build_join_executor(self.key_expr, self.value_expr, self.filter_expr)?;

        Ok(JoinPlan {
            input,
            relation,
            join_operator,
            join_executor,
        })
    }
}

impl BuildJoinPlan for HashJoinNode<'_> {
    fn build_join_plan(self) -> Result<JoinPlan> {
        self.build_join_plan_with_constraint(JoinConstraintPlan::None)
    }
}

impl BuildJoinInputPlan for HashJoinNode<'_> {
    fn build_join_input_plan(self) -> Result<JoinInputPlan> {
        self.build_join_plan()
            .map(Box::new)
            .map(JoinInputPlan::Join)
    }
}

impl BuildSelect for HashJoinNode<'_> {
    fn build_select(self) -> Result<Select> {
        Err(QueryBuilderError::HashJoinExecutorRequiresPlan.into())
    }
}

fn build_join_executor(
    key_expr: ExprNode,
    value_expr: ExprNode,
    filter_expr: Option<ExprNode>,
) -> Result<JoinExecutorPlan> {
    Ok(JoinExecutorPlan::Hash {
        key_expr: key_expr.build_expr_plan()?,
        value_expr: value_expr.build_expr_plan()?,
        where_clause: filter_expr.map(ExprNode::build_expr_plan).transpose()?,
    })
}

#[cfg(test)]
mod tests {
    use {
        crate::{
            plan::{
                JoinConstraintPlan, JoinExecutorPlan, JoinInputPlan, JoinOperatorPlan, JoinPlan,
                ProjectInputPlan, ProjectPlan, ProjectionPlan, QueryPlan, StatementPlan,
                TableAliasPlan, TableFactorPlan,
            },
            query_builder::{
                Build, QueryBuilderError, SelectItemList, col, expr,
                select::{BuildQuery, BuildSelect},
                table,
            },
            result::Error,
        },
        pretty_assertions::assert_eq,
    };

    #[test]
    fn hash_join() {
        let actual = table("Player")
            .select()
            .join("PlayerItem")
            .hash_executor("PlayerItem.user_id", col("Player.id"))
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
                input: ProjectInputPlan::Join(Box::new(join)),
                projection: ProjectionPlan::SelectItems(
                    SelectItemList::from("*").build_select_items_plan().unwrap(),
                ),
            };

            Ok(StatementPlan::Query(QueryPlan::Project(project)))
        };
        assert_eq!(actual, expected, "without filter");

        let actual = table("Player")
            .select()
            .join("PlayerItem")
            .hash_executor("PlayerItem.user_id", "Player.id")
            .hash_filter("PlayerItem.amount > 10")
            .hash_filter("PlayerItem.amount * 3 <= 2")
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
                    where_clause: Some(
                        expr("PlayerItem.amount > 10 AND PlayerItem.amount * 3 <= 2")
                            .build_expr_plan()
                            .unwrap(),
                    ),
                },
            };
            let project = ProjectPlan {
                input: ProjectInputPlan::Join(Box::new(join)),
                projection: ProjectionPlan::SelectItems(
                    SelectItemList::from("*").build_select_items_plan().unwrap(),
                ),
            };

            Ok(StatementPlan::Query(QueryPlan::Project(project)))
        };
        assert_eq!(actual, expected, "with filter");

        // join -> hash -> derived subquery
        let actual = table("Foo")
            .select()
            .join("Bar")
            .hash_executor("Foo.id", "Bar.id")
            .alias_as("Sub")
            .select()
            .build();

        let expected = {
            let join = JoinPlan {
                input: JoinInputPlan::Relation(TableFactorPlan::Table {
                    name: "Foo".to_owned(),
                    alias: None,
                    index: None,
                }),
                relation: TableFactorPlan::Table {
                    name: "Bar".to_owned(),
                    alias: None,
                    index: None,
                },
                join_operator: JoinOperatorPlan::Inner(JoinConstraintPlan::None),
                join_executor: JoinExecutorPlan::Hash {
                    key_expr: col("Foo.id").build_expr_plan().unwrap(),
                    value_expr: col("Bar.id").build_expr_plan().unwrap(),
                    where_clause: None,
                },
            };

            let subquery = ProjectPlan {
                input: ProjectInputPlan::Join(Box::new(join)),
                projection: ProjectionPlan::SelectItems(
                    SelectItemList::from("*").build_select_items_plan().unwrap(),
                ),
            };

            let project = ProjectPlan {
                input: ProjectInputPlan::Relation(TableFactorPlan::Derived {
                    subquery: Box::new(QueryPlan::Project(subquery)),
                    alias: TableAliasPlan {
                        name: "Sub".to_owned(),
                        columns: Vec::new(),
                    },
                }),
                projection: ProjectionPlan::SelectItems(
                    SelectItemList::from("*").build_select_items_plan().unwrap(),
                ),
            };

            Ok(StatementPlan::Query(QueryPlan::Project(project)))
        };
        assert_eq!(actual, expected);
    }

    #[test]
    fn hash_join_preserves_previous_join_stage() {
        let actual = table("Player")
            .select()
            .join("Team")
            .on("Player.team_id = Team.id")
            .join("PlayerItem")
            .hash_executor("PlayerItem.user_id", "Player.id")
            .build();
        let expected = {
            let previous_join = JoinPlan {
                input: JoinInputPlan::Relation(TableFactorPlan::Table {
                    name: "Player".to_owned(),
                    alias: None,
                    index: None,
                }),
                relation: TableFactorPlan::Table {
                    name: "Team".to_owned(),
                    alias: None,
                    index: None,
                },
                join_operator: JoinOperatorPlan::Inner(JoinConstraintPlan::On(
                    expr("Player.team_id = Team.id").build_expr_plan().unwrap(),
                )),
                join_executor: JoinExecutorPlan::NestedLoop,
            };
            let join = JoinPlan {
                input: JoinInputPlan::Join(Box::new(previous_join)),
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
                input: ProjectInputPlan::Join(Box::new(join)),
                projection: ProjectionPlan::SelectItems(
                    SelectItemList::from("*").build_select_items_plan().unwrap(),
                ),
            };

            Ok(StatementPlan::Query(QueryPlan::Project(project)))
        };

        assert_eq!(actual, expected);
    }

    #[test]
    fn hash_join_ast_build_requires_plan() {
        fn expected_error<T>(actual: crate::result::Result<T>) {
            assert_eq!(
                actual.map(|_| ()),
                Err(Error::QueryBuilder(
                    QueryBuilderError::HashJoinExecutorRequiresPlan
                ))
            );
        }

        fn hash_join() -> super::HashJoinNode<'static> {
            table("Player")
                .select()
                .join("PlayerItem")
                .hash_executor("PlayerItem.user_id", "Player.id")
        }

        let actual = table("Player")
            .select()
            .join("PlayerItem")
            .hash_executor("PlayerItem.user_id", "Player.id")
            .build_select();
        assert_eq!(
            actual,
            Err(Error::QueryBuilder(
                QueryBuilderError::HashJoinExecutorRequiresPlan
            ))
        );

        let actual = table("Player")
            .select()
            .join("PlayerItem")
            .hash_executor("PlayerItem.user_id", "Player.id")
            .on("PlayerItem.flag IS NOT NULL")
            .build_select();
        assert_eq!(
            actual,
            Err(Error::QueryBuilder(
                QueryBuilderError::HashJoinExecutorRequiresPlan
            ))
        );

        expected_error(hash_join().filter("Player.id > 0").build_select());
        expected_error(hash_join().group_by("Player.id").build_select());
        expected_error(hash_join().join("OtherItem").build_select());
        expected_error(hash_join().project("*").build_select());
        expected_error(hash_join().limit(1).build_query());
        expected_error(hash_join().offset(1).build_query());
        expected_error(hash_join().order_by("Player.id").build_query());
    }
}
