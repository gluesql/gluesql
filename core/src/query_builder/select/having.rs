use {
    super::{
        BuildAggregationInputPlan, BuildAggregationPlan, BuildHavingPlan, BuildProjectInputPlan,
        BuildSelect, DistinctNode,
    },
    crate::{
        ast::Select,
        plan::{AggregationPlan, HavingPlan, ProjectInputPlan},
        query_builder::{
            ExprNode, FilterNode, GroupByNode, HashJoinNode, JoinConstraintNode, JoinNode,
            LimitNode, OffsetNode, OrderByExprList, ProjectNode, QueryNode, SelectItemList,
            SelectNode, SelectOrderByNode, SourceNode,
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
    Filter(FilterNode<'a>),
    GroupBy(GroupByNode<'a>),
}

impl PrevNode<'_> {
    fn build_aggregation_plan(self) -> Result<AggregationPlan> {
        match self {
            Self::Select(node) => node.build_aggregation_input_plan(),
            Self::Join(node) => node.build_aggregation_input_plan(),
            Self::JoinConstraint(node) => node.build_aggregation_input_plan(),
            Self::HashJoin(node) => node.build_aggregation_input_plan(),
            Self::Filter(node) => node.build_aggregation_input_plan(),
            Self::GroupBy(node) => return node.build_aggregation_plan(),
        }
        .map(|input| AggregationPlan {
            input,
            group_by: Vec::new(),
            aggregate_slots: Vec::new(),
        })
    }
}

impl BuildSelect for PrevNode<'_> {
    fn build_select(self) -> Result<Select> {
        match self {
            Self::Select(node) => node.build_select(),
            Self::Join(node) => node.build_select(),
            Self::JoinConstraint(node) => node.build_select(),
            Self::HashJoin(node) => node.build_select(),
            Self::Filter(node) => node.build_select(),
            Self::GroupBy(node) => node.build_select(),
        }
    }
}

impl<'a> From<SelectNode<'a>> for PrevNode<'a> {
    fn from(node: SelectNode<'a>) -> Self {
        Self::Select(node)
    }
}

impl<'a> From<JoinNode<'a>> for PrevNode<'a> {
    fn from(node: JoinNode<'a>) -> Self {
        Self::Join(Box::new(node))
    }
}

impl<'a> From<JoinConstraintNode<'a>> for PrevNode<'a> {
    fn from(node: JoinConstraintNode<'a>) -> Self {
        Self::JoinConstraint(Box::new(node))
    }
}

impl<'a> From<HashJoinNode<'a>> for PrevNode<'a> {
    fn from(node: HashJoinNode<'a>) -> Self {
        Self::HashJoin(Box::new(node))
    }
}

impl<'a> From<FilterNode<'a>> for PrevNode<'a> {
    fn from(node: FilterNode<'a>) -> Self {
        Self::Filter(node)
    }
}

impl<'a> From<GroupByNode<'a>> for PrevNode<'a> {
    fn from(node: GroupByNode<'a>) -> Self {
        Self::GroupBy(node)
    }
}

#[derive(Clone, Debug)]
pub struct HavingNode<'a> {
    prev_node: PrevNode<'a>,
    expr: ExprNode<'a>,
}

impl<'a> HavingNode<'a> {
    pub(super) fn new<N: Into<PrevNode<'a>>, T: Into<ExprNode<'a>>>(prev_node: N, expr: T) -> Self {
        Self {
            prev_node: prev_node.into(),
            expr: expr.into(),
        }
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

    pub fn order_by<T: Into<OrderByExprList<'a>>>(self, expr_list: T) -> SelectOrderByNode<'a> {
        SelectOrderByNode::new(self, expr_list)
    }

    pub fn distinct(self) -> DistinctNode<'a> {
        DistinctNode::new(self)
    }

    pub fn alias_as(self, table_alias: &'a str) -> SourceNode<'a> {
        QueryNode::HavingNode(self).alias_as(table_alias)
    }
}

impl BuildHavingPlan for HavingNode<'_> {
    fn build_having_plan(self) -> Result<HavingPlan> {
        Ok(HavingPlan {
            input: self.prev_node.build_aggregation_plan()?,
            expr: self.expr.build_expr_plan()?,
        })
    }
}

impl BuildProjectInputPlan for HavingNode<'_> {
    fn build_project_input_plan(self) -> Result<ProjectInputPlan> {
        self.build_having_plan().map(ProjectInputPlan::Having)
    }
}

impl BuildSelect for HavingNode<'_> {
    fn build_select(self) -> Result<Select> {
        let mut select = self.prev_node.build_select()?;
        select.having = Some(self.expr.build_expr()?);

        Ok(select)
    }
}

#[cfg(test)]
mod tests {
    use {
        crate::{
            data::Value,
            plan::{
                AggregationInputPlan, AggregationPlan, ExprPlan, HavingPlan, JoinConstraintPlan,
                JoinExecutorPlan, JoinInputPlan, JoinOperatorPlan, JoinPlan, ProjectInputPlan,
                ProjectPlan, ProjectionPlan, QueryPlan, SelectItemPlan, SourcePlan, StatementPlan,
                TableAccessPlan, TableSourcePlan,
            },
            query_builder::{
                Build, QueryBuilderError, select::BuildQuery, table, test_query_builder,
            },
            result::Error,
        },
        pretty_assertions::assert_eq,
    };

    #[test]
    fn prev_nodes() {
        let actual = table("Foo").select().having("TRUE");
        let expected = "SELECT * FROM Foo HAVING TRUE";
        test_query_builder(actual, expected);

        let actual = table("Foo").select().join("Bar").having("TRUE");
        let expected = "SELECT * FROM Foo JOIN Bar HAVING TRUE";
        test_query_builder(actual, expected);

        let actual = table("Foo")
            .select()
            .join("Bar")
            .on("Foo.id = Bar.id")
            .having("TRUE");
        let expected = "SELECT * FROM Foo JOIN Bar ON Foo.id = Bar.id HAVING TRUE";
        test_query_builder(actual, expected);

        let actual = table("Foo").select().filter("id > 1").having("TRUE");
        let expected = "SELECT * FROM Foo WHERE id > 1 HAVING TRUE";
        test_query_builder(actual, expected);
    }

    #[test]
    fn plan_only_hash_join() {
        let actual = table("Foo")
            .select()
            .join("Bar")
            .hash_executor("Foo.id", "Bar.id")
            .having("TRUE")
            .build_query()
            .map(|_| ());
        let expected = Err(Error::QueryBuilder(
            QueryBuilderError::HashJoinExecutorRequiresPlan,
        ));
        assert_eq!(actual, expected);

        let actual = table("Foo")
            .select()
            .join("Bar")
            .hash_executor("Foo.id", "Bar.id")
            .having("TRUE")
            .build();
        let expected = Ok(StatementPlan::Query(QueryPlan::Project(ProjectPlan {
            input: ProjectInputPlan::Having(HavingPlan {
                input: AggregationPlan {
                    input: AggregationInputPlan::Join(Box::new(JoinPlan {
                        input: JoinInputPlan::Source(SourcePlan::Table(TableSourcePlan {
                            name: "Foo".to_owned(),
                            alias: None,
                            access: TableAccessPlan::FullScan,
                        })),
                        right: SourcePlan::Table(TableSourcePlan {
                            name: "Bar".to_owned(),
                            alias: None,
                            access: TableAccessPlan::FullScan,
                        }),
                        join_operator: JoinOperatorPlan::Inner(JoinConstraintPlan::None),
                        join_executor: JoinExecutorPlan::Hash {
                            key_expr: ExprPlan::CompoundIdentifier {
                                alias: "Foo".to_owned(),
                                ident: "id".to_owned(),
                            },
                            value_expr: ExprPlan::CompoundIdentifier {
                                alias: "Bar".to_owned(),
                                ident: "id".to_owned(),
                            },
                            where_clause: None,
                        },
                    })),
                    group_by: Vec::new(),
                    aggregate_slots: Vec::new(),
                },
                expr: ExprPlan::Value(Value::Bool(true)),
            }),
            projection: ProjectionPlan::SelectItems(vec![SelectItemPlan::Wildcard]),
        })));

        assert_eq!(actual, expected);
    }

    #[test]
    fn having() {
        // group by node -> having node -> offset node
        let actual = table("Bar")
            .select()
            .filter("id IS NULL")
            .group_by("id, (a + name)")
            .having("COUNT(id) > 10")
            .offset(10);
        let expected = "
            SELECT * FROM Bar
            WHERE id IS NULL
            GROUP BY id, (a + name)
            HAVING COUNT(id) > 10
            OFFSET 10
        ";
        test_query_builder(actual, expected);

        // group by node -> having node -> limit node
        let actual = table("Bar")
            .select()
            .filter("id IS NULL")
            .group_by("id, (a + name)")
            .having("COUNT(id) > 10")
            .limit(10);
        let expected = "
            SELECT * FROM Bar
            WHERE id IS NULL
            GROUP BY id, (a + name)
            HAVING COUNT(id) > 10
            LIMIT 10
            ";
        test_query_builder(actual, expected);

        // group by node -> having node -> project node
        let actual = table("Bar")
            .select()
            .filter("id IS NULL")
            .group_by("id, (a + name)")
            .having("COUNT(id) > 10")
            .project(vec!["id", "(a + name) AS b", "COUNT(id) AS c"]);
        let expected = "
            SELECT id, (a + name) AS b, COUNT(id) AS c
            FROM Bar
            WHERE id IS NULL
            GROUP BY id, (a + name)
            HAVING COUNT(id) > 10
        ";
        test_query_builder(actual, expected);

        // group by node -> having node -> build
        let actual = table("Bar")
            .select()
            .filter("id IS NULL")
            .group_by("id, (a + name)")
            .having("COUNT(id) > 10");
        let expected = "
                SELECT * FROM Bar
                WHERE id IS NULL
                GROUP BY id, (a + name)
                HAVING COUNT(id) > 10
            ";
        test_query_builder(actual, expected);

        // select -> group by -> having -> derived subquery
        let actual = table("Foo")
            .select()
            .group_by("a")
            .having("a > 1")
            .alias_as("Sub")
            .select();
        let expected = "SELECT * FROM (SELECT * FROM Foo GROUP BY a HAVING a > 1) Sub";
        test_query_builder(actual, expected);
    }
}
