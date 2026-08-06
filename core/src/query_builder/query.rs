use {
    super::{
        DistinctNode, ExprList, FilterNode, GroupByNode, HavingNode, InnerHashJoinNode,
        InnerJoinConditionNode, InnerNestedLoopJoinNode, LeftOuterHashJoinNode,
        LeftOuterJoinConditionNode, LeftOuterNestedLoopJoinNode, LimitNode, OffsetLimitNode,
        OffsetNode, ProjectNode, SelectNode, SelectOrderByNode, SourceNode, ValuesOrderByNode,
        select::{BuildQuery, BuildQueryPlan, ValuesNode},
    },
    crate::{
        ast::{Query, SetExpr, Values},
        parse_sql::parse_query,
        plan::{QueryPlan, ValuesPlan},
        result::Result,
        translate::{NO_PARAMS, translate_query},
    },
};

#[derive(Clone, Debug)]
pub enum QueryNode<'a> {
    Text(String),
    Values(Vec<ExprList<'a>>),
    SelectNode(SelectNode<'a>),
    ValuesNode(ValuesNode<'a>),
    InnerNestedLoopJoinNode(InnerNestedLoopJoinNode<'a>),
    LeftOuterNestedLoopJoinNode(LeftOuterNestedLoopJoinNode<'a>),
    InnerHashJoinNode(InnerHashJoinNode<'a>),
    LeftOuterHashJoinNode(LeftOuterHashJoinNode<'a>),
    InnerJoinConditionNode(InnerJoinConditionNode<'a>),
    LeftOuterJoinConditionNode(LeftOuterJoinConditionNode<'a>),
    GroupByNode(GroupByNode<'a>),
    HavingNode(HavingNode<'a>),
    LimitNode(LimitNode<'a>),
    OffsetNode(OffsetNode<'a>),
    OffsetLimitNode(OffsetLimitNode<'a>),
    FilterNode(FilterNode<'a>),
    ProjectNode(ProjectNode<'a>),
    SelectOrderByNode(SelectOrderByNode<'a>),
    ValuesOrderByNode(ValuesOrderByNode<'a>),
    DistinctNode(DistinctNode<'a>),
}

impl<'a> QueryNode<'a> {
    pub fn alias_as(self, table_alias: &'a str) -> SourceNode<'a> {
        SourceNode::Derived {
            query: Box::new(self),
            alias: table_alias.to_owned(),
        }
    }

    pub(super) fn build_query(self) -> Result<Query> {
        match self {
            QueryNode::Text(query_node) => {
                parse_query(query_node).and_then(|item| translate_query(&item, NO_PARAMS))
            }
            QueryNode::Values(values) => {
                let values = values
                    .into_iter()
                    .map(ExprList::build_exprs)
                    .collect::<Result<Vec<_>>>()?;

                Ok(Query {
                    body: SetExpr::Values(Values(values)),
                    order_by: Vec::new(),
                    limit: None,
                    offset: None,
                })
            }
            QueryNode::SelectNode(node) => node.build_query(),
            QueryNode::ValuesNode(node) => node.build_query(),
            QueryNode::InnerNestedLoopJoinNode(node) => node.build_query(),
            QueryNode::LeftOuterNestedLoopJoinNode(node) => node.build_query(),
            QueryNode::InnerHashJoinNode(node) => node.build_query(),
            QueryNode::LeftOuterHashJoinNode(node) => node.build_query(),
            QueryNode::InnerJoinConditionNode(node) => node.build_query(),
            QueryNode::LeftOuterJoinConditionNode(node) => node.build_query(),
            QueryNode::GroupByNode(node) => node.build_query(),
            QueryNode::HavingNode(node) => node.build_query(),
            QueryNode::FilterNode(node) => node.build_query(),
            QueryNode::LimitNode(node) => node.build_query(),
            QueryNode::OffsetNode(node) => node.build_query(),
            QueryNode::OffsetLimitNode(node) => node.build_query(),
            QueryNode::ProjectNode(node) => node.build_query(),
            QueryNode::SelectOrderByNode(node) => node.build_query(),
            QueryNode::ValuesOrderByNode(node) => node.build_query(),
            QueryNode::DistinctNode(node) => node.build_query(),
        }
    }

    pub(super) fn build_query_plan(self) -> Result<QueryPlan> {
        match self {
            QueryNode::Text(query_node) => parse_query(query_node)
                .and_then(|item| translate_query(&item, NO_PARAMS).map(Into::into)),
            QueryNode::Values(values) => {
                let values = values
                    .into_iter()
                    .map(ExprList::build_exprs_plan)
                    .collect::<Result<Vec<_>>>()?;

                Ok(QueryPlan::Values(ValuesPlan(values)))
            }
            QueryNode::SelectNode(node) => node.build_query_plan(),
            QueryNode::ValuesNode(node) => node.build_query_plan(),
            QueryNode::InnerNestedLoopJoinNode(node) => node.build_query_plan(),
            QueryNode::LeftOuterNestedLoopJoinNode(node) => node.build_query_plan(),
            QueryNode::InnerHashJoinNode(node) => node.build_query_plan(),
            QueryNode::LeftOuterHashJoinNode(node) => node.build_query_plan(),
            QueryNode::InnerJoinConditionNode(node) => node.build_query_plan(),
            QueryNode::LeftOuterJoinConditionNode(node) => node.build_query_plan(),
            QueryNode::GroupByNode(node) => node.build_query_plan(),
            QueryNode::HavingNode(node) => node.build_query_plan(),
            QueryNode::FilterNode(node) => node.build_query_plan(),
            QueryNode::LimitNode(node) => node.build_query_plan(),
            QueryNode::OffsetNode(node) => node.build_query_plan(),
            QueryNode::OffsetLimitNode(node) => node.build_query_plan(),
            QueryNode::ProjectNode(node) => node.build_query_plan(),
            QueryNode::SelectOrderByNode(node) => node.build_query_plan(),
            QueryNode::ValuesOrderByNode(node) => node.build_query_plan(),
            QueryNode::DistinctNode(node) => node.build_query_plan(),
        }
    }
}

impl From<&str> for QueryNode<'_> {
    fn from(query: &str) -> Self {
        Self::Text(query.to_owned())
    }
}

impl<'a> From<SelectNode<'a>> for QueryNode<'a> {
    fn from(node: SelectNode<'a>) -> Self {
        QueryNode::SelectNode(node)
    }
}

macro_rules! impl_from_select_nodes {
    ($type: ident) => {
        impl<'a> From<$type<'a>> for QueryNode<'a> {
            fn from(node: $type<'a>) -> Self {
                QueryNode::$type(node)
            }
        }
    };
}

impl_from_select_nodes!(InnerNestedLoopJoinNode);
impl_from_select_nodes!(LeftOuterNestedLoopJoinNode);
impl_from_select_nodes!(InnerHashJoinNode);
impl_from_select_nodes!(LeftOuterHashJoinNode);
impl_from_select_nodes!(InnerJoinConditionNode);
impl_from_select_nodes!(LeftOuterJoinConditionNode);
impl_from_select_nodes!(GroupByNode);
impl_from_select_nodes!(HavingNode);
impl_from_select_nodes!(FilterNode);
impl_from_select_nodes!(LimitNode);
impl_from_select_nodes!(OffsetNode);
impl_from_select_nodes!(OffsetLimitNode);
impl_from_select_nodes!(ProjectNode);
impl_from_select_nodes!(SelectOrderByNode);
impl_from_select_nodes!(ValuesOrderByNode);
impl_from_select_nodes!(DistinctNode);

#[cfg(test)]
mod test {
    use {
        super::QueryNode,
        crate::{
            plan::{
                HashJoinInputPlan, HashJoinPlan, InnerJoinInputPlan, InnerJoinPlan,
                ProjectInputPlan, ProjectPlan, ProjectionPlan, QueryPlan, SourcePlan,
                TableAccessPlan, TableSourcePlan,
            },
            query_builder::{
                SelectItemList, col, glue_indexes, glue_objects, glue_table_columns, glue_tables,
                series, table, test_query, test_query_builder, values,
            },
        },
        pretty_assertions::assert_eq,
    };

    #[test]
    fn query() {
        let actual = QueryNode::Values(vec!["1, 'a'".into(), "2, 'b'".into()]);
        let expected = "VALUES(1, 'a'), (2, 'b')";
        test_query(actual, expected);

        let actual = table("FOO").select().into();
        let expected = "SELECT * FROM FOO";
        test_query(actual, expected);

        let actual = table("Bar").select().join("Foo").into();
        let expected = "SELECT * FROM Bar JOIN Foo";
        test_query(actual, expected);

        let actual = table("Bar")
            .select()
            .join("Foo")
            .on("Foo.id = Bar.foo_id")
            .into();
        let expected = "SELECT * FROM Bar JOIN Foo ON Foo.id = Bar.foo_id";
        test_query(actual, expected);

        let actual = QueryNode::from(
            table("Player")
                .select()
                .join("PlayerItem")
                .hash_executor("PlayerItem.user_id", "Player.id"),
        )
        .build_query_plan()
        .unwrap();
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
                input: ProjectInputPlan::InnerJoin(Box::new(join)),
                projection: ProjectionPlan::SelectItems(
                    SelectItemList::from("*").build_select_items_plan().unwrap(),
                ),
            };

            QueryPlan::Project(project)
        };
        assert_eq!(actual, expected);

        let actual = table("FOO").select().group_by("id").into();
        let expected = "SELECT * FROM FOO GROUP BY id";
        test_query(actual, expected);

        let actual = table("FOO")
            .select()
            .group_by("id")
            .having("COUNT(id) > 10")
            .into();
        let expected = "SELECT * FROM FOO GROUP BY id HAVING COUNT(id) > 10";
        test_query(actual, expected);

        let actual = table("FOO")
            .select()
            .group_by("city")
            .having("COUNT(name) < 100")
            .limit(3)
            .into();
        let expected = "SELECT * FROM FOO GROUP BY city HAVING COUNT(name) < 100 LIMIT 3";
        test_query(actual, expected);

        let actual = table("FOO").select().offset(10).into();
        let expected = "SELECT * FROM FOO OFFSET 10";
        test_query(actual, expected);

        let actual = table("FOO")
            .select()
            .group_by("city")
            .having("COUNT(name) < 100")
            .offset(1)
            .limit(3)
            .into();
        let expected = "SELECT * FROM FOO GROUP BY city HAVING COUNT(name) < 100 OFFSET 1 LIMIT 3";
        test_query(actual, expected);

        let actual = table("FOO").select().project("id, name").limit(10).into();
        let expected = r"SELECT id, name FROM FOO LIMIT 10";
        test_query(actual, expected);

        let actual = table("Foo").select().order_by("score DESC").into();
        let expected = "SELECT * FROM Foo ORDER BY score DESC";
        test_query(actual, expected);

        let actual = table("Foo")
            .select()
            .project("id")
            .order_by("id")
            .distinct()
            .into();
        let expected = "SELECT DISTINCT id FROM Foo ORDER BY id";
        test_query(actual, expected);

        let actual = glue_objects().select().into();
        let expected = "SELECT * FROM GLUE_OBJECTS";
        test_query(actual, expected);

        let actual = glue_tables().select().into();
        let expected = "SELECT * FROM GLUE_TABLES";
        test_query(actual, expected);

        let actual = glue_indexes().select().into();
        let expected = "SELECT * FROM GLUE_INDEXES";
        test_query(actual, expected);

        let actual = glue_table_columns().select().into();
        let expected = "SELECT * FROM GLUE_TABLE_COLUMNS";
        test_query(actual, expected);

        let actual = series("1 + 2").select().into();
        let expected = "SELECT * FROM SERIES(1 + 2)";
        test_query(actual, expected);

        let actual = table("Items").select().alias_as("Sub").select().into();
        let expected = "SELECT * FROM (SELECT * FROM Items) AS Sub";
        test_query(actual, expected);
    }

    #[test]
    fn select_distinct_builds_after_order_by() {
        let actual = table("Item").select().distinct();
        test_query_builder(actual, "SELECT DISTINCT * FROM Item");

        let actual = table("Item").select().order_by("id").distinct();
        test_query_builder(actual, "SELECT DISTINCT * FROM Item ORDER BY id");

        let actual = table("Item").select().distinct().offset(2);
        test_query_builder(actual, "SELECT DISTINCT * FROM Item OFFSET 2");

        let actual = table("Item").select().order_by("id").distinct().offset(2);
        test_query_builder(actual, "SELECT DISTINCT * FROM Item ORDER BY id OFFSET 2");

        let actual = table("Item").select().distinct().limit(3);
        test_query_builder(actual, "SELECT DISTINCT * FROM Item LIMIT 3");

        let actual = table("Item").select().order_by("id").distinct().limit(3);
        test_query_builder(actual, "SELECT DISTINCT * FROM Item ORDER BY id LIMIT 3");

        let actual = table("Item").select().distinct().offset(2).limit(3);
        test_query_builder(actual, "SELECT DISTINCT * FROM Item OFFSET 2 LIMIT 3");

        let actual = table("Item")
            .select()
            .order_by("id")
            .distinct()
            .offset(2)
            .limit(3);
        test_query_builder(
            actual,
            "SELECT DISTINCT * FROM Item ORDER BY id OFFSET 2 LIMIT 3",
        );
    }

    #[test]
    fn query_builder_builds_only_valid_terminal_stage_relations() {
        let actual = table("Foo").select();
        test_query_builder(actual, "SELECT * FROM Foo");

        let actual = table("Foo").select().order_by("id");
        test_query_builder(actual, "SELECT * FROM Foo ORDER BY id");

        let actual = table("Foo").select().offset(2);
        test_query_builder(actual, "SELECT * FROM Foo OFFSET 2");

        let actual = table("Foo").select().order_by("id").offset(2);
        test_query_builder(actual, "SELECT * FROM Foo ORDER BY id OFFSET 2");

        let actual = table("Foo").select().limit(3);
        test_query_builder(actual, "SELECT * FROM Foo LIMIT 3");

        let actual = table("Foo").select().order_by("id").limit(3);
        test_query_builder(actual, "SELECT * FROM Foo ORDER BY id LIMIT 3");

        let actual = table("Foo").select().offset(2).limit(3);
        test_query_builder(actual, "SELECT * FROM Foo OFFSET 2 LIMIT 3");

        let actual = table("Foo").select().order_by("id").offset(2).limit(3);
        test_query_builder(actual, "SELECT * FROM Foo ORDER BY id OFFSET 2 LIMIT 3");
    }

    #[test]
    fn query_builder_preserves_values_terminal_stage_relations() {
        let actual = values(vec!["1"]);
        test_query_builder(actual, "VALUES (1)");

        let actual = values(vec!["1"]).order_by("column1");
        test_query_builder(actual, "VALUES (1) ORDER BY column1");

        let actual = values(vec!["1"]).offset(2);
        test_query_builder(actual, "VALUES (1) OFFSET 2");

        let actual = values(vec!["1"]).order_by("column1").offset(2);
        test_query_builder(actual, "VALUES (1) ORDER BY column1 OFFSET 2");

        let actual = values(vec!["1"]).limit(3);
        test_query_builder(actual, "VALUES (1) LIMIT 3");

        let actual = values(vec!["1"]).order_by("column1").limit(3);
        test_query_builder(actual, "VALUES (1) ORDER BY column1 LIMIT 3");

        let actual = values(vec!["1"]).offset(2).limit(3);
        test_query_builder(actual, "VALUES (1) OFFSET 2 LIMIT 3");

        let actual = values(vec!["1"]).order_by("column1").offset(2).limit(3);
        test_query_builder(actual, "VALUES (1) ORDER BY column1 OFFSET 2 LIMIT 3");
    }
}
