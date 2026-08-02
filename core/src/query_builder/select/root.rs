use {
    super::{
        BuildAggregationInputPlan, BuildFilterInputPlan, BuildProjectInputPlan, BuildSelect,
        BuildSourcePlan, DistinctNode,
    },
    crate::{
        ast::{
            Expr, Literal, Projection, Select, SelectItem, TableAlias, TableFactor, TableWithJoins,
        },
        plan::{
            AggregationInputPlan, DerivedSourcePlan, DictionarySourcePlan, FilterInputPlan,
            ProjectInputPlan, SeriesSourcePlan, SourcePlan, TableAliasPlan, TableSourcePlan,
        },
        query_builder::{
            ExprList, ExprNode, FilterNode, GroupByNode, HavingNode, InnerNestedLoopJoinNode,
            LeftOuterNestedLoopJoinNode, LimitNode, OffsetNode, OrderByExprList, ProjectNode,
            QueryBuilderError, QueryNode, SelectItemList, SelectOrderByNode, SourceNode,
            TableAccessNode,
        },
        result::Result,
        translate::alias_or_name,
    },
};

#[derive(Clone, Debug)]
pub struct SelectNode<'a> {
    source_node: SourceNode<'a>,
}

impl<'a> SelectNode<'a> {
    pub(in crate::query_builder) fn new(source_node: SourceNode<'a>) -> Self {
        Self { source_node }
    }

    pub fn distinct(self) -> DistinctNode<'a> {
        DistinctNode::new(self)
    }

    pub fn filter<T: Into<ExprNode<'a>>>(self, expr: T) -> FilterNode<'a> {
        FilterNode::new(self, expr)
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

    pub fn project<T: Into<SelectItemList<'a>>>(self, select_items: T) -> ProjectNode<'a> {
        ProjectNode::new(self, select_items)
    }

    pub fn order_by<T: Into<OrderByExprList<'a>>>(
        self,
        order_by_exprs: T,
    ) -> SelectOrderByNode<'a> {
        SelectOrderByNode::new(self, order_by_exprs)
    }

    pub fn join(self, table_name: &str) -> InnerNestedLoopJoinNode<'a> {
        InnerNestedLoopJoinNode::from_select(self, table_name.to_owned(), None)
    }

    pub fn join_as(self, table_name: &str, alias: &str) -> InnerNestedLoopJoinNode<'a> {
        InnerNestedLoopJoinNode::from_select(self, table_name.to_owned(), Some(alias.to_owned()))
    }

    pub fn left_join(self, table_name: &str) -> LeftOuterNestedLoopJoinNode<'a> {
        LeftOuterNestedLoopJoinNode::from_select(self, table_name.to_owned(), None)
    }

    pub fn left_join_as(self, table_name: &str, alias: &str) -> LeftOuterNestedLoopJoinNode<'a> {
        LeftOuterNestedLoopJoinNode::from_select(
            self,
            table_name.to_owned(),
            Some(alias.to_owned()),
        )
    }

    pub fn alias_as(self, table_alias: &'a str) -> SourceNode<'a> {
        QueryNode::SelectNode(self).alias_as(table_alias)
    }
}

impl BuildSourcePlan for SelectNode<'_> {
    fn build_source_plan(self) -> Result<SourcePlan> {
        match self.source_node {
            SourceNode::Table {
                name,
                alias,
                access,
            } => Ok(SourcePlan::Table(TableSourcePlan {
                name,
                alias: alias.map(|name| TableAliasPlan {
                    name,
                    columns: Vec::new(),
                }),
                access: access.build_table_access_plan()?,
            })),
            SourceNode::Dictionary { dictionary, alias } => {
                Ok(SourcePlan::Dictionary(DictionarySourcePlan {
                    dictionary,
                    alias: TableAliasPlan {
                        name: alias,
                        columns: Vec::new(),
                    },
                }))
            }
            SourceNode::Series { size, alias } => Ok(SourcePlan::Series(SeriesSourcePlan {
                alias: TableAliasPlan {
                    name: alias,
                    columns: Vec::new(),
                },
                size: size.build_expr_plan()?,
            })),
            SourceNode::Derived { query, alias } => Ok(SourcePlan::Derived(DerivedSourcePlan {
                query: Box::new(query.build_query_plan()?),
                alias: TableAliasPlan {
                    name: alias,
                    columns: Vec::new(),
                },
            })),
        }
    }
}

impl BuildFilterInputPlan for SelectNode<'_> {
    fn build_filter_input_plan(self) -> Result<FilterInputPlan> {
        self.build_source_plan().map(FilterInputPlan::Source)
    }
}

impl BuildAggregationInputPlan for SelectNode<'_> {
    fn build_aggregation_input_plan(self) -> Result<AggregationInputPlan> {
        self.build_source_plan().map(AggregationInputPlan::Source)
    }
}

impl BuildProjectInputPlan for SelectNode<'_> {
    fn build_project_input_plan(self) -> Result<ProjectInputPlan> {
        self.build_source_plan().map(ProjectInputPlan::Source)
    }
}

impl BuildSelect for SelectNode<'_> {
    fn build_select(self) -> Result<Select> {
        let relation = match self.source_node {
            SourceNode::Table {
                name,
                alias,
                access: TableAccessNode::FullScan,
            } => TableFactor::Table {
                name,
                alias: alias.map(|name| TableAlias {
                    name,
                    columns: Vec::new(),
                }),
            },
            SourceNode::Table { .. } => {
                return Err(QueryBuilderError::IndexByRequiresPlan.into());
            }
            SourceNode::Dictionary { dictionary, alias } => TableFactor::Dictionary {
                dict: dictionary,
                alias: alias_or_name(None, alias),
            },
            SourceNode::Series { size, alias } => TableFactor::Series {
                alias: alias_or_name(None, alias),
                size: size.build_expr()?,
            },
            SourceNode::Derived { query, alias } => TableFactor::Derived {
                subquery: query.build_query()?,
                alias: TableAlias {
                    name: alias,
                    columns: Vec::new(),
                },
            },
        };

        let from = TableWithJoins {
            relation,
            joins: Vec::new(),
        };

        Ok(Select {
            distinct: false,
            projection: Projection::SelectItems(vec![SelectItem::Wildcard]),
            from,
            selection: None,
            group_by: Vec::new(),
            having: None,
        })
    }
}

pub fn select<'a>() -> SelectNode<'a> {
    SelectNode {
        source_node: SourceNode::Series {
            size: Expr::Literal(Literal::Number(1.into())).into(),
            alias: "Series".to_owned(),
        },
    }
}

#[cfg(test)]
mod tests {
    use {
        crate::{
            query_builder::{
                QueryBuilderError, primary_key, select, select::BuildSelect, table,
                test_query_builder,
            },
            result::Error,
        },
        pretty_assertions::assert_eq,
    };

    #[test]
    fn select_root() {
        // select node -> build
        let actual = table("App").select();
        let expected = "SELECT * FROM App";
        test_query_builder(actual, expected);

        let actual = table("Item").alias_as("i").select();
        let expected = "SELECT * FROM Item i";
        test_query_builder(actual, expected);

        // select -> derived subquery
        let actual = table("App").select().alias_as("Sub").select();
        let expected = "SELECT * FROM (SELECT * FROM App) Sub";
        test_query_builder(actual, expected);

        // select without table
        let actual = select().project("1 + 1");
        let expected = "SELECT 1 + 1";
        test_query_builder(actual, expected);

        // select distinct
        let actual = table("User").select().distinct();
        let expected = "SELECT DISTINCT * FROM User";
        test_query_builder(actual, expected);

        // select distinct with project
        let actual = table("Item").select().project("name").distinct();
        let expected = "SELECT DISTINCT name FROM Item";
        test_query_builder(actual, expected);
    }

    #[test]
    fn index_by_ast_build_requires_plan() {
        let actual = table("Player")
            .index_by(primary_key().eq("1"))
            .select()
            .build_select();

        assert_eq!(
            actual,
            Err(Error::QueryBuilder(QueryBuilderError::IndexByRequiresPlan))
        );
    }
}
