mod distinct;
mod filter;
mod group_by;
mod having;
mod join;
mod limit;
mod offset;
mod offset_limit;
mod order_by;
mod project;
mod root;
mod values;

use {
    super::Build,
    crate::{
        ast::{Query, Select, SetExpr},
        plan::{
            AggregationInputPlan, AggregationPlan, FilterInputPlan, FilterPlan, HavingPlan,
            JoinInputPlan, ProjectInputPlan, ProjectPlan, ProjectionPlan, QueryPlan,
            SelectItemPlan, StatementPlan, TableFactorPlan,
        },
        result::Result,
    },
};
pub use {
    distinct::DistinctNode,
    filter::FilterNode,
    group_by::GroupByNode,
    having::HavingNode,
    join::{HashJoinNode, JoinConstraintNode, JoinNode},
    limit::LimitNode,
    offset::OffsetNode,
    offset_limit::OffsetLimitNode,
    order_by::{SelectOrderByNode, ValuesOrderByNode},
    project::ProjectNode,
    root::{SelectNode, select},
    values::{ValuesNode, values},
};

pub(super) trait BuildTableFactorPlan {
    fn build_table_factor_plan(self) -> Result<TableFactorPlan>;
}

pub(super) trait BuildJoinInputPlan {
    fn build_join_input_plan(self) -> Result<JoinInputPlan>;
}

pub(super) trait BuildJoinPlan {
    fn build_join_plan(self) -> Result<crate::plan::JoinPlan>;
}

pub(super) trait BuildFilterInputPlan {
    fn build_filter_input_plan(self) -> Result<FilterInputPlan>;
}

pub(super) trait BuildFilterPlan {
    fn build_filter_plan(self) -> Result<FilterPlan>;
}

pub(super) trait BuildAggregationInputPlan {
    fn build_aggregation_input_plan(self) -> Result<AggregationInputPlan>;
}

pub(super) trait BuildAggregationPlan {
    fn build_aggregation_plan(self) -> Result<AggregationPlan>;
}

pub(super) trait BuildHavingPlan {
    fn build_having_plan(self) -> Result<HavingPlan>;
}

pub(super) trait BuildProjectInputPlan {
    fn build_project_input_plan(self) -> Result<ProjectInputPlan>;
}

pub(super) trait BuildProjectPlan {
    fn build_project_plan(self) -> Result<ProjectPlan>;
}

pub(super) trait BuildSelect {
    fn build_select(self) -> Result<Select>;
}

pub(super) trait BuildQueryPlan {
    fn build_query_plan(self) -> Result<QueryPlan>;
}

pub(super) trait BuildQuery {
    fn build_query(self) -> Result<Query>;
}

impl<T: BuildJoinInputPlan> BuildFilterInputPlan for T {
    fn build_filter_input_plan(self) -> Result<FilterInputPlan> {
        self.build_join_input_plan().map(|input| match input {
            JoinInputPlan::Relation(relation) => FilterInputPlan::Relation(relation),
            JoinInputPlan::Join(join) => FilterInputPlan::Join(join),
        })
    }
}

impl<T: BuildJoinInputPlan> BuildProjectInputPlan for T {
    fn build_project_input_plan(self) -> Result<ProjectInputPlan> {
        self.build_join_input_plan().map(|input| match input {
            JoinInputPlan::Relation(relation) => ProjectInputPlan::Relation(relation),
            JoinInputPlan::Join(join) => ProjectInputPlan::Join(join),
        })
    }
}

impl<T: BuildJoinInputPlan> BuildAggregationInputPlan for T {
    fn build_aggregation_input_plan(self) -> Result<AggregationInputPlan> {
        self.build_join_input_plan().map(|input| match input {
            JoinInputPlan::Relation(relation) => AggregationInputPlan::Relation(relation),
            JoinInputPlan::Join(join) => AggregationInputPlan::Join(join),
        })
    }
}

impl<T: BuildProjectInputPlan> BuildProjectPlan for T {
    fn build_project_plan(self) -> Result<ProjectPlan> {
        self.build_project_input_plan().map(|input| ProjectPlan {
            input,
            projection: ProjectionPlan::SelectItems(vec![SelectItemPlan::Wildcard]),
        })
    }
}

impl<T: BuildProjectPlan> BuildQueryPlan for T {
    fn build_query_plan(self) -> Result<QueryPlan> {
        self.build_project_plan().map(QueryPlan::Project)
    }
}

impl<T: BuildSelect> BuildQuery for T {
    fn build_query(self) -> Result<Query> {
        let select = self.build_select()?;
        let body = SetExpr::Select(Box::new(select));
        let query = Query {
            body,
            order_by: Vec::new(),
            limit: None,
            offset: None,
        };

        Ok(query)
    }
}

impl<T: BuildQueryPlan> Build for T {
    fn build(self) -> Result<StatementPlan> {
        let query = self.build_query_plan()?;

        Ok(StatementPlan::Query(query))
    }
}
