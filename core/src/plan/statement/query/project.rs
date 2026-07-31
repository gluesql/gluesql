use {
    super::{AggregationPlan, FilterPlan, HavingPlan},
    crate::plan::{InnerJoinPlan, LeftOuterJoinPlan, ProjectionPlan, SourcePlan},
    serde::{Deserialize, Serialize},
};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ProjectInputPlan {
    Source(SourcePlan),
    InnerJoin(Box<InnerJoinPlan>),
    LeftOuterJoin(Box<LeftOuterJoinPlan>),
    Filter(FilterPlan),
    Aggregation(AggregationPlan),
    Having(HavingPlan),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ProjectPlan {
    pub input: ProjectInputPlan,
    pub projection: ProjectionPlan,
}

#[cfg(test)]
mod tests {
    use {
        super::{ProjectInputPlan, ProjectPlan},
        crate::{
            data::Value,
            plan::{
                AggregationInputPlan, AggregationPlan, ExprPlan, FilterInputPlan, FilterPlan,
                HavingPlan, InnerJoinInputPlan, InnerJoinPlan, LeftOuterJoinInputPlan,
                LeftOuterJoinPlan, NestedLoopJoinInputPlan, NestedLoopJoinPlan, ProjectionPlan,
                SourcePlan, TableAccessPlan, TableSourcePlan,
            },
        },
        pretty_assertions::assert_eq,
    };

    fn table(name: &str) -> SourcePlan {
        SourcePlan::Table(TableSourcePlan {
            name: name.to_owned(),
            alias: None,
            access: TableAccessPlan::FullScan,
        })
    }

    #[test]
    fn project_accepts_each_typed_source_input() {
        let inner_join = InnerJoinPlan {
            input: InnerJoinInputPlan::NestedLoop(NestedLoopJoinPlan {
                input: NestedLoopJoinInputPlan::Source(table("A")),
                right: table("B"),
            }),
        };
        let left_outer_join = LeftOuterJoinPlan {
            input: LeftOuterJoinInputPlan::NestedLoop(NestedLoopJoinPlan {
                input: NestedLoopJoinInputPlan::Source(table("A")),
                right: table("B"),
            }),
        };
        let filter = FilterPlan {
            input: FilterInputPlan::InnerJoin(Box::new(inner_join.clone())),
            expr: ExprPlan::Value(Value::Bool(true)),
        };
        let aggregation = AggregationPlan {
            input: AggregationInputPlan::Filter(filter.clone()),
            group_by: Vec::new(),
            aggregate_slots: Vec::new(),
        };
        let having_plan = HavingPlan {
            input: aggregation.clone(),
            expr: ExprPlan::Value(Value::Bool(true)),
        };
        let projection = ProjectionPlan::SelectItems(Vec::new());

        let relation = ProjectPlan {
            input: ProjectInputPlan::Source(table("A")),
            projection: projection.clone(),
        };
        let inner = ProjectPlan {
            input: ProjectInputPlan::InnerJoin(Box::new(inner_join.clone())),
            projection: projection.clone(),
        };
        let left_outer = ProjectPlan {
            input: ProjectInputPlan::LeftOuterJoin(Box::new(left_outer_join.clone())),
            projection: projection.clone(),
        };
        let filtered = ProjectPlan {
            input: ProjectInputPlan::Filter(filter.clone()),
            projection: projection.clone(),
        };
        let aggregated = ProjectPlan {
            input: ProjectInputPlan::Aggregation(aggregation.clone()),
            projection: projection.clone(),
        };
        let having = ProjectPlan {
            input: ProjectInputPlan::Having(having_plan.clone()),
            projection,
        };

        assert_eq!(relation.input, ProjectInputPlan::Source(table("A")));
        assert_eq!(
            inner.input,
            ProjectInputPlan::InnerJoin(Box::new(inner_join))
        );
        assert_eq!(
            left_outer.input,
            ProjectInputPlan::LeftOuterJoin(Box::new(left_outer_join))
        );
        assert_eq!(filtered.input, ProjectInputPlan::Filter(filter));
        assert_eq!(aggregated.input, ProjectInputPlan::Aggregation(aggregation));
        assert_eq!(having.input, ProjectInputPlan::Having(having_plan));
    }
}
