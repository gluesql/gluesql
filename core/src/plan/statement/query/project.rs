use {
    super::{AggregationPlan, FilterPlan, HavingPlan},
    crate::plan::{JoinPlan, ProjectionPlan, SourcePlan},
    serde::{Deserialize, Serialize},
};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ProjectInputPlan {
    Source(SourcePlan),
    Join(Box<JoinPlan>),
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
                HavingPlan, JoinConstraintPlan, JoinExecutorPlan, JoinInputPlan, JoinOperatorPlan,
                JoinPlan, ProjectionPlan, SourcePlan, TableAccessPlan, TableSourcePlan,
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
        let join = JoinPlan {
            input: JoinInputPlan::Source(table("A")),
            right: table("B"),
            join_operator: JoinOperatorPlan::Inner(JoinConstraintPlan::None),
            join_executor: JoinExecutorPlan::NestedLoop,
        };
        let filter = FilterPlan {
            input: FilterInputPlan::Join(Box::new(join.clone())),
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
        let joined = ProjectPlan {
            input: ProjectInputPlan::Join(Box::new(join.clone())),
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
        assert_eq!(joined.input, ProjectInputPlan::Join(Box::new(join)));
        assert_eq!(filtered.input, ProjectInputPlan::Filter(filter));
        assert_eq!(aggregated.input, ProjectInputPlan::Aggregation(aggregation));
        assert_eq!(having.input, ProjectInputPlan::Having(having_plan));
    }
}
