use {
    super::FilterPlan,
    crate::plan::{
        AggregateExprPlan, ExprPlan, InnerJoinPlan, LeftOuterJoinPlan, RightOuterJoinPlan,
        SourcePlan, UnplannedRightOuterJoinPlan,
    },
    serde::{Deserialize, Serialize},
};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum AggregationInputPlan {
    Source(SourcePlan),
    InnerJoin(Box<InnerJoinPlan>),
    LeftOuterJoin(Box<LeftOuterJoinPlan>),
    UnplannedRightOuterJoin(Box<UnplannedRightOuterJoinPlan>),
    RightOuterJoin(Box<RightOuterJoinPlan>),
    Filter(FilterPlan),
}

impl AggregationInputPlan {
    pub(crate) fn base_source(&self) -> &SourcePlan {
        match self {
            Self::Source(source) => source,
            Self::InnerJoin(join) => join.base_source(),
            Self::LeftOuterJoin(join) => join.base_source(),
            Self::UnplannedRightOuterJoin(join) => join.base_source(),
            Self::RightOuterJoin(join) => join.base_source(),
            Self::Filter(filter) => filter.input.base_source(),
        }
    }

    pub(crate) fn joined_sources(&self) -> Vec<&SourcePlan> {
        match self {
            Self::Source(_) => Vec::new(),
            Self::InnerJoin(join) => join.joined_sources(),
            Self::LeftOuterJoin(join) => join.joined_sources(),
            Self::UnplannedRightOuterJoin(join) => join.joined_sources(),
            Self::RightOuterJoin(join) => join.joined_sources(),
            Self::Filter(filter) => filter.input.joined_sources(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct AggregationPlan {
    pub input: AggregationInputPlan,
    pub group_by: Vec<ExprPlan>,
    pub aggregate_slots: Vec<AggregateExprPlan>,
}

#[cfg(test)]
mod tests {
    use {
        super::{AggregationInputPlan, AggregationPlan},
        crate::{
            data::Value,
            plan::{
                ExprPlan, FilterInputPlan, FilterPlan, InnerJoinInputPlan, InnerJoinPlan,
                LeftOuterJoinInputPlan, LeftOuterJoinPlan, NestedLoopJoinInputPlan,
                NestedLoopJoinPlan, SourcePlan, TableAccessPlan, TableSourcePlan,
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
    fn aggregation_accepts_relation_join_and_filter_inputs() {
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
        let relation = AggregationPlan {
            input: AggregationInputPlan::Source(table("A")),
            group_by: Vec::new(),
            aggregate_slots: Vec::new(),
        };
        let inner = AggregationPlan {
            input: AggregationInputPlan::InnerJoin(Box::new(inner_join.clone())),
            group_by: Vec::new(),
            aggregate_slots: Vec::new(),
        };
        let left_outer = AggregationPlan {
            input: AggregationInputPlan::LeftOuterJoin(Box::new(left_outer_join.clone())),
            group_by: Vec::new(),
            aggregate_slots: Vec::new(),
        };
        let filtered = AggregationPlan {
            input: AggregationInputPlan::Filter(filter.clone()),
            group_by: Vec::new(),
            aggregate_slots: Vec::new(),
        };

        assert_eq!(relation.input, AggregationInputPlan::Source(table("A")));
        assert_eq!(
            inner.input,
            AggregationInputPlan::InnerJoin(Box::new(inner_join))
        );
        assert_eq!(
            left_outer.input,
            AggregationInputPlan::LeftOuterJoin(Box::new(left_outer_join))
        );
        assert_eq!(filtered.input, AggregationInputPlan::Filter(filter));

        assert_eq!(relation.input.base_source(), &table("A"));
        assert_eq!(relation.input.joined_sources(), Vec::<&SourcePlan>::new());
        assert_eq!(inner.input.base_source(), &table("A"));
        let expected = [table("B")];
        assert_eq!(
            inner.input.joined_sources(),
            expected.iter().collect::<Vec<_>>()
        );
        assert_eq!(left_outer.input.base_source(), &table("A"));
        let expected = [table("B")];
        assert_eq!(
            left_outer.input.joined_sources(),
            expected.iter().collect::<Vec<_>>()
        );
        assert_eq!(filtered.input.base_source(), &table("A"));
        let expected = [table("B")];
        assert_eq!(
            filtered.input.joined_sources(),
            expected.iter().collect::<Vec<_>>()
        );
    }
}
