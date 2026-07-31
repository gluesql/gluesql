use {
    super::FilterPlan,
    crate::plan::{AggregateExprPlan, ExprPlan, InnerJoinPlan, LeftOuterJoinPlan, SourcePlan},
    serde::{Deserialize, Serialize},
};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum AggregationInputPlan {
    Source(SourcePlan),
    InnerJoin(Box<InnerJoinPlan>),
    LeftOuterJoin(Box<LeftOuterJoinPlan>),
    Filter(FilterPlan),
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
    }
}
