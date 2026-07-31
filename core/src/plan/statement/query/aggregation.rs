use {
    super::FilterPlan,
    crate::plan::{AggregateExprPlan, ExprPlan, JoinPlan, TableFactorPlan},
    serde::{Deserialize, Serialize},
};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum AggregationInputPlan {
    Relation(TableFactorPlan),
    Join(Box<JoinPlan>),
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
                ExprPlan, FilterInputPlan, FilterPlan, JoinConstraintPlan, JoinExecutorPlan,
                JoinInputPlan, JoinOperatorPlan, JoinPlan, TableFactorPlan,
            },
        },
        pretty_assertions::assert_eq,
    };

    fn table(name: &str) -> TableFactorPlan {
        TableFactorPlan::Table {
            name: name.to_owned(),
            alias: None,
            index: None,
        }
    }

    #[test]
    fn aggregation_accepts_relation_join_and_filter_inputs() {
        let join = JoinPlan {
            input: JoinInputPlan::Relation(table("A")),
            relation: table("B"),
            join_operator: JoinOperatorPlan::Inner(JoinConstraintPlan::None),
            join_executor: JoinExecutorPlan::NestedLoop,
        };
        let filter = FilterPlan {
            input: FilterInputPlan::Join(Box::new(join.clone())),
            expr: ExprPlan::Value(Value::Bool(true)),
        };
        let relation = AggregationPlan {
            input: AggregationInputPlan::Relation(table("A")),
            group_by: Vec::new(),
            aggregate_slots: Vec::new(),
        };
        let joined = AggregationPlan {
            input: AggregationInputPlan::Join(Box::new(join.clone())),
            group_by: Vec::new(),
            aggregate_slots: Vec::new(),
        };
        let filtered = AggregationPlan {
            input: AggregationInputPlan::Filter(filter.clone()),
            group_by: Vec::new(),
            aggregate_slots: Vec::new(),
        };

        assert_eq!(relation.input, AggregationInputPlan::Relation(table("A")));
        assert_eq!(joined.input, AggregationInputPlan::Join(Box::new(join)));
        assert_eq!(filtered.input, AggregationInputPlan::Filter(filter));
    }
}
