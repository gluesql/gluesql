use {
    super::{FilterPlan, SelectPlan},
    crate::plan::{AggregateExprPlan, ExprPlan},
    serde::{Deserialize, Serialize},
};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum AggregationInputPlan {
    Select(Box<SelectPlan>),
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
            plan::{ExprPlan, FilterPlan, SelectPlan, TableFactorPlan, TableWithJoinsPlan},
        },
        pretty_assertions::assert_eq,
    };

    #[test]
    fn aggregation_accepts_select_input() {
        let select = SelectPlan {
            from: TableWithJoinsPlan {
                relation: TableFactorPlan::Table {
                    name: "Item".to_owned(),
                    alias: None,
                    index: None,
                },
                joins: Vec::new(),
            },
        };
        let group_by = vec![ExprPlan::Identifier("category".to_owned())];
        let inputs = [
            AggregationInputPlan::Select(Box::new(select.clone())),
            AggregationInputPlan::Filter(FilterPlan {
                input: Box::new(select),
                expr: ExprPlan::Value(Value::Bool(true)),
            }),
        ];

        for input in inputs {
            let aggregation = AggregationPlan {
                input: input.clone(),
                group_by: group_by.clone(),
                aggregate_slots: Vec::new(),
            };

            assert_eq!(aggregation.input, input);
            assert_eq!(aggregation.group_by, group_by);
            assert_eq!(aggregation.aggregate_slots, Vec::new());
        }
    }
}
