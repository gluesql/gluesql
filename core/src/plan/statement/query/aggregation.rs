use {
    super::SelectPlan,
    crate::plan::{AggregateExprPlan, ExprPlan},
    serde::{Deserialize, Serialize},
};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct AggregationPlan {
    pub input: Box<SelectPlan>,
    pub group_by: Vec<ExprPlan>,
    pub aggregate_slots: Vec<AggregateExprPlan>,
}

#[cfg(test)]
mod tests {
    use {
        super::AggregationPlan,
        crate::plan::{ExprPlan, SelectPlan, TableFactorPlan, TableWithJoinsPlan},
        pretty_assertions::assert_eq,
    };

    #[test]
    fn aggregation_accepts_select_input() {
        let input = SelectPlan {
            from: TableWithJoinsPlan {
                relation: TableFactorPlan::Table {
                    name: "Item".to_owned(),
                    alias: None,
                    index: None,
                },
                joins: Vec::new(),
            },
            selection: None,
        };
        let group_by = vec![ExprPlan::Identifier("category".to_owned())];
        let aggregation = AggregationPlan {
            input: Box::new(input.clone()),
            group_by: group_by.clone(),
            aggregate_slots: Vec::new(),
        };

        assert_eq!(*aggregation.input, input);
        assert_eq!(aggregation.group_by, group_by);
        assert_eq!(aggregation.aggregate_slots, Vec::new());
    }
}
