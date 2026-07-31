use {
    super::AggregationPlan,
    crate::plan::ExprPlan,
    serde::{Deserialize, Serialize},
};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct HavingPlan {
    pub input: AggregationPlan,
    pub expr: ExprPlan,
}

#[cfg(test)]
mod tests {
    use {
        super::HavingPlan,
        crate::{
            data::Value,
            plan::{AggregationPlan, ExprPlan, SelectPlan, TableFactorPlan, TableWithJoinsPlan},
        },
        pretty_assertions::assert_eq,
    };

    #[test]
    fn having_accepts_aggregation_input() {
        let input = AggregationPlan {
            input: Box::new(SelectPlan {
                from: TableWithJoinsPlan {
                    relation: TableFactorPlan::Table {
                        name: "Item".to_owned(),
                        alias: None,
                        index: None,
                    },
                    joins: Vec::new(),
                },
                selection: None,
            }),
            group_by: Vec::new(),
            aggregate_slots: Vec::new(),
        };
        let expr = ExprPlan::Value(Value::Bool(true));
        let having = HavingPlan {
            input: input.clone(),
            expr: expr.clone(),
        };

        assert_eq!(having.input, input);
        assert_eq!(having.expr, expr);
    }
}
