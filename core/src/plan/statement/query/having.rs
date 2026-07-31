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
            plan::{AggregationInputPlan, AggregationPlan, ExprPlan, TableFactorPlan},
        },
        pretty_assertions::assert_eq,
    };

    #[test]
    fn having_accepts_aggregation_input() {
        let input = AggregationPlan {
            input: AggregationInputPlan::Relation(TableFactorPlan::Table {
                name: "Item".to_owned(),
                alias: None,
                index: None,
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
