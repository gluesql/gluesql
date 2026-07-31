use {
    crate::plan::{ExprPlan, JoinPlan, TableFactorPlan},
    serde::{Deserialize, Serialize},
};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum FilterInputPlan {
    Relation(TableFactorPlan),
    Join(Box<JoinPlan>),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct FilterPlan {
    pub input: FilterInputPlan,
    pub expr: ExprPlan,
}

#[cfg(test)]
mod tests {
    use {
        super::{FilterInputPlan, FilterPlan},
        crate::{
            data::Value,
            plan::{
                ExprPlan, JoinConstraintPlan, JoinExecutorPlan, JoinInputPlan, JoinOperatorPlan,
                JoinPlan, TableFactorPlan,
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
    fn filter_accepts_relation_and_join_inputs() {
        let expr = ExprPlan::Value(Value::Bool(true));
        let relation = FilterPlan {
            input: FilterInputPlan::Relation(table("A")),
            expr: expr.clone(),
        };
        let join = FilterPlan {
            input: FilterInputPlan::Join(Box::new(JoinPlan {
                input: JoinInputPlan::Relation(table("A")),
                relation: table("B"),
                join_operator: JoinOperatorPlan::Inner(JoinConstraintPlan::None),
                join_executor: JoinExecutorPlan::NestedLoop,
            })),
            expr,
        };

        assert_eq!(relation.input, FilterInputPlan::Relation(table("A")));
        assert_eq!(
            join.input,
            FilterInputPlan::Join(Box::new(JoinPlan {
                input: JoinInputPlan::Relation(table("A")),
                relation: table("B"),
                join_operator: JoinOperatorPlan::Inner(JoinConstraintPlan::None),
                join_executor: JoinExecutorPlan::NestedLoop,
            }))
        );
    }
}
