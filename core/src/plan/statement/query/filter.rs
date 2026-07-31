use {
    crate::plan::{ExprPlan, JoinPlan, SourcePlan},
    serde::{Deserialize, Serialize},
};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum FilterInputPlan {
    Source(SourcePlan),
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
                JoinPlan, SourcePlan, TableAccessPlan, TableSourcePlan,
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
    fn filter_accepts_relation_and_join_inputs() {
        let expr = ExprPlan::Value(Value::Bool(true));
        let relation = FilterPlan {
            input: FilterInputPlan::Source(table("A")),
            expr: expr.clone(),
        };
        let join = FilterPlan {
            input: FilterInputPlan::Join(Box::new(JoinPlan {
                input: JoinInputPlan::Source(table("A")),
                right: table("B"),
                join_operator: JoinOperatorPlan::Inner(JoinConstraintPlan::None),
                join_executor: JoinExecutorPlan::NestedLoop,
            })),
            expr,
        };

        assert_eq!(relation.input, FilterInputPlan::Source(table("A")));
        assert_eq!(
            join.input,
            FilterInputPlan::Join(Box::new(JoinPlan {
                input: JoinInputPlan::Source(table("A")),
                right: table("B"),
                join_operator: JoinOperatorPlan::Inner(JoinConstraintPlan::None),
                join_executor: JoinExecutorPlan::NestedLoop,
            }))
        );
    }
}
