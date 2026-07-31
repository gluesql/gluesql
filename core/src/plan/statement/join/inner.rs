use {
    super::{HashJoinPlan, JoinConditionPlan, NestedLoopJoinPlan},
    serde::{Deserialize, Serialize},
};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum InnerJoinInputPlan {
    NestedLoop(NestedLoopJoinPlan),
    Hash(HashJoinPlan),
    Condition(JoinConditionPlan),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct InnerJoinPlan {
    pub input: InnerJoinInputPlan,
}

#[cfg(test)]
mod tests {
    use {
        super::{InnerJoinInputPlan, InnerJoinPlan},
        crate::{
            data::Value,
            plan::{
                ExprPlan, HashJoinInputPlan, HashJoinPlan, JoinConditionInputPlan,
                JoinConditionPlan, NestedLoopJoinInputPlan, NestedLoopJoinPlan, SourcePlan,
                TableAccessPlan, TableSourcePlan,
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

    fn expr() -> ExprPlan {
        ExprPlan::Value(Value::Bool(true))
    }

    fn nested_loop() -> NestedLoopJoinPlan {
        NestedLoopJoinPlan {
            input: NestedLoopJoinInputPlan::Source(table("A")),
            right: table("B"),
        }
    }

    fn hash() -> HashJoinPlan {
        HashJoinPlan {
            input: HashJoinInputPlan::Source(table("A")),
            right: table("B"),
            input_key: expr(),
            right_key: expr(),
            right_filter: None,
        }
    }

    fn condition() -> JoinConditionPlan {
        JoinConditionPlan {
            input: JoinConditionInputPlan::NestedLoop(nested_loop()),
            expr: expr(),
        }
    }

    #[test]
    fn accepts_each_input() {
        let actual = InnerJoinPlan {
            input: InnerJoinInputPlan::NestedLoop(nested_loop()),
        };
        let expected = InnerJoinInputPlan::NestedLoop(nested_loop());
        assert_eq!(actual.input, expected);

        let actual = InnerJoinPlan {
            input: InnerJoinInputPlan::Hash(hash()),
        };
        let expected = InnerJoinInputPlan::Hash(hash());
        assert_eq!(actual.input, expected);

        let actual = InnerJoinPlan {
            input: InnerJoinInputPlan::Condition(condition()),
        };
        let expected = InnerJoinInputPlan::Condition(condition());
        assert_eq!(actual.input, expected);
    }
}
