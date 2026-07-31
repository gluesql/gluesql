use {
    super::{HashJoinPlan, JoinConditionPlan, NestedLoopJoinPlan},
    serde::{Deserialize, Serialize},
};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum LeftOuterJoinInputPlan {
    NestedLoop(NestedLoopJoinPlan),
    Hash(HashJoinPlan),
    Condition(JoinConditionPlan),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct LeftOuterJoinPlan {
    pub input: LeftOuterJoinInputPlan,
}

#[cfg(test)]
mod tests {
    use {
        super::{LeftOuterJoinInputPlan, LeftOuterJoinPlan},
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
        let actual = LeftOuterJoinPlan {
            input: LeftOuterJoinInputPlan::NestedLoop(nested_loop()),
        };
        let expected = LeftOuterJoinInputPlan::NestedLoop(nested_loop());
        assert_eq!(actual.input, expected);

        let actual = LeftOuterJoinPlan {
            input: LeftOuterJoinInputPlan::Hash(hash()),
        };
        let expected = LeftOuterJoinInputPlan::Hash(hash());
        assert_eq!(actual.input, expected);

        let actual = LeftOuterJoinPlan {
            input: LeftOuterJoinInputPlan::Condition(condition()),
        };
        let expected = LeftOuterJoinInputPlan::Condition(condition());
        assert_eq!(actual.input, expected);
    }
}
