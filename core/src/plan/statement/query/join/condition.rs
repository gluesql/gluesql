use {
    super::{HashJoinPlan, NestedLoopJoinPlan},
    crate::plan::ExprPlan,
    serde::{Deserialize, Serialize},
};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum JoinConditionInputPlan {
    NestedLoop(NestedLoopJoinPlan),
    Hash(HashJoinPlan),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct JoinConditionPlan {
    pub input: JoinConditionInputPlan,
    pub expr: ExprPlan,
}

#[cfg(test)]
mod tests {
    use {
        super::{JoinConditionInputPlan, JoinConditionPlan},
        crate::{
            data::Value,
            plan::{
                ExprPlan, HashJoinInputPlan, HashJoinPlan, NestedLoopJoinInputPlan,
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

    #[test]
    fn accepts_each_input() {
        let actual = JoinConditionPlan {
            input: JoinConditionInputPlan::NestedLoop(nested_loop()),
            expr: expr(),
        };
        let expected = JoinConditionInputPlan::NestedLoop(nested_loop());
        assert_eq!(actual.input, expected);

        let actual = JoinConditionPlan {
            input: JoinConditionInputPlan::Hash(hash()),
            expr: expr(),
        };
        let expected = JoinConditionInputPlan::Hash(hash());
        assert_eq!(actual.input, expected);
    }
}
