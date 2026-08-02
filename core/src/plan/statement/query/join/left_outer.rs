use {
    super::{HashJoinPlan, JoinConditionPlan, NestedLoopJoinPlan},
    crate::plan::SourcePlan,
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

impl LeftOuterJoinPlan {
    pub(crate) fn base_source(&self) -> &SourcePlan {
        match &self.input {
            LeftOuterJoinInputPlan::NestedLoop(join) => join.base_source(),
            LeftOuterJoinInputPlan::Hash(join) => join.base_source(),
            LeftOuterJoinInputPlan::Condition(condition) => condition.base_source(),
        }
    }

    pub(crate) fn base_source_mut(&mut self) -> &mut SourcePlan {
        match &mut self.input {
            LeftOuterJoinInputPlan::NestedLoop(join) => join.base_source_mut(),
            LeftOuterJoinInputPlan::Hash(join) => join.base_source_mut(),
            LeftOuterJoinInputPlan::Condition(condition) => condition.base_source_mut(),
        }
    }
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
        let mut actual = LeftOuterJoinPlan {
            input: LeftOuterJoinInputPlan::NestedLoop(nested_loop()),
        };
        let expected = LeftOuterJoinInputPlan::NestedLoop(nested_loop());
        assert_eq!(actual.input, expected);
        assert_eq!(actual.base_source(), &table("A"));
        *actual.base_source_mut() = table("nested-loop");
        assert_eq!(actual.base_source(), &table("nested-loop"));

        let mut actual = LeftOuterJoinPlan {
            input: LeftOuterJoinInputPlan::Hash(hash()),
        };
        let expected = LeftOuterJoinInputPlan::Hash(hash());
        assert_eq!(actual.input, expected);
        assert_eq!(actual.base_source(), &table("A"));
        *actual.base_source_mut() = table("hash");
        assert_eq!(actual.base_source(), &table("hash"));

        let mut actual = LeftOuterJoinPlan {
            input: LeftOuterJoinInputPlan::Condition(condition()),
        };
        let expected = LeftOuterJoinInputPlan::Condition(condition());
        assert_eq!(actual.input, expected);
        assert_eq!(actual.base_source(), &table("A"));
        *actual.base_source_mut() = table("condition");
        assert_eq!(actual.base_source(), &table("condition"));
    }
}
