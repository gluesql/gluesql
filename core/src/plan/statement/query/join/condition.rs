use {
    super::{HashJoinPlan, NestedLoopJoinPlan},
    crate::plan::{
        ExprPlan, SourcePlan,
        explain::{Explain, ExplainContext, ExplainNode},
    },
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

impl JoinConditionPlan {
    pub(crate) fn base_source(&self) -> &SourcePlan {
        match &self.input {
            JoinConditionInputPlan::NestedLoop(join) => join.base_source(),
            JoinConditionInputPlan::Hash(join) => join.base_source(),
        }
    }

    pub(crate) fn base_source_mut(&mut self) -> &mut SourcePlan {
        match &mut self.input {
            JoinConditionInputPlan::NestedLoop(join) => join.base_source_mut(),
            JoinConditionInputPlan::Hash(join) => join.base_source_mut(),
        }
    }

    pub(crate) fn joined_sources(&self) -> Vec<&SourcePlan> {
        match &self.input {
            JoinConditionInputPlan::NestedLoop(join) => join.joined_sources(),
            JoinConditionInputPlan::Hash(join) => join.joined_sources(),
        }
    }
}

impl Explain for JoinConditionPlan {
    type Output = ExplainNode;

    fn explain(&self, context: &mut ExplainContext) -> ExplainNode {
        let node = match &self.input {
            JoinConditionInputPlan::NestedLoop(join) => join.explain(context),
            JoinConditionInputPlan::Hash(join) => join.explain(context),
        };
        node.with_property("condition", self.expr.explain(context))
    }
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
                explain::test_explain,
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
        let mut actual = JoinConditionPlan {
            input: JoinConditionInputPlan::NestedLoop(nested_loop()),
            expr: expr(),
        };
        let expected = JoinConditionInputPlan::NestedLoop(nested_loop());
        assert_eq!(actual.input, expected);
        assert_eq!(actual.base_source(), &table("A"));
        let expected = [table("B")];
        assert_eq!(actual.joined_sources(), expected.iter().collect::<Vec<_>>());
        *actual.base_source_mut() = table("nested-loop");
        assert_eq!(actual.base_source(), &table("nested-loop"));

        let mut actual = JoinConditionPlan {
            input: JoinConditionInputPlan::Hash(hash()),
            expr: expr(),
        };
        let expected = JoinConditionInputPlan::Hash(hash());
        assert_eq!(actual.input, expected);
        assert_eq!(actual.base_source(), &table("A"));
        let expected = [table("B")];
        assert_eq!(actual.joined_sources(), expected.iter().collect::<Vec<_>>());
        *actual.base_source_mut() = table("hash");
        assert_eq!(actual.base_source(), &table("hash"));
    }

    #[test]
    fn explain() {
        let actual = JoinConditionPlan {
            input: JoinConditionInputPlan::NestedLoop(nested_loop()),
            expr: ExprPlan::Value(Value::Bool(true)),
        };
        let expected = r"
• nested-loop join
│ condition: TRUE
│
├── • scan A
│     access: full scan
│
└── • scan B
      access: full scan
";
        test_explain(&actual, expected);

        let actual = JoinConditionPlan {
            input: JoinConditionInputPlan::Hash(hash()),
            expr: ExprPlan::Value(Value::Bool(true)),
        };
        let expected = r"
• hash join
│ equality: TRUE = TRUE
│ condition: TRUE
│
├── • scan A
│     access: full scan
│
└── • scan B
      access: full scan
";
        test_explain(&actual, expected);
    }
}
