use {
    super::{HashJoinPlan, JoinConditionPlan, NestedLoopJoinPlan},
    crate::plan::{
        SourcePlan,
        explain::{Explain, ExplainContext, ExplainNode},
    },
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

impl InnerJoinPlan {
    pub(crate) fn base_source(&self) -> &SourcePlan {
        match &self.input {
            InnerJoinInputPlan::NestedLoop(join) => join.base_source(),
            InnerJoinInputPlan::Hash(join) => join.base_source(),
            InnerJoinInputPlan::Condition(condition) => condition.base_source(),
        }
    }

    pub(crate) fn base_source_mut(&mut self) -> &mut SourcePlan {
        match &mut self.input {
            InnerJoinInputPlan::NestedLoop(join) => join.base_source_mut(),
            InnerJoinInputPlan::Hash(join) => join.base_source_mut(),
            InnerJoinInputPlan::Condition(condition) => condition.base_source_mut(),
        }
    }

    pub(crate) fn joined_sources(&self) -> Vec<&SourcePlan> {
        match &self.input {
            InnerJoinInputPlan::NestedLoop(join) => join.joined_sources(),
            InnerJoinInputPlan::Hash(join) => join.joined_sources(),
            InnerJoinInputPlan::Condition(condition) => condition.joined_sources(),
        }
    }
}

impl Explain for InnerJoinPlan {
    type Output = ExplainNode;

    fn explain(&self, context: &mut ExplainContext) -> ExplainNode {
        self.input.explain(context).with_annotation("inner")
    }
}

impl Explain for InnerJoinInputPlan {
    type Output = ExplainNode;

    fn explain(&self, context: &mut ExplainContext) -> ExplainNode {
        match self {
            Self::NestedLoop(join) => join.explain(context),
            Self::Hash(join) => join.explain(context),
            Self::Condition(condition) => condition.explain(context),
        }
    }
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
                TableAccessPlan, TableSourcePlan, explain::test_explain,
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
        let mut actual = InnerJoinPlan {
            input: InnerJoinInputPlan::NestedLoop(nested_loop()),
        };
        let expected = InnerJoinInputPlan::NestedLoop(nested_loop());
        assert_eq!(actual.input, expected);
        assert_eq!(actual.base_source(), &table("A"));
        let expected = [table("B")];
        assert_eq!(actual.joined_sources(), expected.iter().collect::<Vec<_>>());
        *actual.base_source_mut() = table("nested-loop");
        assert_eq!(actual.base_source(), &table("nested-loop"));

        let mut actual = InnerJoinPlan {
            input: InnerJoinInputPlan::Hash(hash()),
        };
        let expected = InnerJoinInputPlan::Hash(hash());
        assert_eq!(actual.input, expected);
        assert_eq!(actual.base_source(), &table("A"));
        let expected = [table("B")];
        assert_eq!(actual.joined_sources(), expected.iter().collect::<Vec<_>>());
        *actual.base_source_mut() = table("hash");
        assert_eq!(actual.base_source(), &table("hash"));

        let mut actual = InnerJoinPlan {
            input: InnerJoinInputPlan::Condition(condition()),
        };
        let expected = InnerJoinInputPlan::Condition(condition());
        assert_eq!(actual.input, expected);
        assert_eq!(actual.base_source(), &table("A"));
        let expected = [table("B")];
        assert_eq!(actual.joined_sources(), expected.iter().collect::<Vec<_>>());
        *actual.base_source_mut() = table("condition");
        assert_eq!(actual.base_source(), &table("condition"));
    }

    #[test]
    fn explain() {
        let actual = InnerJoinPlan {
            input: InnerJoinInputPlan::NestedLoop(nested_loop()),
        };
        let expected = r"
• nested-loop join (inner)
├── • scan A
│     access: full scan
│
└── • scan B
      access: full scan
";
        test_explain(&actual, expected);

        let actual = InnerJoinPlan {
            input: InnerJoinInputPlan::Hash(hash()),
        };
        let expected = r"
• hash join (inner)
│ equality: TRUE = TRUE
│
├── • scan A
│     access: full scan
│
└── • scan B
      access: full scan
";
        test_explain(&actual, expected);

        let actual = InnerJoinPlan {
            input: InnerJoinInputPlan::Condition(condition()),
        };
        let expected = r"
• nested-loop join (inner)
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
