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
                ExprPlan, HashJoinInputPlan, HashJoinPlan, InnerJoinInputPlan, InnerJoinPlan,
                NestedLoopJoinInputPlan, NestedLoopJoinPlan, SourcePlan, TableAccessPlan,
                TableSourcePlan,
                explain::{Explain, ExplainContext, ExplainNode},
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
    fn explains_join_condition_on_its_algorithm_node() {
        let plan = InnerJoinPlan {
            input: InnerJoinInputPlan::Condition(JoinConditionPlan {
                input: JoinConditionInputPlan::NestedLoop(nested_loop()),
                expr: ExprPlan::Value(Value::Bool(true)),
            }),
        };

        assert_eq!(
            plan.explain(&mut ExplainContext::default()),
            ExplainNode::new("nested-loop join")
                .with_annotation("inner")
                .with_property("condition", "TRUE")
                .with_children([
                    ExplainNode::new("scan A").with_property("access", "full scan"),
                    ExplainNode::new("scan B").with_property("access", "full scan"),
                ])
        );
    }
}
