use {
    super::{HashJoinPlan, JoinConditionInputPlan, JoinConditionPlan, NestedLoopJoinPlan},
    crate::plan::SourcePlan,
    serde::{Deserialize, Serialize},
    std::iter,
};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum RightOuterJoinInputPlan {
    NestedLoop(NestedLoopJoinPlan),
    Hash(HashJoinPlan),
    Condition(JoinConditionPlan),
}

impl RightOuterJoinInputPlan {
    /// The left input relations, base first and then in join order, without the right relation.
    pub(crate) fn left_sources(&self) -> Vec<&SourcePlan> {
        fn sources<'a>(base: &'a SourcePlan, joined: Vec<&'a SourcePlan>) -> Vec<&'a SourcePlan> {
            iter::once(base).chain(joined).collect()
        }

        match self {
            Self::NestedLoop(join) => {
                sources(join.input.base_source(), join.input.joined_sources())
            }
            Self::Hash(join) => sources(join.input.base_source(), join.input.joined_sources()),
            Self::Condition(condition) => match &condition.input {
                JoinConditionInputPlan::NestedLoop(join) => {
                    sources(join.input.base_source(), join.input.joined_sources())
                }
                JoinConditionInputPlan::Hash(join) => {
                    sources(join.input.base_source(), join.input.joined_sources())
                }
            },
        }
    }
}

/// The left relations an unmatched right row is NULL-extended with, ordered to line up positionally
/// with the `SelectedSources` the left pipeline produces at execution time.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct NullExtendPlan {
    pub relations: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct RightOuterJoinPlan {
    pub input: RightOuterJoinInputPlan,
    pub null_extend: NullExtendPlan,
}

impl RightOuterJoinPlan {
    pub(crate) fn base_source(&self) -> &SourcePlan {
        match &self.input {
            RightOuterJoinInputPlan::NestedLoop(join) => join.base_source(),
            RightOuterJoinInputPlan::Hash(join) => join.base_source(),
            RightOuterJoinInputPlan::Condition(condition) => condition.base_source(),
        }
    }

    pub(crate) fn base_source_mut(&mut self) -> &mut SourcePlan {
        match &mut self.input {
            RightOuterJoinInputPlan::NestedLoop(join) => join.base_source_mut(),
            RightOuterJoinInputPlan::Hash(join) => join.base_source_mut(),
            RightOuterJoinInputPlan::Condition(condition) => condition.base_source_mut(),
        }
    }

    pub(crate) fn joined_sources(&self) -> Vec<&SourcePlan> {
        match &self.input {
            RightOuterJoinInputPlan::NestedLoop(join) => join.joined_sources(),
            RightOuterJoinInputPlan::Hash(join) => join.joined_sources(),
            RightOuterJoinInputPlan::Condition(condition) => condition.joined_sources(),
        }
    }
}

#[cfg(test)]
mod tests {
    use {
        super::{NullExtendPlan, RightOuterJoinInputPlan, RightOuterJoinPlan},
        crate::{
            data::Value,
            plan::{
                ExprPlan, HashJoinInputPlan, HashJoinPlan, InnerJoinInputPlan, InnerJoinPlan,
                JoinConditionInputPlan, JoinConditionPlan, NestedLoopJoinInputPlan,
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

    fn condition(input: JoinConditionInputPlan) -> JoinConditionPlan {
        JoinConditionPlan {
            input,
            expr: expr(),
        }
    }

    fn null_extend() -> NullExtendPlan {
        NullExtendPlan {
            relations: vec!["A".to_owned()],
        }
    }

    fn plan(input: RightOuterJoinInputPlan) -> RightOuterJoinPlan {
        RightOuterJoinPlan {
            input,
            null_extend: null_extend(),
        }
    }

    #[test]
    fn accepts_each_input() {
        for (mut actual, name) in [
            (
                plan(RightOuterJoinInputPlan::NestedLoop(nested_loop())),
                "nested-loop",
            ),
            (plan(RightOuterJoinInputPlan::Hash(hash())), "hash"),
            (
                plan(RightOuterJoinInputPlan::Condition(condition(
                    JoinConditionInputPlan::NestedLoop(nested_loop()),
                ))),
                "condition",
            ),
        ] {
            assert_eq!(actual.null_extend, null_extend(), "{name}");
            assert_eq!(actual.base_source(), &table("A"), "{name}");
            let expected = [table("B")];
            assert_eq!(
                actual.joined_sources(),
                expected.iter().collect::<Vec<_>>(),
                "{name}"
            );
            let expected = [table("A")];
            assert_eq!(
                actual.input.left_sources(),
                expected.iter().collect::<Vec<_>>(),
                "{name}"
            );
            *actual.base_source_mut() = table(name);
            assert_eq!(actual.base_source(), &table(name), "{name}");
        }
    }

    #[test]
    fn left_sources_accumulate_in_join_order() {
        let inner_join = InnerJoinPlan {
            input: InnerJoinInputPlan::NestedLoop(nested_loop()),
        };
        let input = RightOuterJoinInputPlan::NestedLoop(NestedLoopJoinPlan {
            input: NestedLoopJoinInputPlan::InnerJoin(Box::new(inner_join)),
            right: table("C"),
        });
        let expected = [table("A"), table("B")];

        assert_eq!(input.left_sources(), expected.iter().collect::<Vec<_>>());

        let input = RightOuterJoinInputPlan::Condition(condition(JoinConditionInputPlan::Hash(
            HashJoinPlan {
                input: HashJoinInputPlan::Source(table("A")),
                right: table("C"),
                input_key: expr(),
                right_key: expr(),
                right_filter: None,
            },
        )));
        let expected = [table("A")];

        assert_eq!(input.left_sources(), expected.iter().collect::<Vec<_>>());
    }
}
