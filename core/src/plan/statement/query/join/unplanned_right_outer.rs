use {
    super::{JoinConditionPlan, NestedLoopJoinPlan},
    crate::plan::SourcePlan,
    serde::{Deserialize, Serialize},
};

/// No `Hash` variant: translation only builds nested loop mechanisms, and the hash join planner
/// runs after this state has already been lowered.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum UnplannedRightOuterJoinInputPlan {
    NestedLoop(NestedLoopJoinPlan),
    Condition(JoinConditionPlan),
}

/// Exists only between translation and [`crate::planner::plan_right_outer_join`], which replaces it
/// with a [`super::RightOuterJoinPlan`]. Later planners pass it through untouched and the executor
/// rejects it, so reaching execution means a custom [`crate::store::Planner`] skipped that pass.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct UnplannedRightOuterJoinPlan {
    pub input: UnplannedRightOuterJoinInputPlan,
}

impl UnplannedRightOuterJoinPlan {
    pub(crate) fn base_source(&self) -> &SourcePlan {
        match &self.input {
            UnplannedRightOuterJoinInputPlan::NestedLoop(join) => join.base_source(),
            UnplannedRightOuterJoinInputPlan::Condition(condition) => condition.base_source(),
        }
    }

    pub(crate) fn base_source_mut(&mut self) -> &mut SourcePlan {
        match &mut self.input {
            UnplannedRightOuterJoinInputPlan::NestedLoop(join) => join.base_source_mut(),
            UnplannedRightOuterJoinInputPlan::Condition(condition) => condition.base_source_mut(),
        }
    }

    pub(crate) fn joined_sources(&self) -> Vec<&SourcePlan> {
        match &self.input {
            UnplannedRightOuterJoinInputPlan::NestedLoop(join) => join.joined_sources(),
            UnplannedRightOuterJoinInputPlan::Condition(condition) => condition.joined_sources(),
        }
    }
}

#[cfg(test)]
mod tests {
    use {
        super::{UnplannedRightOuterJoinInputPlan, UnplannedRightOuterJoinPlan},
        crate::{
            data::Value,
            plan::{
                ExprPlan, JoinConditionInputPlan, JoinConditionPlan, NestedLoopJoinInputPlan,
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

    fn nested_loop() -> NestedLoopJoinPlan {
        NestedLoopJoinPlan {
            input: NestedLoopJoinInputPlan::Source(table("A")),
            right: table("B"),
        }
    }

    fn condition() -> JoinConditionPlan {
        JoinConditionPlan {
            input: JoinConditionInputPlan::NestedLoop(nested_loop()),
            expr: ExprPlan::Value(Value::Bool(true)),
        }
    }

    #[test]
    fn accepts_each_input() {
        let mut actual = UnplannedRightOuterJoinPlan {
            input: UnplannedRightOuterJoinInputPlan::NestedLoop(nested_loop()),
        };
        let expected = UnplannedRightOuterJoinInputPlan::NestedLoop(nested_loop());
        assert_eq!(actual.input, expected);
        assert_eq!(actual.base_source(), &table("A"));
        let expected = [table("B")];
        assert_eq!(actual.joined_sources(), expected.iter().collect::<Vec<_>>());
        *actual.base_source_mut() = table("nested-loop");
        assert_eq!(actual.base_source(), &table("nested-loop"));

        let mut actual = UnplannedRightOuterJoinPlan {
            input: UnplannedRightOuterJoinInputPlan::Condition(condition()),
        };
        let expected = UnplannedRightOuterJoinInputPlan::Condition(condition());
        assert_eq!(actual.input, expected);
        assert_eq!(actual.base_source(), &table("A"));
        let expected = [table("B")];
        assert_eq!(actual.joined_sources(), expected.iter().collect::<Vec<_>>());
        *actual.base_source_mut() = table("condition");
        assert_eq!(actual.base_source(), &table("condition"));
    }
}
