use {
    super::{InnerJoinPlan, LeftOuterJoinPlan, RightOuterJoinPlan},
    crate::plan::{ExprPlan, SourcePlan},
    serde::{Deserialize, Serialize},
};

/// No `UnplannedRightOuterJoin` variant: the hash join planner runs after the right outer join
/// planner, so a hash mechanism can only sit above an already lowered join.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum HashJoinInputPlan {
    Source(SourcePlan),
    InnerJoin(Box<InnerJoinPlan>),
    LeftOuterJoin(Box<LeftOuterJoinPlan>),
    RightOuterJoin(Box<RightOuterJoinPlan>),
}

impl HashJoinInputPlan {
    pub(crate) fn base_source(&self) -> &SourcePlan {
        match self {
            Self::Source(source) => source,
            Self::InnerJoin(join) => join.base_source(),
            Self::LeftOuterJoin(join) => join.base_source(),
            Self::RightOuterJoin(join) => join.base_source(),
        }
    }

    pub(crate) fn base_source_mut(&mut self) -> &mut SourcePlan {
        match self {
            Self::Source(source) => source,
            Self::InnerJoin(join) => join.base_source_mut(),
            Self::LeftOuterJoin(join) => join.base_source_mut(),
            Self::RightOuterJoin(join) => join.base_source_mut(),
        }
    }

    pub(crate) fn joined_sources(&self) -> Vec<&SourcePlan> {
        match self {
            Self::Source(_) => Vec::new(),
            Self::InnerJoin(join) => join.joined_sources(),
            Self::LeftOuterJoin(join) => join.joined_sources(),
            Self::RightOuterJoin(join) => join.joined_sources(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct HashJoinPlan {
    pub input: HashJoinInputPlan,
    pub right: SourcePlan,
    pub input_key: ExprPlan,
    pub right_key: ExprPlan,
    pub right_filter: Option<ExprPlan>,
}

impl HashJoinPlan {
    pub(crate) fn base_source(&self) -> &SourcePlan {
        self.input.base_source()
    }

    pub(crate) fn base_source_mut(&mut self) -> &mut SourcePlan {
        self.input.base_source_mut()
    }

    pub(crate) fn joined_sources(&self) -> Vec<&SourcePlan> {
        let mut sources = self.input.joined_sources();
        sources.push(&self.right);

        sources
    }
}

#[cfg(test)]
mod tests {
    use {
        super::{HashJoinInputPlan, HashJoinPlan},
        crate::{
            data::Value,
            plan::{
                ExprPlan, InnerJoinInputPlan, InnerJoinPlan, LeftOuterJoinInputPlan,
                LeftOuterJoinPlan, NestedLoopJoinInputPlan, NestedLoopJoinPlan, SourcePlan,
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

    fn inner_join() -> InnerJoinPlan {
        InnerJoinPlan {
            input: InnerJoinInputPlan::NestedLoop(NestedLoopJoinPlan {
                input: NestedLoopJoinInputPlan::Source(table("A")),
                right: table("B"),
            }),
        }
    }

    fn left_outer_join() -> LeftOuterJoinPlan {
        LeftOuterJoinPlan {
            input: LeftOuterJoinInputPlan::NestedLoop(NestedLoopJoinPlan {
                input: NestedLoopJoinInputPlan::Source(table("A")),
                right: table("B"),
            }),
        }
    }

    #[test]
    fn accepts_each_input() {
        let mut actual = HashJoinPlan {
            input: HashJoinInputPlan::Source(table("A")),
            right: table("B"),
            input_key: expr(),
            right_key: expr(),
            right_filter: None,
        };
        let expected = HashJoinInputPlan::Source(table("A"));
        assert_eq!(actual.input, expected);
        assert_eq!(actual.base_source(), &table("A"));
        let expected = [table("B")];
        assert_eq!(actual.joined_sources(), expected.iter().collect::<Vec<_>>());
        *actual.base_source_mut() = table("source");
        assert_eq!(actual.base_source(), &table("source"));

        let mut actual = HashJoinPlan {
            input: HashJoinInputPlan::InnerJoin(Box::new(inner_join())),
            right: table("C"),
            input_key: expr(),
            right_key: expr(),
            right_filter: None,
        };
        let expected = HashJoinInputPlan::InnerJoin(Box::new(inner_join()));
        assert_eq!(actual.input, expected);
        assert_eq!(actual.base_source(), &table("A"));
        let expected = [table("B"), table("C")];
        assert_eq!(actual.joined_sources(), expected.iter().collect::<Vec<_>>());
        *actual.base_source_mut() = table("inner");
        assert_eq!(actual.base_source(), &table("inner"));

        let mut actual = HashJoinPlan {
            input: HashJoinInputPlan::LeftOuterJoin(Box::new(left_outer_join())),
            right: table("C"),
            input_key: expr(),
            right_key: expr(),
            right_filter: None,
        };
        let expected = HashJoinInputPlan::LeftOuterJoin(Box::new(left_outer_join()));
        assert_eq!(actual.input, expected);
        assert_eq!(actual.base_source(), &table("A"));
        let expected = [table("B"), table("C")];
        assert_eq!(actual.joined_sources(), expected.iter().collect::<Vec<_>>());
        *actual.base_source_mut() = table("left-outer");
        assert_eq!(actual.base_source(), &table("left-outer"));
    }
}
