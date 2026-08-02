use {
    super::{InnerJoinPlan, LeftOuterJoinPlan},
    crate::plan::SourcePlan,
    serde::{Deserialize, Serialize},
};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum NestedLoopJoinInputPlan {
    Source(SourcePlan),
    InnerJoin(Box<InnerJoinPlan>),
    LeftOuterJoin(Box<LeftOuterJoinPlan>),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct NestedLoopJoinPlan {
    pub input: NestedLoopJoinInputPlan,
    pub right: SourcePlan,
}

impl NestedLoopJoinPlan {
    pub(crate) fn base_source(&self) -> &SourcePlan {
        match &self.input {
            NestedLoopJoinInputPlan::Source(source) => source,
            NestedLoopJoinInputPlan::InnerJoin(join) => join.base_source(),
            NestedLoopJoinInputPlan::LeftOuterJoin(join) => join.base_source(),
        }
    }

    pub(crate) fn base_source_mut(&mut self) -> &mut SourcePlan {
        match &mut self.input {
            NestedLoopJoinInputPlan::Source(source) => source,
            NestedLoopJoinInputPlan::InnerJoin(join) => join.base_source_mut(),
            NestedLoopJoinInputPlan::LeftOuterJoin(join) => join.base_source_mut(),
        }
    }
}

#[cfg(test)]
mod tests {
    use {
        super::{NestedLoopJoinInputPlan, NestedLoopJoinPlan},
        crate::plan::{
            InnerJoinInputPlan, InnerJoinPlan, LeftOuterJoinInputPlan, LeftOuterJoinPlan,
            SourcePlan, TableAccessPlan, TableSourcePlan,
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
        let mut actual = NestedLoopJoinPlan {
            input: NestedLoopJoinInputPlan::Source(table("A")),
            right: table("B"),
        };
        let expected = NestedLoopJoinInputPlan::Source(table("A"));
        assert_eq!(actual.input, expected);
        assert_eq!(actual.base_source(), &table("A"));
        *actual.base_source_mut() = table("source");
        assert_eq!(actual.base_source(), &table("source"));

        let mut actual = NestedLoopJoinPlan {
            input: NestedLoopJoinInputPlan::InnerJoin(Box::new(inner_join())),
            right: table("C"),
        };
        let expected = NestedLoopJoinInputPlan::InnerJoin(Box::new(inner_join()));
        assert_eq!(actual.input, expected);
        assert_eq!(actual.base_source(), &table("A"));
        *actual.base_source_mut() = table("inner");
        assert_eq!(actual.base_source(), &table("inner"));

        let mut actual = NestedLoopJoinPlan {
            input: NestedLoopJoinInputPlan::LeftOuterJoin(Box::new(left_outer_join())),
            right: table("C"),
        };
        let expected = NestedLoopJoinInputPlan::LeftOuterJoin(Box::new(left_outer_join()));
        assert_eq!(actual.input, expected);
        assert_eq!(actual.base_source(), &table("A"));
        *actual.base_source_mut() = table("left-outer");
        assert_eq!(actual.base_source(), &table("left-outer"));
    }
}
