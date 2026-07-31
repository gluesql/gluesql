use {
    super::{InnerJoinPlan, LeftOuterJoinPlan},
    crate::plan::{ExprPlan, SourcePlan},
    serde::{Deserialize, Serialize},
};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum HashJoinInputPlan {
    Source(SourcePlan),
    InnerJoin(Box<InnerJoinPlan>),
    LeftOuterJoin(Box<LeftOuterJoinPlan>),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct HashJoinPlan {
    pub input: HashJoinInputPlan,
    pub right: SourcePlan,
    pub input_key: ExprPlan,
    pub right_key: ExprPlan,
    pub right_filter: Option<ExprPlan>,
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
        let actual = HashJoinPlan {
            input: HashJoinInputPlan::Source(table("A")),
            right: table("B"),
            input_key: expr(),
            right_key: expr(),
            right_filter: None,
        };
        let expected = HashJoinInputPlan::Source(table("A"));
        assert_eq!(actual.input, expected);

        let actual = HashJoinPlan {
            input: HashJoinInputPlan::InnerJoin(Box::new(inner_join())),
            right: table("C"),
            input_key: expr(),
            right_key: expr(),
            right_filter: None,
        };
        let expected = HashJoinInputPlan::InnerJoin(Box::new(inner_join()));
        assert_eq!(actual.input, expected);

        let actual = HashJoinPlan {
            input: HashJoinInputPlan::LeftOuterJoin(Box::new(left_outer_join())),
            right: table("C"),
            input_key: expr(),
            right_key: expr(),
            right_filter: None,
        };
        let expected = HashJoinInputPlan::LeftOuterJoin(Box::new(left_outer_join()));
        assert_eq!(actual.input, expected);
    }
}
