use {
    crate::plan::{ExprPlan, InnerJoinPlan, LeftOuterJoinPlan, SourcePlan},
    serde::{Deserialize, Serialize},
};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum FilterInputPlan {
    Source(SourcePlan),
    InnerJoin(Box<InnerJoinPlan>),
    LeftOuterJoin(Box<LeftOuterJoinPlan>),
}

impl FilterInputPlan {
    pub(crate) fn base_source(&self) -> &SourcePlan {
        match self {
            Self::Source(source) => source,
            Self::InnerJoin(join) => join.base_source(),
            Self::LeftOuterJoin(join) => join.base_source(),
        }
    }

    pub(crate) fn base_source_mut(&mut self) -> &mut SourcePlan {
        match self {
            Self::Source(source) => source,
            Self::InnerJoin(join) => join.base_source_mut(),
            Self::LeftOuterJoin(join) => join.base_source_mut(),
        }
    }

    pub(crate) fn joined_sources(&self) -> Vec<&SourcePlan> {
        match self {
            Self::Source(_) => Vec::new(),
            Self::InnerJoin(join) => join.joined_sources(),
            Self::LeftOuterJoin(join) => join.joined_sources(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct FilterPlan {
    pub input: FilterInputPlan,
    pub expr: ExprPlan,
}

#[cfg(test)]
mod tests {
    use {
        super::{FilterInputPlan, FilterPlan},
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

    #[test]
    fn filter_accepts_relation_and_join_inputs() {
        let expr = ExprPlan::Value(Value::Bool(true));
        let mut relation = FilterPlan {
            input: FilterInputPlan::Source(table("A")),
            expr: expr.clone(),
        };
        let inner_join = InnerJoinPlan {
            input: InnerJoinInputPlan::NestedLoop(NestedLoopJoinPlan {
                input: NestedLoopJoinInputPlan::Source(table("A")),
                right: table("B"),
            }),
        };
        let left_outer_join = LeftOuterJoinPlan {
            input: LeftOuterJoinInputPlan::NestedLoop(NestedLoopJoinPlan {
                input: NestedLoopJoinInputPlan::Source(table("A")),
                right: table("B"),
            }),
        };
        let mut inner = FilterPlan {
            input: FilterInputPlan::InnerJoin(Box::new(inner_join.clone())),
            expr: expr.clone(),
        };
        let mut left_outer = FilterPlan {
            input: FilterInputPlan::LeftOuterJoin(Box::new(left_outer_join.clone())),
            expr,
        };

        assert_eq!(relation.input, FilterInputPlan::Source(table("A")));
        assert_eq!(
            inner.input,
            FilterInputPlan::InnerJoin(Box::new(inner_join))
        );
        assert_eq!(
            left_outer.input,
            FilterInputPlan::LeftOuterJoin(Box::new(left_outer_join))
        );

        assert_eq!(relation.input.base_source(), &table("A"));
        assert_eq!(relation.input.joined_sources(), Vec::<&SourcePlan>::new());
        *relation.input.base_source_mut() = table("source");
        assert_eq!(relation.input.base_source(), &table("source"));

        assert_eq!(inner.input.base_source(), &table("A"));
        let expected = [table("B")];
        assert_eq!(
            inner.input.joined_sources(),
            expected.iter().collect::<Vec<_>>()
        );
        *inner.input.base_source_mut() = table("inner");
        assert_eq!(inner.input.base_source(), &table("inner"));

        assert_eq!(left_outer.input.base_source(), &table("A"));
        let expected = [table("B")];
        assert_eq!(
            left_outer.input.joined_sources(),
            expected.iter().collect::<Vec<_>>()
        );
        *left_outer.input.base_source_mut() = table("left-outer");
        assert_eq!(left_outer.input.base_source(), &table("left-outer"));
    }
}
