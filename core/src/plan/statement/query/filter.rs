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
        let relation = FilterPlan {
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
        let inner = FilterPlan {
            input: FilterInputPlan::InnerJoin(Box::new(inner_join.clone())),
            expr: expr.clone(),
        };
        let left_outer = FilterPlan {
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
    }
}
