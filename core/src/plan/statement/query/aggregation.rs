use {
    super::FilterPlan,
    crate::plan::{
        AggregateExprPlan, ExprPlan, InnerJoinPlan, LeftOuterJoinPlan, SourcePlan,
        explain::{Explain, ExplainContext, ExplainNode},
    },
    serde::{Deserialize, Serialize},
};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum AggregationInputPlan {
    Source(SourcePlan),
    InnerJoin(Box<InnerJoinPlan>),
    LeftOuterJoin(Box<LeftOuterJoinPlan>),
    Filter(FilterPlan),
}

impl AggregationInputPlan {
    pub(crate) fn base_source(&self) -> &SourcePlan {
        match self {
            Self::Source(source) => source,
            Self::InnerJoin(join) => join.base_source(),
            Self::LeftOuterJoin(join) => join.base_source(),
            Self::Filter(filter) => filter.input.base_source(),
        }
    }

    pub(crate) fn joined_sources(&self) -> Vec<&SourcePlan> {
        match self {
            Self::Source(_) => Vec::new(),
            Self::InnerJoin(join) => join.joined_sources(),
            Self::LeftOuterJoin(join) => join.joined_sources(),
            Self::Filter(filter) => filter.input.joined_sources(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct AggregationPlan {
    pub input: AggregationInputPlan,
    pub group_by: Vec<ExprPlan>,
    pub aggregate_slots: Vec<AggregateExprPlan>,
}

impl Explain for AggregationPlan {
    type Output = ExplainNode;

    fn explain(&self, context: &mut ExplainContext) -> ExplainNode {
        ExplainNode::new("aggregate")
            .with_optional_property(
                "group by",
                (!self.group_by.is_empty()).then(|| self.group_by.as_slice().explain(context)),
            )
            .with_optional_property(
                "aggregates",
                (!self.aggregate_slots.is_empty())
                    .then(|| self.aggregate_slots.as_slice().explain(context)),
            )
            .with_child(self.input.explain(context))
    }
}

impl Explain for AggregationInputPlan {
    type Output = ExplainNode;

    fn explain(&self, context: &mut ExplainContext) -> ExplainNode {
        match self {
            Self::Source(source) => source.explain(context),
            Self::InnerJoin(join) => join.explain(context),
            Self::LeftOuterJoin(join) => join.explain(context),
            Self::Filter(filter) => filter.explain(context),
        }
    }
}

#[cfg(test)]
mod tests {
    use {
        super::{AggregationInputPlan, AggregationPlan},
        crate::{
            data::Value,
            plan::{
                AggregateExprPlan, AggregateFunctionPlan, CountArgExprPlan, ExprPlan,
                FilterInputPlan, FilterPlan, InnerJoinInputPlan, InnerJoinPlan,
                LeftOuterJoinInputPlan, LeftOuterJoinPlan, NestedLoopJoinInputPlan,
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

    #[test]
    fn aggregation_accepts_relation_join_and_filter_inputs() {
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
        let filter = FilterPlan {
            input: FilterInputPlan::InnerJoin(Box::new(inner_join.clone())),
            expr: ExprPlan::Value(Value::Bool(true)),
        };
        let relation = AggregationPlan {
            input: AggregationInputPlan::Source(table("A")),
            group_by: Vec::new(),
            aggregate_slots: Vec::new(),
        };
        let inner = AggregationPlan {
            input: AggregationInputPlan::InnerJoin(Box::new(inner_join.clone())),
            group_by: Vec::new(),
            aggregate_slots: Vec::new(),
        };
        let left_outer = AggregationPlan {
            input: AggregationInputPlan::LeftOuterJoin(Box::new(left_outer_join.clone())),
            group_by: Vec::new(),
            aggregate_slots: Vec::new(),
        };
        let filtered = AggregationPlan {
            input: AggregationInputPlan::Filter(filter.clone()),
            group_by: Vec::new(),
            aggregate_slots: Vec::new(),
        };

        assert_eq!(relation.input, AggregationInputPlan::Source(table("A")));
        assert_eq!(
            inner.input,
            AggregationInputPlan::InnerJoin(Box::new(inner_join))
        );
        assert_eq!(
            left_outer.input,
            AggregationInputPlan::LeftOuterJoin(Box::new(left_outer_join))
        );
        assert_eq!(filtered.input, AggregationInputPlan::Filter(filter));

        assert_eq!(relation.input.base_source(), &table("A"));
        assert_eq!(relation.input.joined_sources(), Vec::<&SourcePlan>::new());
        assert_eq!(inner.input.base_source(), &table("A"));
        let expected = [table("B")];
        assert_eq!(
            inner.input.joined_sources(),
            expected.iter().collect::<Vec<_>>()
        );
        assert_eq!(left_outer.input.base_source(), &table("A"));
        let expected = [table("B")];
        assert_eq!(
            left_outer.input.joined_sources(),
            expected.iter().collect::<Vec<_>>()
        );
        assert_eq!(filtered.input.base_source(), &table("A"));
        let expected = [table("B")];
        assert_eq!(
            filtered.input.joined_sources(),
            expected.iter().collect::<Vec<_>>()
        );
    }

    #[test]
    fn explain() {
        let actual = AggregationPlan {
            input: AggregationInputPlan::Source(table("Player")),
            group_by: vec![ExprPlan::Identifier("team_id".to_owned())],
            aggregate_slots: vec![AggregateExprPlan {
                func: AggregateFunctionPlan::Count(CountArgExprPlan::Wildcard),
                distinct: false,
                slot: Some(0),
            }],
        };
        let expected = r"
• aggregate
│ group by: team_id
│ aggregates: COUNT(*)
│
└── • scan Player
      access: full scan
";
        test_explain(&actual, expected);

        let actual = AggregationPlan {
            input: AggregationInputPlan::InnerJoin(Box::new(InnerJoinPlan {
                input: InnerJoinInputPlan::NestedLoop(NestedLoopJoinPlan {
                    input: NestedLoopJoinInputPlan::Source(table("A")),
                    right: table("B"),
                }),
            })),
            group_by: Vec::new(),
            aggregate_slots: Vec::new(),
        };
        let expected = r"
• aggregate
└── • nested-loop join (inner)
    ├── • scan A
    │     access: full scan
    │
    └── • scan B
          access: full scan
";
        test_explain(&actual, expected);

        let actual = AggregationPlan {
            input: AggregationInputPlan::LeftOuterJoin(Box::new(LeftOuterJoinPlan {
                input: LeftOuterJoinInputPlan::NestedLoop(NestedLoopJoinPlan {
                    input: NestedLoopJoinInputPlan::Source(table("A")),
                    right: table("B"),
                }),
            })),
            group_by: Vec::new(),
            aggregate_slots: Vec::new(),
        };
        let expected = r"
• aggregate
└── • nested-loop join (left outer)
    ├── • scan A
    │     access: full scan
    │
    └── • scan B
          access: full scan
";
        test_explain(&actual, expected);

        let actual = AggregationPlan {
            input: AggregationInputPlan::Filter(FilterPlan {
                input: FilterInputPlan::Source(table("Player")),
                expr: ExprPlan::Identifier("active".to_owned()),
            }),
            group_by: Vec::new(),
            aggregate_slots: Vec::new(),
        };
        let expected = r"
• aggregate
└── • filter
    │ expression: active
    │
    └── • scan Player
          access: full scan
";
        test_explain(&actual, expected);
    }
}
