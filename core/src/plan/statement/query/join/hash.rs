use {
    super::{InnerJoinPlan, LeftOuterJoinPlan},
    crate::plan::{
        ExprPlan, SourcePlan,
        explain::{Explain, ExplainContext, ExplainNode},
    },
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

impl HashJoinPlan {
    pub(crate) fn base_source(&self) -> &SourcePlan {
        match &self.input {
            HashJoinInputPlan::Source(source) => source,
            HashJoinInputPlan::InnerJoin(join) => join.base_source(),
            HashJoinInputPlan::LeftOuterJoin(join) => join.base_source(),
        }
    }

    pub(crate) fn base_source_mut(&mut self) -> &mut SourcePlan {
        match &mut self.input {
            HashJoinInputPlan::Source(source) => source,
            HashJoinInputPlan::InnerJoin(join) => join.base_source_mut(),
            HashJoinInputPlan::LeftOuterJoin(join) => join.base_source_mut(),
        }
    }

    pub(crate) fn joined_sources(&self) -> Vec<&SourcePlan> {
        let mut sources = match &self.input {
            HashJoinInputPlan::Source(_) => Vec::new(),
            HashJoinInputPlan::InnerJoin(join) => join.joined_sources(),
            HashJoinInputPlan::LeftOuterJoin(join) => join.joined_sources(),
        };
        sources.push(&self.right);

        sources
    }
}

impl Explain for HashJoinPlan {
    type Output = ExplainNode;

    fn explain(&self, context: &mut ExplainContext) -> ExplainNode {
        let equality = format!(
            "{} = {}",
            self.input_key.explain(context),
            self.right_key.explain(context)
        );
        let right_filter = self.right_filter.as_ref().map(|expr| expr.explain(context));

        ExplainNode::new("hash join")
            .with_property("equality", equality)
            .with_optional_property("right filter", right_filter)
            .with_children([self.input.explain(context), self.right.explain(context)])
    }
}

impl Explain for HashJoinInputPlan {
    type Output = ExplainNode;

    fn explain(&self, context: &mut ExplainContext) -> ExplainNode {
        match self {
            Self::Source(source) => source.explain(context),
            Self::InnerJoin(join) => join.explain(context),
            Self::LeftOuterJoin(join) => join.explain(context),
        }
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

    #[test]
    fn explains_hash_join_node() {
        let plan = InnerJoinPlan {
            input: InnerJoinInputPlan::Hash(HashJoinPlan {
                input: HashJoinInputPlan::Source(table("A")),
                right: table("B"),
                input_key: ExprPlan::CompoundIdentifier {
                    alias: "A".to_owned(),
                    ident: "id".to_owned(),
                },
                right_key: ExprPlan::CompoundIdentifier {
                    alias: "B".to_owned(),
                    ident: "id".to_owned(),
                },
                right_filter: Some(ExprPlan::Value(Value::Bool(true))),
            }),
        };

        assert_eq!(
            plan.explain(&mut ExplainContext::default()),
            ExplainNode::new("hash join")
                .with_annotation("inner")
                .with_property("equality", "A.id = B.id")
                .with_property("right filter", "TRUE")
                .with_children([
                    ExplainNode::new("scan A").with_property("access", "full scan"),
                    ExplainNode::new("scan B").with_property("access", "full scan"),
                ])
        );
    }
}
