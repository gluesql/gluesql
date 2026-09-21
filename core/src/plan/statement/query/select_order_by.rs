use {
    super::ProjectPlan,
    crate::plan::{
        OrderByExprPlan,
        explain::{Explain, ExplainContext, ExplainNode},
    },
    serde::{Deserialize, Serialize},
};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SelectOrderByPlan {
    pub input: ProjectPlan,
    pub exprs: Vec<OrderByExprPlan>,
}

impl Explain for SelectOrderByPlan {
    type Output = ExplainNode;

    fn explain(&self, context: &mut ExplainContext) -> ExplainNode {
        ExplainNode::new("sort")
            .with_property("order", self.exprs.as_slice().explain(context))
            .with_child(self.input.explain(context))
    }
}

#[cfg(test)]
mod tests {
    use {
        super::SelectOrderByPlan,
        crate::plan::{
            ExprPlan, OrderByExprPlan, ProjectInputPlan, ProjectPlan, ProjectionPlan,
            SelectItemPlan, SourcePlan, TableAccessPlan, TableSourcePlan, explain::test_explain,
        },
    };

    #[test]
    fn explain() {
        let actual = SelectOrderByPlan {
            input: ProjectPlan {
                input: ProjectInputPlan::Source(SourcePlan::Table(TableSourcePlan {
                    name: "Player".to_owned(),
                    alias: None,
                    access: TableAccessPlan::FullScan,
                })),
                projection: ProjectionPlan::SelectItems(vec![SelectItemPlan::Wildcard]),
            },
            exprs: vec![OrderByExprPlan {
                expr: ExprPlan::Identifier("id".to_owned()),
                asc: Some(false),
            }],
        };
        let expected = r"
• sort
│ order: id DESC
│
└── • project
    │ columns: *
    │
    └── • scan Player
          access: full scan
";
        test_explain(&actual, expected);
    }
}
