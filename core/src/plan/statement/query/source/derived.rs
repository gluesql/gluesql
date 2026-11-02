use {
    super::TableAliasPlan,
    crate::plan::{
        QueryPlan,
        explain::{Explain, ExplainContext, ExplainNode},
    },
    serde::{Deserialize, Serialize},
};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct DerivedSourcePlan {
    pub query: Box<QueryPlan>,
    pub alias: TableAliasPlan,
}

impl Explain for DerivedSourcePlan {
    type Output = ExplainNode;

    fn explain(&self, context: &mut ExplainContext) -> ExplainNode {
        ExplainNode::new(format!("derived {}", self.alias.name))
            .with_optional_property(
                "columns",
                (!self.alias.columns.is_empty()).then(|| self.alias.columns.join(", ")),
            )
            .with_child(self.query.explain(context))
    }
}

#[cfg(test)]
mod tests {
    use {
        super::{DerivedSourcePlan, TableAliasPlan},
        crate::{
            ast::Literal,
            plan::{ExprPlan, QueryPlan, ValuesPlan, explain::test_explain},
        },
    };

    #[test]
    fn explain() {
        let actual = DerivedSourcePlan {
            query: Box::new(QueryPlan::Values(ValuesPlan(vec![vec![
                ExprPlan::Literal(Literal::Number(1.into())),
            ]]))),
            alias: TableAliasPlan {
                name: "derived".to_owned(),
                columns: vec!["value".to_owned()],
            },
        };
        let expected = r"
• derived derived
│ columns: value
│
└── • values
      size: 1 columns, 1 rows
";
        test_explain(&actual, expected);
    }
}
