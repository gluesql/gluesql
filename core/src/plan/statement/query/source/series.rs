use {
    super::TableAliasPlan,
    crate::plan::{
        ExprPlan,
        explain::{Explain, ExplainContext, ExplainNode},
    },
    serde::{Deserialize, Serialize},
};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SeriesSourcePlan {
    pub alias: TableAliasPlan,
    pub size: ExprPlan,
}

impl Explain for SeriesSourcePlan {
    type Output = ExplainNode;

    fn explain(&self, context: &mut ExplainContext) -> ExplainNode {
        ExplainNode::new(format!("series {}", self.alias.name))
            .with_property("size", self.size.explain(context))
    }
}

#[cfg(test)]
mod tests {
    use {
        super::{SeriesSourcePlan, TableAliasPlan},
        crate::{
            ast::Literal,
            plan::{ExprPlan, explain::test_explain},
        },
    };

    #[test]
    fn explain() {
        let actual = SeriesSourcePlan {
            alias: TableAliasPlan {
                name: "numbers".to_owned(),
                columns: vec!["number".to_owned()],
            },
            size: ExprPlan::Literal(Literal::Number(3.into())),
        };
        let expected = r"
• series numbers
  size: 3
";
        test_explain(&actual, expected);
    }
}
