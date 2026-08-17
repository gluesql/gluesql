use {
    crate::{
        ast,
        plan::{
            ExprPlan, OrderByExprPlan,
            explain::{Explain, ExplainContext, ExplainNode},
        },
    },
    serde::{Deserialize, Serialize},
};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ValuesPlan(pub Vec<Vec<ExprPlan>>);

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ValuesOrderByPlan {
    pub input: ValuesPlan,
    pub exprs: Vec<OrderByExprPlan>,
}

impl From<ast::Values> for ValuesPlan {
    fn from(values: ast::Values) -> Self {
        Self(
            values
                .0
                .into_iter()
                .map(|exprs| exprs.into_iter().map(Into::into).collect())
                .collect(),
        )
    }
}

impl Explain for ValuesPlan {
    type Output = ExplainNode;

    fn explain(&self, context: &mut ExplainContext) -> ExplainNode {
        let columns = self.0.first().map_or(0, Vec::len);
        let subquery_count = context.subquery_count();
        let expressions = self
            .0
            .iter()
            .map(|row| format!("({})", row.as_slice().explain(context)))
            .collect::<Vec<_>>()
            .join(", ");
        let has_subqueries = context.subquery_count() > subquery_count;

        ExplainNode::new("values")
            .with_property("size", format!("{columns} columns, {} rows", self.0.len()))
            .with_optional_property("expressions", has_subqueries.then_some(expressions))
    }
}

impl Explain for ValuesOrderByPlan {
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
        super::{ValuesOrderByPlan, ValuesPlan},
        crate::{
            ast::Literal,
            plan::{ExprPlan, OrderByExprPlan, QueryPlan, explain::test_explain},
        },
    };

    #[test]
    fn order_by_accepts_values_input() {
        let plan = ValuesOrderByPlan {
            input: ValuesPlan(Vec::new()),
            exprs: vec![OrderByExprPlan {
                expr: ExprPlan::Literal(Literal::Number(1.into())),
                asc: Some(false),
            }],
        };

        assert!(
            plan.input == ValuesPlan(Vec::new())
                && plan.exprs.len() == 1
                && plan.exprs[0].asc == Some(false)
        );
    }

    #[test]
    fn explain() {
        let actual = ValuesPlan(vec![
            vec![
                ExprPlan::Literal(Literal::Number(1.into())),
                ExprPlan::Literal(Literal::QuotedString("a".to_owned())),
            ],
            vec![
                ExprPlan::Literal(Literal::Number(2.into())),
                ExprPlan::Literal(Literal::QuotedString("b".to_owned())),
            ],
        ]);
        let expected = r"
• values
  size: 2 columns, 2 rows
";
        test_explain(&actual, expected);

        let actual = ValuesPlan(vec![vec![ExprPlan::Subquery(Box::new(QueryPlan::Values(
            ValuesPlan(vec![vec![ExprPlan::Literal(Literal::Number(1.into()))]]),
        )))]]);
        let expected = r"
• root
├── • values
│     size: 1 columns, 1 rows
│     expressions: (@S1)
│
└── • subquery
    │ id: @S1
    │ exec mode: one row
    │
    └── • values
          size: 1 columns, 1 rows
";
        test_explain(&actual, expected);

        let actual = ValuesOrderByPlan {
            input: ValuesPlan(vec![vec![ExprPlan::Literal(Literal::Number(1.into()))]]),
            exprs: vec![OrderByExprPlan {
                expr: ExprPlan::Literal(Literal::Number(1.into())),
                asc: Some(false),
            }],
        };
        let expected = r"
• sort
│ order: 1 DESC
│
└── • values
      size: 1 columns, 1 rows
";
        test_explain(&actual, expected);
    }
}
