use {
    crate::{
        ast,
        plan::{ExprPlan, OrderByExprPlan},
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

#[cfg(test)]
mod tests {
    use {
        super::{ValuesOrderByPlan, ValuesPlan},
        crate::{
            ast::Literal,
            plan::{ExprPlan, OrderByExprPlan},
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
}
