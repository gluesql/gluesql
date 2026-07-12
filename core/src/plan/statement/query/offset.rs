use {
    super::{OrderByPlan, SetExprPlan},
    crate::plan::ExprPlan,
    serde::{Deserialize, Serialize},
};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct OffsetPlan {
    pub input: OffsetInputPlan,
    pub count: ExprPlan,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum OffsetInputPlan {
    Body(SetExprPlan),
    OrderBy(OrderByPlan),
}

#[cfg(test)]
mod tests {
    use {
        super::{OffsetInputPlan, OffsetPlan},
        crate::{
            ast::Literal,
            plan::{ExprPlan, OrderByPlan, SetExprPlan, ValuesPlan},
        },
    };

    fn body() -> SetExprPlan {
        SetExprPlan::Values(ValuesPlan(Vec::new()))
    }

    fn count(value: i64) -> ExprPlan {
        ExprPlan::Literal(Literal::Number(value.into()))
    }

    #[test]
    fn offset_accepts_body_input() {
        let plan = OffsetPlan {
            input: OffsetInputPlan::Body(body()),
            count: count(2),
        };

        assert!(matches!(
            plan,
            OffsetPlan {
                input: OffsetInputPlan::Body(_),
                count: actual,
            } if actual == count(2)
        ));
    }

    #[test]
    fn offset_accepts_order_by_input() {
        let plan = OffsetPlan {
            input: OffsetInputPlan::OrderBy(OrderByPlan {
                input: body(),
                exprs: Vec::new(),
            }),
            count: count(2),
        };

        assert!(matches!(plan.input, OffsetInputPlan::OrderBy(_)));
    }
}
