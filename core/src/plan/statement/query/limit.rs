use {
    super::{OffsetPlan, OrderByPlan, SetExprPlan},
    crate::plan::ExprPlan,
    serde::{Deserialize, Serialize},
};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct LimitPlan {
    pub input: LimitInputPlan,
    pub count: ExprPlan,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum LimitInputPlan {
    Body(SetExprPlan),
    OrderBy(OrderByPlan),
    Offset(OffsetPlan),
}

#[cfg(test)]
mod tests {
    use {
        super::{LimitInputPlan, LimitPlan},
        crate::{
            ast::Literal,
            plan::{ExprPlan, OffsetInputPlan, OffsetPlan, OrderByPlan, SetExprPlan, ValuesPlan},
        },
    };

    fn body() -> SetExprPlan {
        SetExprPlan::Values(ValuesPlan(Vec::new()))
    }

    #[test]
    fn limit_accepts_order_by_input() {
        let plan = LimitPlan {
            input: LimitInputPlan::OrderBy(OrderByPlan {
                input: body(),
                exprs: Vec::new(),
            }),
            count: count(3),
        };

        assert!(matches!(plan.input, LimitInputPlan::OrderBy(_)));
    }

    fn count(value: i64) -> ExprPlan {
        ExprPlan::Literal(Literal::Number(value.into()))
    }

    #[test]
    fn limit_accepts_body_input() {
        let plan = LimitPlan {
            input: LimitInputPlan::Body(body()),
            count: count(3),
        };

        assert!(matches!(
            plan,
            LimitPlan {
                input: LimitInputPlan::Body(_),
                count: actual,
            } if actual == count(3)
        ));
    }

    #[test]
    fn limit_accepts_offset_input() {
        let plan = LimitPlan {
            input: LimitInputPlan::Offset(OffsetPlan {
                input: OffsetInputPlan::Body(body()),
                count: count(2),
            }),
            count: count(3),
        };

        assert!(matches!(
            plan,
            LimitPlan {
                input: LimitInputPlan::Offset(_),
                count: actual,
            } if actual == count(3)
        ));
    }
}
