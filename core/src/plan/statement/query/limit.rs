use {
    super::{OffsetPlan, QueryBodyPlan},
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
    Body(QueryBodyPlan),
    Offset(OffsetPlan),
}

#[cfg(test)]
mod tests {
    use {
        super::{LimitInputPlan, LimitPlan},
        crate::{
            ast::Literal,
            plan::{ExprPlan, OffsetPlan, QueryBodyPlan, SetExprPlan, ValuesPlan},
        },
    };

    fn body() -> QueryBodyPlan {
        QueryBodyPlan {
            body: SetExprPlan::Values(ValuesPlan(Vec::new())),
            order_by: Vec::new(),
        }
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
                input: body(),
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
