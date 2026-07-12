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
        let LimitPlan {
            input: LimitInputPlan::Body(_),
            count: actual,
        } = (LimitPlan {
            input: LimitInputPlan::Body(body()),
            count: count(3),
        })
        else {
            panic!("expected body input");
        };

        assert_eq!(actual, count(3));
    }

    #[test]
    fn limit_accepts_offset_input() {
        let LimitPlan {
            input: LimitInputPlan::Offset(_),
            count: actual,
        } = (LimitPlan {
            input: LimitInputPlan::Offset(OffsetPlan {
                input: body(),
                count: count(2),
            }),
            count: count(3),
        })
        else {
            panic!("expected offset input");
        };

        assert_eq!(actual, count(3));
    }
}
