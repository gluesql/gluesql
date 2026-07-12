use {
    super::QueryBodyPlan,
    crate::plan::ExprPlan,
    serde::{Deserialize, Serialize},
};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct OffsetPlan {
    pub input: QueryBodyPlan,
    pub count: ExprPlan,
}

#[cfg(test)]
mod tests {
    use {
        super::OffsetPlan,
        crate::{
            ast::Literal,
            plan::{ExprPlan, QueryBodyPlan, SetExprPlan, ValuesPlan},
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
    fn offset_accepts_body_input() {
        let OffsetPlan { count: actual, .. } = OffsetPlan {
            input: body(),
            count: count(2),
        };

        assert_eq!(actual, count(2));
    }
}
