use {
    super::{
        DistinctPlan, OffsetPlan, ProjectPlan, SelectOrderByPlan, ValuesOrderByPlan, ValuesPlan,
    },
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
    Project(ProjectPlan),
    Values(ValuesPlan),
    SelectOrderBy(SelectOrderByPlan),
    ValuesOrderBy(ValuesOrderByPlan),
    Distinct(DistinctPlan),
    Offset(OffsetPlan),
}

#[cfg(test)]
mod tests {
    use {
        super::{LimitInputPlan, LimitPlan},
        crate::{
            ast::Literal,
            plan::{ExprPlan, OffsetInputPlan, OffsetPlan, ValuesPlan},
        },
    };

    #[test]
    fn limit_accepts_values_input() {
        let plan = LimitPlan {
            input: LimitInputPlan::Values(ValuesPlan(Vec::new())),
            count: count(3),
        };

        assert!(matches!(plan.input, LimitInputPlan::Values(_)));
    }

    fn count(value: i64) -> ExprPlan {
        ExprPlan::Literal(Literal::Number(value.into()))
    }

    #[test]
    fn limit_accepts_offset_input() {
        let plan = LimitPlan {
            input: LimitInputPlan::Offset(OffsetPlan {
                input: OffsetInputPlan::Values(ValuesPlan(Vec::new())),
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
