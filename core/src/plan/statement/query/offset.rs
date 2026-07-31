use {
    super::{DistinctPlan, ProjectPlan, SelectOrderByPlan, ValuesOrderByPlan, ValuesPlan},
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
    Project(ProjectPlan),
    Values(ValuesPlan),
    SelectOrderBy(SelectOrderByPlan),
    ValuesOrderBy(ValuesOrderByPlan),
    Distinct(DistinctPlan),
}

#[cfg(test)]
mod tests {
    use {
        super::{OffsetInputPlan, OffsetPlan},
        crate::{
            ast::Literal,
            plan::{ExprPlan, ValuesPlan},
        },
    };

    fn count(value: i64) -> ExprPlan {
        ExprPlan::Literal(Literal::Number(value.into()))
    }

    #[test]
    fn offset_accepts_values_input() {
        let plan = OffsetPlan {
            input: OffsetInputPlan::Values(ValuesPlan(Vec::new())),
            count: count(2),
        };

        assert!(matches!(
            plan,
            OffsetPlan {
                input: OffsetInputPlan::Values(_),
                count: actual,
            } if actual == count(2)
        ));
    }
}
