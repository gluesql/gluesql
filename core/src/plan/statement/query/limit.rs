use {
    super::{
        DistinctPlan, OffsetPlan, ProjectPlan, SelectOrderByPlan, ValuesOrderByPlan, ValuesPlan,
    },
    crate::plan::{
        ExprPlan,
        explain::{Explain, ExplainContext, ExplainNode},
    },
    serde::{Deserialize, Serialize},
};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct LimitPlan {
    pub input: LimitInputPlan,
    pub count: ExprPlan,
}

impl LimitPlan {
    pub(super) fn project(&self) -> Option<&ProjectPlan> {
        match &self.input {
            LimitInputPlan::Project(project) => Some(project),
            LimitInputPlan::Values(_) | LimitInputPlan::ValuesOrderBy(_) => None,
            LimitInputPlan::SelectOrderBy(order_by) => Some(&order_by.input),
            LimitInputPlan::Distinct(distinct) => Some(distinct.project()),
            LimitInputPlan::Offset(offset) => offset.project(),
        }
    }
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

impl Explain for LimitPlan {
    type Output = ExplainNode;

    fn explain(&self, context: &mut ExplainContext) -> ExplainNode {
        ExplainNode::new("limit")
            .with_property("count", self.count.explain(context))
            .with_child(self.input.explain(context))
    }
}

impl Explain for LimitInputPlan {
    type Output = ExplainNode;

    fn explain(&self, context: &mut ExplainContext) -> ExplainNode {
        match self {
            Self::Project(project) => project.explain(context),
            Self::Values(values) => values.explain(context),
            Self::SelectOrderBy(order_by) => order_by.explain(context),
            Self::ValuesOrderBy(order_by) => order_by.explain(context),
            Self::Distinct(distinct) => distinct.explain(context),
            Self::Offset(offset) => offset.explain(context),
        }
    }
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
