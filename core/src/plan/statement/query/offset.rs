use {
    super::{DistinctPlan, ProjectPlan, SelectOrderByPlan, ValuesOrderByPlan, ValuesPlan},
    crate::plan::{
        ExprPlan,
        explain::{Explain, ExplainContext, ExplainNode},
    },
    serde::{Deserialize, Serialize},
};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct OffsetPlan {
    pub input: OffsetInputPlan,
    pub count: ExprPlan,
}

impl OffsetPlan {
    pub(super) fn project(&self) -> Option<&ProjectPlan> {
        match &self.input {
            OffsetInputPlan::Project(project) => Some(project),
            OffsetInputPlan::Values(_) | OffsetInputPlan::ValuesOrderBy(_) => None,
            OffsetInputPlan::SelectOrderBy(order_by) => Some(&order_by.input),
            OffsetInputPlan::Distinct(distinct) => Some(distinct.project()),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum OffsetInputPlan {
    Project(ProjectPlan),
    Values(ValuesPlan),
    SelectOrderBy(SelectOrderByPlan),
    ValuesOrderBy(ValuesOrderByPlan),
    Distinct(DistinctPlan),
}

impl Explain for OffsetPlan {
    type Output = ExplainNode;

    fn explain(&self, context: &mut ExplainContext) -> ExplainNode {
        ExplainNode::new("offset")
            .with_property("count", self.count.explain(context))
            .with_child(self.input.explain(context))
    }
}

impl Explain for OffsetInputPlan {
    type Output = ExplainNode;

    fn explain(&self, context: &mut ExplainContext) -> ExplainNode {
        match self {
            Self::Project(project) => project.explain(context),
            Self::Values(values) => values.explain(context),
            Self::SelectOrderBy(order_by) => order_by.explain(context),
            Self::ValuesOrderBy(order_by) => order_by.explain(context),
            Self::Distinct(distinct) => distinct.explain(context),
        }
    }
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
