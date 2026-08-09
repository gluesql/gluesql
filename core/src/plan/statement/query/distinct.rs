use {
    super::{ProjectPlan, SelectOrderByPlan},
    crate::plan::explain::{Explain, ExplainContext, ExplainNode},
    serde::{Deserialize, Serialize},
};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct DistinctPlan {
    pub input: DistinctInputPlan,
}

impl DistinctPlan {
    pub(super) fn project(&self) -> &ProjectPlan {
        match &self.input {
            DistinctInputPlan::Project(project) => project,
            DistinctInputPlan::SelectOrderBy(order_by) => &order_by.input,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum DistinctInputPlan {
    Project(ProjectPlan),
    SelectOrderBy(SelectOrderByPlan),
}

impl Explain for DistinctPlan {
    type Output = ExplainNode;

    fn explain(&self, context: &mut ExplainContext) -> ExplainNode {
        ExplainNode::new("distinct").with_child(self.input.explain(context))
    }
}

impl Explain for DistinctInputPlan {
    type Output = ExplainNode;

    fn explain(&self, context: &mut ExplainContext) -> ExplainNode {
        match self {
            Self::Project(project) => project.explain(context),
            Self::SelectOrderBy(order_by) => order_by.explain(context),
        }
    }
}

#[cfg(test)]
mod tests {
    use {
        super::{DistinctInputPlan, DistinctPlan},
        crate::plan::{
            ProjectInputPlan, ProjectPlan, ProjectionPlan, SelectItemPlan, SelectOrderByPlan,
            SourcePlan, TableAccessPlan, TableSourcePlan,
        },
    };

    fn project_plan() -> ProjectPlan {
        ProjectPlan {
            input: ProjectInputPlan::Source(SourcePlan::Table(TableSourcePlan {
                name: "Item".to_owned(),
                alias: None,
                access: TableAccessPlan::FullScan,
            })),
            projection: ProjectionPlan::SelectItems(vec![SelectItemPlan::Wildcard]),
        }
    }

    #[test]
    fn distinct_accepts_project_and_select_order_by_inputs() {
        let distinct = DistinctPlan {
            input: DistinctInputPlan::Project(project_plan()),
        };
        assert!(matches!(distinct.input, DistinctInputPlan::Project(_)));

        let order_by = DistinctPlan {
            input: DistinctInputPlan::SelectOrderBy(SelectOrderByPlan {
                input: project_plan(),
                exprs: Vec::new(),
            }),
        };
        assert!(matches!(
            order_by.input,
            DistinctInputPlan::SelectOrderBy(_)
        ));
    }
}
