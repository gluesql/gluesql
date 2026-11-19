use {
    super::{ProjectPlan, SelectOrderByPlan},
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

    pub(super) fn project_mut(&mut self) -> &mut ProjectPlan {
        match &mut self.input {
            DistinctInputPlan::Project(project) => project,
            DistinctInputPlan::SelectOrderBy(order_by) => &mut order_by.input,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum DistinctInputPlan {
    Project(ProjectPlan),
    SelectOrderBy(SelectOrderByPlan),
}

#[cfg(test)]
mod tests {
    use {
        super::{DistinctInputPlan, DistinctPlan},
        crate::plan::{
            ProjectInputPlan, ProjectPlan, ProjectionPlan, SelectOrderByPlan, SourcePlan,
            TableAccessPlan, TableSourcePlan,
        },
    };

    fn project_plan() -> ProjectPlan {
        ProjectPlan {
            input: ProjectInputPlan::Source(SourcePlan::Table(TableSourcePlan {
                name: "Item".to_owned(),
                alias: None,
                access: TableAccessPlan::FullScan,
            })),
            projection: ProjectionPlan::SelectItems(Vec::new()),
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
