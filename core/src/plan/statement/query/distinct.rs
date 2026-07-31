use {
    super::{ProjectPlan, SelectOrderByPlan},
    serde::{Deserialize, Serialize},
};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct DistinctPlan {
    pub input: DistinctInputPlan,
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
            ProjectInputPlan, ProjectPlan, ProjectionPlan, SelectOrderByPlan, SelectPlan,
            TableFactorPlan, TableWithJoinsPlan,
        },
    };

    fn project_plan() -> ProjectPlan {
        ProjectPlan {
            input: ProjectInputPlan::Select(Box::new(SelectPlan {
                from: TableWithJoinsPlan {
                    relation: TableFactorPlan::Table {
                        name: "Item".to_owned(),
                        alias: None,
                        index: None,
                    },
                    joins: Vec::new(),
                },
                selection: None,
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
