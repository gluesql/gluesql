use {
    super::SelectPlan,
    crate::plan::ProjectionPlan,
    serde::{Deserialize, Serialize},
};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ProjectPlan {
    pub input: Box<SelectPlan>,
    pub projection: ProjectionPlan,
}

#[cfg(test)]
mod tests {
    use {
        super::ProjectPlan,
        crate::plan::{
            ProjectionPlan, SelectItemPlan, SelectPlan, TableFactorPlan, TableWithJoinsPlan,
        },
        pretty_assertions::assert_eq,
    };

    #[test]
    fn project_accepts_select_input() {
        let input = SelectPlan {
            from: TableWithJoinsPlan {
                relation: TableFactorPlan::Table {
                    name: "Item".to_owned(),
                    alias: None,
                    index: None,
                },
                joins: Vec::new(),
            },
            selection: None,
            group_by: Vec::new(),
            having: None,
            aggregate_slots: None,
        };
        let projection = ProjectionPlan::SelectItems(vec![SelectItemPlan::Wildcard]);

        let project = ProjectPlan {
            input: Box::new(input.clone()),
            projection: projection.clone(),
        };

        assert_eq!(*project.input, input);
        assert_eq!(project.projection, projection);
    }
}
