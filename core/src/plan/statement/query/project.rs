use {
    super::{AggregationPlan, HavingPlan, SelectPlan},
    crate::plan::ProjectionPlan,
    serde::{Deserialize, Serialize},
};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ProjectInputPlan {
    Select(Box<SelectPlan>),
    Aggregation(AggregationPlan),
    Having(HavingPlan),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ProjectPlan {
    pub input: ProjectInputPlan,
    pub projection: ProjectionPlan,
}

#[cfg(test)]
mod tests {
    use {
        super::{ProjectInputPlan, ProjectPlan},
        crate::{
            data::Value,
            plan::{
                AggregationPlan, ExprPlan, HavingPlan, ProjectionPlan, SelectItemPlan, SelectPlan,
                TableFactorPlan, TableWithJoinsPlan,
            },
        },
        pretty_assertions::assert_eq,
    };

    fn select_plan() -> SelectPlan {
        SelectPlan {
            from: TableWithJoinsPlan {
                relation: TableFactorPlan::Table {
                    name: "Item".to_owned(),
                    alias: None,
                    index: None,
                },
                joins: Vec::new(),
            },
            selection: None,
        }
    }

    fn aggregation_plan() -> AggregationPlan {
        AggregationPlan {
            input: Box::new(select_plan()),
            group_by: vec![ExprPlan::Identifier("category".to_owned())],
            aggregate_slots: Vec::new(),
        }
    }

    #[test]
    fn project_accepts_select_aggregation_and_having_inputs() {
        let projection = ProjectionPlan::SelectItems(vec![SelectItemPlan::Wildcard]);
        let inputs = [
            ProjectInputPlan::Select(Box::new(select_plan())),
            ProjectInputPlan::Aggregation(aggregation_plan()),
            ProjectInputPlan::Having(HavingPlan {
                input: aggregation_plan(),
                expr: ExprPlan::Value(Value::Bool(true)),
            }),
        ];

        for input in inputs {
            let project = ProjectPlan {
                input: input.clone(),
                projection: projection.clone(),
            };

            assert_eq!(project.input, input);
            assert_eq!(project.projection, projection);
        }
    }
}
