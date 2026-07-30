use {
    super::{SelectOrderByPlan, SelectPlan},
    serde::{Deserialize, Serialize},
};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct DistinctPlan {
    pub input: DistinctInputPlan,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum DistinctInputPlan {
    Select(Box<SelectPlan>),
    SelectOrderBy(SelectOrderByPlan),
}

#[cfg(test)]
mod tests {
    use {
        super::{DistinctInputPlan, DistinctPlan},
        crate::plan::{
            ProjectionPlan, SelectOrderByPlan, SelectPlan, TableFactorPlan, TableWithJoinsPlan,
        },
    };

    fn select_plan() -> Box<SelectPlan> {
        Box::new(SelectPlan {
            projection: ProjectionPlan::SelectItems(Vec::new()),
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
        })
    }

    #[test]
    fn distinct_accepts_select_and_select_order_by_inputs() {
        let distinct = DistinctPlan {
            input: DistinctInputPlan::Select(select_plan()),
        };
        assert!(matches!(distinct.input, DistinctInputPlan::Select(_)));

        let order_by = DistinctPlan {
            input: DistinctInputPlan::SelectOrderBy(SelectOrderByPlan {
                input: select_plan(),
                exprs: Vec::new(),
            }),
        };
        assert!(matches!(
            order_by.input,
            DistinctInputPlan::SelectOrderBy(_)
        ));
    }
}
