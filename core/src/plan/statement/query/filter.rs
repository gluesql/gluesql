use {
    super::SelectPlan,
    crate::plan::ExprPlan,
    serde::{Deserialize, Serialize},
};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct FilterPlan {
    pub input: Box<SelectPlan>,
    pub expr: ExprPlan,
}

#[cfg(test)]
mod tests {
    use {
        super::FilterPlan,
        crate::{
            data::Value,
            plan::{ExprPlan, SelectPlan, TableFactorPlan, TableWithJoinsPlan},
        },
        pretty_assertions::assert_eq,
    };

    #[test]
    fn filter_accepts_select_input() {
        let input = SelectPlan {
            from: TableWithJoinsPlan {
                relation: TableFactorPlan::Table {
                    name: "Item".to_owned(),
                    alias: None,
                    index: None,
                },
                joins: Vec::new(),
            },
        };
        let expr = ExprPlan::Value(Value::Bool(true));
        let filter = FilterPlan {
            input: Box::new(input.clone()),
            expr: expr.clone(),
        };

        assert_eq!(*filter.input, input);
        assert_eq!(filter.expr, expr);
    }
}
