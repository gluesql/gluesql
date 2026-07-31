use {
    super::{ExprPlan, TableFactorPlan},
    crate::ast,
    serde::{Deserialize, Serialize},
};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum JoinInputPlan {
    Relation(TableFactorPlan),
    Join(Box<JoinPlan>),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct JoinPlan {
    pub input: JoinInputPlan,
    pub relation: TableFactorPlan,
    pub join_operator: JoinOperatorPlan,
    pub join_executor: JoinExecutorPlan,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum JoinExecutorPlan {
    NestedLoop,
    Hash {
        key_expr: ExprPlan,
        value_expr: ExprPlan,
        where_clause: Option<ExprPlan>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum JoinOperatorPlan {
    Inner(JoinConstraintPlan),
    LeftOuter(JoinConstraintPlan),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum JoinConstraintPlan {
    On(ExprPlan),
    None,
}

impl From<ast::JoinOperator> for JoinOperatorPlan {
    fn from(join_operator: ast::JoinOperator) -> Self {
        match join_operator {
            ast::JoinOperator::Inner(constraint) => Self::Inner(constraint.into()),
            ast::JoinOperator::LeftOuter(constraint) => Self::LeftOuter(constraint.into()),
        }
    }
}

impl From<ast::JoinConstraint> for JoinConstraintPlan {
    fn from(join_constraint: ast::JoinConstraint) -> Self {
        match join_constraint {
            ast::JoinConstraint::On(expr) => Self::On(expr.into()),
            ast::JoinConstraint::None => Self::None,
        }
    }
}

#[cfg(test)]
mod tests {
    use {
        super::{JoinConstraintPlan, JoinExecutorPlan, JoinInputPlan, JoinOperatorPlan, JoinPlan},
        crate::plan::TableFactorPlan,
        pretty_assertions::assert_eq,
    };

    fn table(name: &str) -> TableFactorPlan {
        TableFactorPlan::Table {
            name: name.to_owned(),
            alias: None,
            index: None,
        }
    }

    #[test]
    fn join_accepts_relation_and_previous_join_inputs() {
        let first = JoinPlan {
            input: JoinInputPlan::Relation(table("A")),
            relation: table("B"),
            join_operator: JoinOperatorPlan::Inner(JoinConstraintPlan::None),
            join_executor: JoinExecutorPlan::NestedLoop,
        };
        let second = JoinPlan {
            input: JoinInputPlan::Join(Box::new(first.clone())),
            relation: table("C"),
            join_operator: JoinOperatorPlan::LeftOuter(JoinConstraintPlan::None),
            join_executor: JoinExecutorPlan::NestedLoop,
        };

        assert_eq!(first.input, JoinInputPlan::Relation(table("A")));
        assert_eq!(
            second.input,
            JoinInputPlan::Join(Box::new(JoinPlan {
                input: JoinInputPlan::Relation(table("A")),
                relation: table("B"),
                join_operator: JoinOperatorPlan::Inner(JoinConstraintPlan::None),
                join_executor: JoinExecutorPlan::NestedLoop,
            }))
        );
    }
}
