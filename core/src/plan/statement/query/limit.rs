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
            plan::{
                DistinctInputPlan, DistinctPlan, ExprPlan, OffsetInputPlan, OffsetPlan,
                OrderByExprPlan, ProjectInputPlan, ProjectPlan, ProjectionPlan, SelectItemPlan,
                SelectOrderByPlan, SourcePlan, TableAccessPlan, TableSourcePlan, ValuesOrderByPlan,
                ValuesPlan, explain::test_explain,
            },
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

    fn project() -> ProjectPlan {
        ProjectPlan {
            input: ProjectInputPlan::Source(SourcePlan::Table(TableSourcePlan {
                name: "Player".to_owned(),
                alias: None,
                access: TableAccessPlan::FullScan,
            })),
            projection: ProjectionPlan::SelectItems(vec![SelectItemPlan::Wildcard]),
        }
    }

    fn values() -> ValuesPlan {
        ValuesPlan(vec![vec![count(1)]])
    }

    fn select_order_by() -> SelectOrderByPlan {
        SelectOrderByPlan {
            input: project(),
            exprs: vec![OrderByExprPlan {
                expr: ExprPlan::Identifier("id".to_owned()),
                asc: Some(false),
            }],
        }
    }

    fn values_order_by() -> ValuesOrderByPlan {
        ValuesOrderByPlan {
            input: values(),
            exprs: vec![OrderByExprPlan {
                expr: count(1),
                asc: Some(false),
            }],
        }
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

    #[test]
    fn explain() {
        let actual = LimitPlan {
            input: LimitInputPlan::Project(project()),
            count: count(3),
        };
        let expected = r"
• limit
│ count: 3
│
└── • project
    │ columns: *
    │
    └── • scan Player
          access: full scan
";
        test_explain(&actual, expected);

        let actual = LimitPlan {
            input: LimitInputPlan::Values(values()),
            count: count(3),
        };
        let expected = r"
• limit
│ count: 3
│
└── • values
      size: 1 columns, 1 rows
";
        test_explain(&actual, expected);

        let actual = LimitPlan {
            input: LimitInputPlan::SelectOrderBy(select_order_by()),
            count: count(3),
        };
        let expected = r"
• limit
│ count: 3
│
└── • sort
    │ order: id DESC
    │
    └── • project
        │ columns: *
        │
        └── • scan Player
              access: full scan
";
        test_explain(&actual, expected);

        let actual = LimitPlan {
            input: LimitInputPlan::ValuesOrderBy(values_order_by()),
            count: count(3),
        };
        let expected = r"
• limit
│ count: 3
│
└── • sort
    │ order: 1 DESC
    │
    └── • values
          size: 1 columns, 1 rows
";
        test_explain(&actual, expected);

        let actual = LimitPlan {
            input: LimitInputPlan::Distinct(DistinctPlan {
                input: DistinctInputPlan::Project(project()),
            }),
            count: count(3),
        };
        let expected = r"
• limit
│ count: 3
│
└── • distinct
    └── • project
        │ columns: *
        │
        └── • scan Player
              access: full scan
";
        test_explain(&actual, expected);

        let actual = LimitPlan {
            input: LimitInputPlan::Offset(OffsetPlan {
                input: OffsetInputPlan::Values(values()),
                count: count(2),
            }),
            count: count(3),
        };
        let expected = r"
• limit
│ count: 3
│
└── • offset
    │ count: 2
    │
    └── • values
          size: 1 columns, 1 rows
";
        test_explain(&actual, expected);
    }
}
