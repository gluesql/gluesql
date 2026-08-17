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
            plan::{
                DistinctInputPlan, DistinctPlan, ExprPlan, OrderByExprPlan, ProjectInputPlan,
                ProjectPlan, ProjectionPlan, SelectItemPlan, SelectOrderByPlan, SourcePlan,
                TableAccessPlan, TableSourcePlan, ValuesOrderByPlan, ValuesPlan,
                explain::test_explain,
            },
        },
    };

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

    #[test]
    fn explain() {
        let actual = OffsetPlan {
            input: OffsetInputPlan::Project(project()),
            count: count(2),
        };
        let expected = r"
• offset
│ count: 2
│
└── • project
    │ columns: *
    │
    └── • scan Player
          access: full scan
";
        test_explain(&actual, expected);

        let actual = OffsetPlan {
            input: OffsetInputPlan::Values(values()),
            count: count(2),
        };
        let expected = r"
• offset
│ count: 2
│
└── • values
      size: 1 columns, 1 rows
";
        test_explain(&actual, expected);

        let actual = OffsetPlan {
            input: OffsetInputPlan::SelectOrderBy(select_order_by()),
            count: count(2),
        };
        let expected = r"
• offset
│ count: 2
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

        let actual = OffsetPlan {
            input: OffsetInputPlan::ValuesOrderBy(values_order_by()),
            count: count(2),
        };
        let expected = r"
• offset
│ count: 2
│
└── • sort
    │ order: 1 DESC
    │
    └── • values
          size: 1 columns, 1 rows
";
        test_explain(&actual, expected);

        let actual = OffsetPlan {
            input: OffsetInputPlan::Distinct(DistinctPlan {
                input: DistinctInputPlan::Project(project()),
            }),
            count: count(2),
        };
        let expected = r"
• offset
│ count: 2
│
└── • distinct
    └── • project
        │ columns: *
        │
        └── • scan Player
              access: full scan
";
        test_explain(&actual, expected);
    }
}
