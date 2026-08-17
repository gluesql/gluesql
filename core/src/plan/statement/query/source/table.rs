use {
    super::{TableAccessPlan, TableAliasPlan},
    crate::plan::explain::{Explain, ExplainContext, ExplainNode},
    serde::{Deserialize, Serialize},
};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TableSourcePlan {
    pub name: String,
    pub alias: Option<TableAliasPlan>,
    pub access: TableAccessPlan,
}

impl Explain for TableSourcePlan {
    type Output = ExplainNode;

    fn explain(&self, context: &mut ExplainContext) -> ExplainNode {
        let name = self.alias.as_ref().map_or_else(
            || self.name.clone(),
            |alias| format!("{} as {}", self.name, alias.name),
        );
        self.access
            .explain(ExplainNode::new(format!("scan {name}")), context)
    }
}

#[cfg(test)]
mod tests {
    use {
        super::TableSourcePlan,
        crate::{
            ast::{IndexOperator, Literal},
            plan::{
                ExprPlan, IndexPredicatePlan, TableAccessPlan, TableAliasPlan,
                explain::test_explain,
            },
        },
    };

    #[test]
    fn explain() {
        let actual = TableSourcePlan {
            name: "Player".to_owned(),
            alias: Some(TableAliasPlan {
                name: "p".to_owned(),
                columns: Vec::new(),
            }),
            access: TableAccessPlan::FullScan,
        };
        let expected = r"
• scan Player as p
  access: full scan
";
        test_explain(&actual, expected);

        let actual = TableSourcePlan {
            name: "Player".to_owned(),
            alias: None,
            access: TableAccessPlan::PrimaryKey {
                expr: ExprPlan::Literal(Literal::Number(1.into())),
            },
        };
        let expected = r"
• scan Player
  access: primary key
  key: 1
";
        test_explain(&actual, expected);

        let actual = TableSourcePlan {
            name: "Player".to_owned(),
            alias: None,
            access: TableAccessPlan::Index {
                name: "idx_name".to_owned(),
                asc: None,
                predicate: None,
            },
        };
        let expected = r"
• scan Player
  access: index idx_name
";
        test_explain(&actual, expected);

        let actual = TableSourcePlan {
            name: "Player".to_owned(),
            alias: None,
            access: TableAccessPlan::Index {
                name: "idx_score".to_owned(),
                asc: Some(true),
                predicate: Some(IndexPredicatePlan {
                    operator: IndexOperator::Gt,
                    expr: ExprPlan::Literal(Literal::Number(10.into())),
                }),
            },
        };
        let expected = r"
• scan Player
  access: index idx_score
  order: ascending
  predicate: > 10
";
        test_explain(&actual, expected);

        let actual = TableSourcePlan {
            name: "Player".to_owned(),
            alias: None,
            access: TableAccessPlan::Index {
                name: "idx_score".to_owned(),
                asc: Some(false),
                predicate: Some(IndexPredicatePlan {
                    operator: IndexOperator::Lt,
                    expr: ExprPlan::Literal(Literal::Number(10.into())),
                }),
            },
        };
        let expected = r"
• scan Player
  access: index idx_score
  order: descending
  predicate: < 10
";
        test_explain(&actual, expected);

        let actual = TableSourcePlan {
            name: "Player".to_owned(),
            alias: None,
            access: TableAccessPlan::Index {
                name: "idx_score".to_owned(),
                asc: None,
                predicate: Some(IndexPredicatePlan {
                    operator: IndexOperator::GtEq,
                    expr: ExprPlan::Literal(Literal::Number(10.into())),
                }),
            },
        };
        let expected = r"
• scan Player
  access: index idx_score
  predicate: >= 10
";
        test_explain(&actual, expected);

        let actual = TableSourcePlan {
            name: "Player".to_owned(),
            alias: None,
            access: TableAccessPlan::Index {
                name: "idx_score".to_owned(),
                asc: None,
                predicate: Some(IndexPredicatePlan {
                    operator: IndexOperator::LtEq,
                    expr: ExprPlan::Literal(Literal::Number(10.into())),
                }),
            },
        };
        let expected = r"
• scan Player
  access: index idx_score
  predicate: <= 10
";
        test_explain(&actual, expected);

        let actual = TableSourcePlan {
            name: "Player".to_owned(),
            alias: None,
            access: TableAccessPlan::Index {
                name: "idx_score".to_owned(),
                asc: None,
                predicate: Some(IndexPredicatePlan {
                    operator: IndexOperator::Eq,
                    expr: ExprPlan::Literal(Literal::Number(10.into())),
                }),
            },
        };
        let expected = r"
• scan Player
  access: index idx_score
  predicate: = 10
";
        test_explain(&actual, expected);
    }
}
