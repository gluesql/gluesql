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
                explain::{Explain, ExplainContext, ExplainNode},
            },
        },
    };

    #[test]
    fn explains_index_access() {
        let table = TableSourcePlan {
            name: "Player".to_owned(),
            alias: Some(TableAliasPlan {
                name: "p".to_owned(),
                columns: Vec::new(),
            }),
            access: TableAccessPlan::Index {
                name: "idx_created_at".to_owned(),
                asc: Some(false),
                predicate: Some(IndexPredicatePlan {
                    operator: IndexOperator::Gt,
                    expr: ExprPlan::Literal(Literal::Number(10.into())),
                }),
            },
        };

        assert_eq!(
            table.explain(&mut ExplainContext::default()),
            ExplainNode::new("scan Player as p")
                .with_property("access", "index idx_created_at")
                .with_property("order", "descending")
                .with_property("predicate", "> 10")
        );
    }
}
