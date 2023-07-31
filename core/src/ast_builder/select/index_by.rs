use crate::{
    ast::{IndexItem, Select, TableFactor},
    ast_builder::{select::Prebuild, ExprNode, FilterNode, SelectNode},
    result::Result,
};

#[derive(Clone, Debug)]
pub enum PrevNode<'a> {
    Select(SelectNode<'a>),
}

impl<'a> Prebuild<Select> for PrevNode<'a> {
    fn prebuild(self) -> Result<Select> {
        match self {
            Self::Select(node) => node.prebuild(),
        }
    }
}

impl<'a> From<SelectNode<'a>> for PrevNode<'a> {
    fn from(node: SelectNode<'a>) -> Self {
        PrevNode::Select(node)
    }
}

#[derive(Clone, Debug)]
pub struct IndexNode<'a> {
    pub prev_node: PrevNode<'a>,
    pub index: IndexItem,
}

impl<'a> IndexNode<'a> {
    pub fn new<N: Into<PrevNode<'a>>>(prev_node: N, index: IndexItem) -> Self {
        Self {
            prev_node: prev_node.into(),
            index,
        }
    }

    pub fn filter<T: Into<ExprNode<'a>>>(self, expr: T) -> FilterNode<'a> {
        FilterNode::new(self, expr)
    }

    // todo( add Next Possible Node )
}

impl<'a> Prebuild<Select> for IndexNode<'a> {
    fn prebuild(self) -> Result<Select> {
        let mut select: Select = self.prev_node.prebuild()?;
        match &mut select.from.relation {
            TableFactor::Table { index, .. } => {
                *index = Some(self.index);
            }
            _ => return Ok(select),
        }
        Ok(select)
    }
}

#[cfg(test)]
use {
    crate::{
        ast_builder::{insert::Expr, insert::Statement},
        mock::MockStorage,
        parse_sql::parse_expr,
        plan::fetch_schema_map,
        plan::plan_primary_key,
        translate::translate_expr,
    },
    futures::executor::block_on,
};

#[cfg(test)]
fn plan_query_builder(storage: &MockStorage, statement: Statement) -> Statement {
    let schema_map = block_on(fetch_schema_map(storage, &statement)).unwrap();
    plan_primary_key(&schema_map, statement)
}

#[cfg(test)]
fn to_expr(sql: &str) -> Expr {
    let parsed = parse_expr(sql).expect(sql);

    translate_expr(&parsed).expect(sql)
}

#[cfg(test)]
mod tests {
    use crate::mock::run_statement;

    use {
        super::{IndexItem, Select},
        crate::{
            ast::{Query, SelectItem, SetExpr, TableFactor, TableWithJoins},
            ast_builder::{
                expr,
                insert::Statement,
                select::index_by::{plan_query_builder, to_expr},
                select::index_item::use_idx,
                table, Build,
            },
        },
    };

    #[test]
    fn test_index() {
        let storage = run_statement(
            table("Player")
                .create_table()
                .add_column("id INTEGER PRIMARY KEY")
                .add_column("name TEXT")
                .build()
                .unwrap(),
        );

        let query_result = table("Player")
            .select()
            .index_by(use_idx().primary_key(expr("1")))
            .filter(expr("id").eq(1))
            .build();

        let actual = plan_query_builder(&storage, query_result.unwrap());

        let expected = {
            let subquery = Query {
                body: SetExpr::Select(Box::new(Select {
                    projection: vec![SelectItem::Wildcard],
                    from: TableWithJoins {
                        relation: TableFactor::Table {
                            name: "Player".to_owned(),
                            alias: None,
                            index: Some(IndexItem::PrimaryKey(to_expr("1"))),
                        },
                        joins: Vec::new(),
                    },
                    selection: None,
                    group_by: Vec::new(),
                    having: None,
                })),
                limit: None,
                offset: None,
                order_by: Vec::new(),
            };

            Statement::Query(subquery)
        };
        assert_eq!(actual, expected);
    }
}
