use crate::{
    ast::IndexItem,
    ast_builder::{SelectNode, TableFactorNode},
};

#[derive(Clone, Debug)]
pub struct IndexNode<'a> {
    pub table_node: TableFactorNode<'a>,
    pub index: Option<IndexItem>,
}

impl<'a> IndexNode<'a> {
    pub fn new(table_node: TableFactorNode<'a>, index: Option<IndexItem>) -> Self {
        Self { table_node, index }
    }

    pub fn alias_as(self, table_alias: &str) -> Self {
        let table_factor = TableFactorNode {
            table_name: self.table_node.table_name,
            table_type: self.table_node.table_type,
            table_alias: Some(table_alias.to_owned()),
            index: None,
        };
        Self {
            table_node: table_factor,
            index: self.index,
        }
    }

    pub fn select(self) -> SelectNode<'a> {
        let table_factor = TableFactorNode {
            table_name: self.table_node.table_name,
            table_type: self.table_node.table_type,
            table_alias: self.table_node.table_alias,
            index: self.index,
        };

        SelectNode::new(table_factor)
    }
}

#[cfg(test)]
use crate::{ast_builder::insert::Expr, parse_sql::parse_expr, translate::translate_expr};

#[cfg(test)]
fn to_expr(sql: &str) -> Expr {
    let parsed = parse_expr(sql).expect(sql);

    translate_expr(&parsed).expect(sql)
}

#[cfg(test)]
mod tests {
    use crate::{
        ast::{Select, TableAlias},
        ast_builder::{index_by::to_expr, primary_key},
    };

    use {
        super::IndexItem,
        crate::{
            ast::{Query, SelectItem, SetExpr, TableFactor, TableWithJoins},
            ast_builder::{insert::Statement, table, Build},
        },
    };

    #[test]
    fn test_index() {
        // index_by - select
        // select * from player where id = 1
        let actual = table("Player")
            .index_by(primary_key().eq("1"))
            .select()
            .build();

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

            Ok(Statement::Query(subquery))
        };
        assert_eq!(actual, expected);

        // index_by - alias_as
        let actual = table("Player")
            .index_by(primary_key().eq("1"))
            .alias_as("p")
            .select()
            .build();

        let expected = {
            let subquery = Query {
                body: SetExpr::Select(Box::new(Select {
                    projection: vec![SelectItem::Wildcard],
                    from: TableWithJoins {
                        relation: TableFactor::Table {
                            name: "Player".to_owned(),
                            alias: Some(TableAlias {
                                name: "p".to_owned(),
                                columns: Vec::new(),
                            }),
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

            Ok(Statement::Query(subquery))
        };
        assert_eq!(actual, expected);
    }
}
