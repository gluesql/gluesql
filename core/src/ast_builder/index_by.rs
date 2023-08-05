use crate::{
    ast::IndexItem,
    ast_builder::{SelectNode, TableFactorNode},
};

#[derive(Clone, Debug)]
pub struct IndexNode<'a> {
    pub table_node: TableFactorNode<'a>,
    pub index: IndexItem,
}

impl<'a> IndexNode<'a> {
    pub fn new(table_node: TableFactorNode<'a>, index: IndexItem) -> Self {
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
            index: Some(self.index),
        };

        SelectNode::new(table_factor)
    }
}

#[cfg(test)]
mod tests {
    use {
        super::IndexItem,
        crate::{
            ast::{Query, Select, SelectItem, SetExpr, TableAlias, TableFactor, TableWithJoins},
            ast_builder::{insert::Statement, primary_key, table, to_expr, Build},
        },
    };

    #[test]
    fn test_index() {
        // index_by - select
        // select * from player where id = 1
        let actual = table("Player")
            .index_by(primary_key().eq("1").build())
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
            .index_by(primary_key().eq("1").build())
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
