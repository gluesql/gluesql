use {
    super::{context::Context, evaluable::check_expr as check_evaluable, planner::Planner},
    crate::{
        ast::{
            BinaryOperator, Expr, IndexItem, Query, Select, SetExpr, Statement, TableFactor,
            TableWithJoins,
        },
        data::Schema,
    },
    std::{collections::HashMap, rc::Rc},
};

pub fn plan(schema_map: &HashMap<String, Schema>, statement: Statement) -> Statement {
    let planner = PrimaryKeyPlanner { schema_map };

    match statement {
        Statement::Query(query) => {
            let query = planner.query(None, query);

            Statement::Query(query)
        }
        _ => statement,
    }
}

struct PrimaryKeyPlanner<'a> {
    schema_map: &'a HashMap<String, Schema>,
}

impl<'a> Planner<'a> for PrimaryKeyPlanner<'a> {
    fn query(&self, outer_context: Option<Rc<Context<'a>>>, query: Query) -> Query {
        let body = match query.body {
            SetExpr::Select(select) => {
                let select = self.select(outer_context, *select);

                SetExpr::Select(Box::new(select))
            }
            SetExpr::Values(_) => query.body,
        };

        Query { body, ..query }
    }

    fn get_schema(&self, name: &str) -> Option<&'a Schema> {
        self.schema_map.get(name)
    }
}

enum PrimaryKey {
    Found {
        index_item: IndexItem,
        expr: Option<Expr>,
    },
    NotFound(Expr),
}

impl<'a> PrimaryKeyPlanner<'a> {
    fn select(&self, outer_context: Option<Rc<Context<'a>>>, select: Select) -> Select {
        let current_context = self.update_context(None, &select.from.relation);
        let current_context = select
            .from
            .joins
            .iter()
            .fold(current_context, |context, join| {
                self.update_context(context, &join.relation)
            });

        let (index, selection) = select
            .selection
            .map(|expr| self.expr(outer_context, current_context, expr))
            .map(|primary_key| match primary_key {
                PrimaryKey::Found { index_item, expr } => (Some(index_item), expr),
                PrimaryKey::NotFound(expr) => (None, Some(expr)),
            })
            .unwrap_or((None, None));

        if let TableFactor::Table {
            name,
            alias,
            index: None,
        } = select.from.relation
        {
            let from = TableWithJoins {
                relation: TableFactor::Table { name, alias, index },
                ..select.from
            };

            Select {
                selection,
                from,
                ..select
            }
        } else {
            Select {
                selection,
                ..select
            }
        }
    }

    fn expr(
        &self,
        outer_context: Option<Rc<Context<'a>>>,
        current_context: Option<Rc<Context<'a>>>,
        expr: Expr,
    ) -> PrimaryKey {
        let check_primary_key = |key: &Expr| {
            let key = match key {
                Expr::Identifier(ident) => ident,
                Expr::CompoundIdentifier { ident, .. } => ident,
                _ => return false,
            };

            current_context
                .as_ref()
                .map(|context| context.contains_primary_key(key))
                .unwrap_or(false)
        };

        match expr {
            Expr::BinaryOp {
                left: key,
                op: BinaryOperator::Eq,
                right: value,
            }
            | Expr::BinaryOp {
                left: value,
                op: BinaryOperator::Eq,
                right: key,
            } if check_primary_key(key.as_ref())
                && check_evaluable(current_context.as_ref().map(Rc::clone), &key)
                && check_evaluable(None, &value) =>
            {
                let index_item = IndexItem::PrimaryKey(*value);

                PrimaryKey::Found {
                    index_item,
                    expr: None,
                }
            }
            Expr::BinaryOp {
                left,
                op: BinaryOperator::And,
                right,
            } => {
                let primary_key = self.expr(
                    outer_context.as_ref().map(Rc::clone),
                    current_context.as_ref().map(Rc::clone),
                    *left,
                );

                let left = match primary_key {
                    PrimaryKey::Found { index_item, expr } => {
                        let expr = match expr {
                            Some(left) => Expr::BinaryOp {
                                left: Box::new(left),
                                op: BinaryOperator::And,
                                right,
                            },
                            None => *right,
                        };

                        return PrimaryKey::Found {
                            index_item,
                            expr: Some(expr),
                        };
                    }
                    PrimaryKey::NotFound(expr) => expr,
                };

                match self.expr(outer_context, current_context, *right) {
                    PrimaryKey::Found { index_item, expr } => {
                        let expr = match expr {
                            Some(right) => Expr::BinaryOp {
                                left: Box::new(left),
                                op: BinaryOperator::And,
                                right: Box::new(right),
                            },
                            None => left,
                        };

                        PrimaryKey::Found {
                            index_item,
                            expr: Some(expr),
                        }
                    }
                    PrimaryKey::NotFound(expr) => {
                        let expr = Expr::BinaryOp {
                            left: Box::new(left),
                            op: BinaryOperator::And,
                            right: Box::new(expr),
                        };

                        PrimaryKey::NotFound(expr)
                    }
                }
            }
            Expr::Nested(expr) => match self.expr(outer_context, current_context, *expr) {
                PrimaryKey::Found { index_item, expr } => {
                    let expr = expr.map(Box::new).map(Expr::Nested);

                    PrimaryKey::Found { index_item, expr }
                }
                PrimaryKey::NotFound(expr) => PrimaryKey::NotFound(Expr::Nested(Box::new(expr))),
            },
            _ => {
                let outer_context = Context::concat(current_context, outer_context);
                let expr = self.subquery_expr(outer_context, expr);

                PrimaryKey::NotFound(expr)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use {
        super::plan as plan_primary_key,
        crate::{
            ast::{
                AstLiteral, BinaryOperator, Expr, IndexItem, Join, JoinConstraint, JoinExecutor,
                JoinOperator, Query, Select, SelectItem, SetExpr, Statement, TableFactor,
                TableWithJoins, Values,
            },
            mock::{run, MockStorage},
            parse_sql::{parse, parse_expr},
            plan::fetch_schema_map,
            translate::{translate, translate_expr},
        },
        futures::executor::block_on,
    };

    fn plan(storage: &MockStorage, sql: &str) -> Statement {
        let parsed = parse(sql).expect(sql).into_iter().next().unwrap();
        let statement = translate(&parsed).unwrap();
        let schema_map = block_on(fetch_schema_map(storage, &statement)).unwrap();

        plan_primary_key(&schema_map, statement)
    }

    fn select(select: Select) -> Statement {
        Statement::Query(Query {
            body: SetExpr::Select(Box::new(select)),
            limit: None,
            offset: None,
            order_by: Vec::new(),
        })
    }

    fn expr(sql: &str) -> Expr {
        let parsed = parse_expr(sql).expect(sql);

        translate_expr(&parsed).expect(sql)
    }

    #[test]
    fn where_expr() {
        let storage = run("
            CREATE TABLE Player (
                id INTEGER PRIMARY KEY,
                name TEXT
            );
        ");

        let sql = "SELECT * FROM Player WHERE id = 1;";
        let actual = plan(&storage, sql);
        let expected = select(Select {
            projection: vec![SelectItem::Wildcard],
            from: TableWithJoins {
                relation: TableFactor::Table {
                    name: "Player".to_owned(),
                    alias: None,
                    index: Some(IndexItem::PrimaryKey(expr("1"))),
                },
                joins: Vec::new(),
            },
            selection: None,
            group_by: Vec::new(),
            having: None,
        });
        assert_eq!(actual, expected, "primary key in lhs:\n{sql}");

        let sql = "SELECT * FROM Player WHERE 1 = id;";
        let actual = plan(&storage, sql);
        let expected = select(Select {
            projection: vec![SelectItem::Wildcard],
            from: TableWithJoins {
                relation: TableFactor::Table {
                    name: "Player".to_owned(),
                    alias: None,
                    index: Some(IndexItem::PrimaryKey(expr("1"))),
                },
                joins: Vec::new(),
            },
            selection: None,
            group_by: Vec::new(),
            having: None,
        });
        assert_eq!(actual, expected, "primary key in rhs:\n{sql}");

        let sql = "SELECT * FROM Player WHERE id = 1 AND True;";
        let actual = plan(&storage, sql);
        let expected = select(Select {
            projection: vec![SelectItem::Wildcard],
            from: TableWithJoins {
                relation: TableFactor::Table {
                    name: "Player".to_owned(),
                    alias: None,
                    index: Some(IndexItem::PrimaryKey(expr("1"))),
                },
                joins: Vec::new(),
            },
            selection: Some(expr("True")),
            group_by: Vec::new(),
            having: None,
        });
        assert_eq!(actual, expected, "AND binary op:\n{sql}");

        let sql = "
            SELECT * FROM Player
            WHERE
                name IS NOT NULL
                AND id = 1
                AND True;
        ";
        let actual = plan(&storage, sql);
        let expected = select(Select {
            projection: vec![SelectItem::Wildcard],
            from: TableWithJoins {
                relation: TableFactor::Table {
                    name: "Player".to_owned(),
                    alias: None,
                    index: Some(IndexItem::PrimaryKey(expr("1"))),
                },
                joins: Vec::new(),
            },
            selection: Some(expr("name IS NOT NULL AND True")),
            group_by: Vec::new(),
            having: None,
        });
        assert_eq!(actual, expected, "AND binary op 2:\n{sql}");

        let sql = "
            SELECT * FROM Player
            WHERE
                name IS NOT NULL
                AND True
                AND id = 1;
        ";
        let actual = plan(&storage, sql);
        assert_eq!(actual, expected, "AND binary op 3:\n{sql}");

        let sql = "
            SELECT * FROM Player
            WHERE
                name IS NOT NULL
                AND (True AND id = 1);
        ";
        let actual = plan(&storage, sql);
        let expected = select(Select {
            projection: vec![SelectItem::Wildcard],
            from: TableWithJoins {
                relation: TableFactor::Table {
                    name: "Player".to_owned(),
                    alias: None,
                    index: Some(IndexItem::PrimaryKey(expr("1"))),
                },
                joins: Vec::new(),
            },
            selection: Some(expr("name IS NOT NULL AND (True)")),
            group_by: Vec::new(),
            having: None,
        });
        assert_eq!(actual, expected, "AND binary op 3:\n{sql}");
    }

    #[test]
    fn join_and_nested() {
        let storage = run("
            CREATE TABLE Player (
                id INTEGER PRIMARY KEY,
                name TEXT
            );
            CREATE TABLE Badge (
                title TEXT PRIMARY KEY,
                user_id INTEGER
            );
        ");

        let sql = "SELECT * FROM Player JOIN Badge WHERE Player.id = 1";
        let actual = plan(&storage, sql);
        let expected = select(Select {
            projection: vec![SelectItem::Wildcard],
            from: TableWithJoins {
                relation: TableFactor::Table {
                    name: "Player".to_owned(),
                    alias: None,
                    index: Some(IndexItem::PrimaryKey(expr("1"))),
                },
                joins: vec![Join {
                    relation: TableFactor::Table {
                        name: "Badge".to_owned(),
                        alias: None,
                        index: None,
                    },
                    join_operator: JoinOperator::Inner(JoinConstraint::None),
                    join_executor: JoinExecutor::NestedLoop,
                }],
            },
            selection: None,
            group_by: Vec::new(),
            having: None,
        });
        assert_eq!(actual, expected, "basic inner join:\n{sql}");

        let sql = "SELECT * FROM Player JOIN Badge WHERE Player.id = Badge.user_id";
        let actual = plan(&storage, sql);
        let expected = select(Select {
            projection: vec![SelectItem::Wildcard],
            from: TableWithJoins {
                relation: TableFactor::Table {
                    name: "Player".to_owned(),
                    alias: None,
                    index: None,
                },
                joins: vec![Join {
                    relation: TableFactor::Table {
                        name: "Badge".to_owned(),
                        alias: None,
                        index: None,
                    },
                    join_operator: JoinOperator::Inner(JoinConstraint::None),
                    join_executor: JoinExecutor::NestedLoop,
                }],
            },
            selection: Some(expr("Player.id = Badge.user_id")),
            group_by: Vec::new(),
            having: None,
        });
        assert_eq!(actual, expected, "join but no primary key:\n{sql}");

        let sql = "
            SELECT * FROM Player
            WHERE name IN (
                SELECT * FROM Player WHERE id = 1
            )";
        let actual = plan(&storage, sql);
        let expected = {
            let subquery = Query {
                body: SetExpr::Select(Box::new(Select {
                    projection: vec![SelectItem::Wildcard],
                    from: TableWithJoins {
                        relation: TableFactor::Table {
                            name: "Player".to_owned(),
                            alias: None,
                            index: Some(IndexItem::PrimaryKey(expr("1"))),
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

            select(Select {
                projection: vec![SelectItem::Wildcard],
                from: TableWithJoins {
                    relation: TableFactor::Table {
                        name: "Player".to_owned(),
                        alias: None,
                        index: None,
                    },
                    joins: Vec::new(),
                },
                selection: Some(Expr::InSubquery {
                    expr: Box::new(expr("name")),
                    subquery: Box::new(subquery),
                    negated: false,
                }),
                group_by: Vec::new(),
                having: None,
            })
        };
        assert_eq!(actual, expected, "nested select:\n{sql}");
    }

    #[test]
    fn not_found() {
        let storage = run("
            CREATE TABLE Player (
                id INTEGER PRIMARY KEY,
                name TEXT
            );
        ");

        let sql = "SELECT * FROM Player WHERE name = (SELECT name FROM Player LIMIT 1);";
        let actual = plan(&storage, sql);
        let expected = {
            let subquery = Query {
                body: SetExpr::Select(Box::new(Select {
                    projection: vec![SelectItem::Expr {
                        expr: Expr::Identifier("name".to_owned()),
                        label: "name".to_owned(),
                    }],
                    from: TableWithJoins {
                        relation: TableFactor::Table {
                            name: "Player".to_owned(),
                            alias: None,
                            index: None,
                        },
                        joins: Vec::new(),
                    },
                    selection: None,
                    group_by: Vec::new(),
                    having: None,
                })),
                limit: Some(expr("1")),
                offset: None,
                order_by: Vec::new(),
            };

            select(Select {
                projection: vec![SelectItem::Wildcard],
                from: TableWithJoins {
                    relation: TableFactor::Table {
                        name: "Player".to_owned(),
                        alias: None,
                        index: None,
                    },
                    joins: Vec::new(),
                },
                selection: Some(Expr::BinaryOp {
                    left: Box::new(Expr::Identifier("name".to_owned())),
                    op: BinaryOperator::Eq,
                    right: Box::new(Expr::Subquery(Box::new(subquery))),
                }),
                group_by: Vec::new(),
                having: None,
            })
        };
        assert_eq!(actual, expected, "name is not primary key:\n{sql}");

        let sql = "
            SELECT * FROM Player WHERE id IN (
                SELECT id FROM Player WHERE id = id
            );
        ";
        let actual = plan(&storage, sql);
        let expected = {
            let subquery = Query {
                body: SetExpr::Select(Box::new(Select {
                    projection: vec![SelectItem::Expr {
                        expr: Expr::Identifier("id".to_owned()),
                        label: "id".to_owned(),
                    }],
                    from: TableWithJoins {
                        relation: TableFactor::Table {
                            name: "Player".to_owned(),
                            alias: None,
                            index: None,
                        },
                        joins: Vec::new(),
                    },
                    selection: Some(expr("id = id")),
                    group_by: Vec::new(),
                    having: None,
                })),
                limit: None,
                offset: None,
                order_by: Vec::new(),
            };

            select(Select {
                projection: vec![SelectItem::Wildcard],
                from: TableWithJoins {
                    relation: TableFactor::Table {
                        name: "Player".to_owned(),
                        alias: None,
                        index: None,
                    },
                    joins: Vec::new(),
                },
                selection: Some(Expr::InSubquery {
                    expr: Box::new(Expr::Identifier("id".to_owned())),
                    subquery: Box::new(subquery),
                    negated: false,
                }),
                group_by: Vec::new(),
                having: None,
            })
        };
        assert_eq!(actual, expected, "ambiguous nested contexts:\n{sql}");

        let sql = "DELETE FROM Player WHERE id = 1;";
        let actual = plan(&storage, sql);
        let expected = Statement::Delete {
            table_name: "Player".to_owned(),
            selection: Some(Expr::BinaryOp {
                left: Box::new(Expr::Identifier("id".to_owned())),
                op: BinaryOperator::Eq,
                right: Box::new(Expr::Literal(AstLiteral::Number(1.into()))),
            }),
        };
        assert_eq!(actual, expected, "delete statement:\n{sql}");

        let sql = "VALUES (1), (2);";
        let actual = plan(&storage, sql);
        let expected = Statement::Query(Query {
            body: SetExpr::Values(Values(vec![
                vec![Expr::Literal(AstLiteral::Number(1.into()))],
                vec![Expr::Literal(AstLiteral::Number(2.into()))],
            ])),
            limit: None,
            offset: None,
            order_by: Vec::new(),
        });
        assert_eq!(actual, expected, "values:\n{sql}");

        let sql = "SELECT * FROM Player WHERE (name);";
        let actual = plan(&storage, sql);
        let expected = select(Select {
            projection: vec![SelectItem::Wildcard],
            from: TableWithJoins {
                relation: TableFactor::Table {
                    name: "Player".to_owned(),
                    alias: None,
                    index: None,
                },
                joins: Vec::new(),
            },
            selection: Some(Expr::Nested(Box::new(expr("name")))),
            group_by: Vec::new(),
            having: None,
        });
        assert_eq!(actual, expected, "nested:\n{sql}");
    }
}
