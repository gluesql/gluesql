use {
    super::{context::Context, evaluable::check_expr as check_evaluable, planner::Planner},
    crate::{
        ast::{
            BinaryOperator, Expr, Join, JoinConstraint, JoinExecutor, JoinOperator, Query, Select,
            SetExpr, Statement, TableWithJoins,
        },
        data::Schema,
    },
    std::{collections::HashMap, rc::Rc},
    utils::Vector,
};

pub fn plan(schema_map: &HashMap<String, Schema>, statement: Statement) -> Statement {
    let planner = JoinPlanner { schema_map };

    match statement {
        Statement::Query(query) => {
            let query = planner.query(None, query);

            Statement::Query(query)
        }
        _ => statement,
    }
}

struct JoinPlanner<'a> {
    schema_map: &'a HashMap<String, Schema>,
}

impl<'a> Planner<'a> for JoinPlanner<'a> {
    fn query(&self, outer_context: Option<Rc<Context<'a>>>, query: Query) -> Query {
        let Query {
            body,
            order_by,
            limit,
            offset,
        } = query;

        let body = match body {
            SetExpr::Select(select) => {
                let select = self.select(outer_context, *select);

                SetExpr::Select(Box::new(select))
            }
            SetExpr::Values(_) => body,
        };

        Query {
            body,
            order_by,
            limit,
            offset,
        }
    }

    fn get_schema(&self, name: &str) -> Option<&'a Schema> {
        self.schema_map.get(name)
    }
}

impl<'a> JoinPlanner<'a> {
    fn select(&self, outer_context: Option<Rc<Context<'a>>>, select: Select) -> Select {
        let Select {
            projection,
            from,
            selection,
            group_by,
            having,
        } = select;

        let (outer_context, from) = self.table_with_joins(outer_context, from);
        let selection = selection.map(|expr| self.subquery_expr(outer_context, expr));

        Select {
            projection,
            from,
            selection,
            group_by,
            having,
        }
    }

    fn table_with_joins(
        &self,
        outer_context: Option<Rc<Context<'a>>>,
        table_with_joins: TableWithJoins,
    ) -> (Option<Rc<Context<'a>>>, TableWithJoins) {
        let TableWithJoins { relation, joins } = table_with_joins;
        let init_context = self.update_context(None, &relation);
        let (context, joins) =
            joins
                .into_iter()
                .fold((init_context, Vector::new()), |(context, joins), join| {
                    let outer_context = outer_context.as_ref().map(Rc::clone);
                    let (context, join) = self.join(outer_context, context, join);
                    let joins = joins.push(join);

                    (context, joins)
                });
        let joins = joins.into();
        let context = Some(Rc::new(Context::concat(context, outer_context)));

        (context, TableWithJoins { relation, joins })
    }

    fn join(
        &self,
        outer_context: Option<Rc<Context<'a>>>,
        inner_context: Option<Rc<Context<'a>>>,
        join: Join,
    ) -> (Option<Rc<Context<'a>>>, Join) {
        let Join {
            relation,
            join_operator,
            join_executor,
        } = join;

        if matches!(join_executor, JoinExecutor::Hash { .. }) {
            let context = self.update_context(inner_context, &relation);
            let join = Join {
                relation,
                join_operator,
                join_executor,
            };

            return (context, join);
        }

        enum JoinOp {
            Inner,
            LeftOuter,
        }

        let (join_op, expr) = match join_operator {
            JoinOperator::Inner(JoinConstraint::On(expr)) => (JoinOp::Inner, expr),
            JoinOperator::LeftOuter(JoinConstraint::On(expr)) => (JoinOp::LeftOuter, expr),
            JoinOperator::Inner(JoinConstraint::None)
            | JoinOperator::LeftOuter(JoinConstraint::None) => {
                let context = self.update_context(inner_context, &relation);
                let join = Join {
                    relation,
                    join_operator,
                    join_executor,
                };

                return (context, join);
            }
        };

        let current_context = self.update_context(None, &relation);
        let (join_executor, expr) = self.join_expr(
            outer_context,
            inner_context.as_ref().map(Rc::clone),
            current_context,
            expr,
        );

        let join_operator = match (join_op, expr) {
            (JoinOp::Inner, Some(expr)) => JoinOperator::Inner(JoinConstraint::On(expr)),
            (JoinOp::Inner, None) => JoinOperator::Inner(JoinConstraint::None),
            (JoinOp::LeftOuter, Some(expr)) => JoinOperator::LeftOuter(JoinConstraint::On(expr)),
            (JoinOp::LeftOuter, None) => JoinOperator::LeftOuter(JoinConstraint::None),
        };

        let context = self.update_context(inner_context, &relation);
        let join = Join {
            relation,
            join_operator,
            join_executor,
        };

        (context, join)
    }

    fn join_expr(
        &self,
        outer_context: Option<Rc<Context<'a>>>,
        inner_context: Option<Rc<Context<'a>>>,
        current_context: Option<Rc<Context<'a>>>,
        expr: Expr,
    ) -> (JoinExecutor, Option<Expr>) {
        match expr {
            Expr::BinaryOp {
                left,
                op: BinaryOperator::Eq,
                right,
            } => {
                let key_context = {
                    let current = current_context.as_ref().map(Rc::clone);
                    let outer = outer_context.as_ref().map(Rc::clone);

                    Some(Rc::new(Context::concat(current, outer)))
                };
                let value_context = {
                    let context = Context::concat(current_context, inner_context);
                    let context = Context::concat(Some(Rc::new(context)), outer_context);

                    Some(Rc::new(context))
                };

                let left_as_key = check_evaluable(key_context.as_ref().map(Rc::clone), &left);
                let right_as_value = check_evaluable(value_context.as_ref().map(Rc::clone), &right);

                if left_as_key && right_as_value {
                    let join_executor = JoinExecutor::Hash {
                        key_expr: *left,
                        value_expr: *right,
                        where_clause: None,
                    };

                    return (join_executor, None);
                }

                let right_as_key = check_evaluable(key_context, &right);
                let left_as_value = left_as_key || check_evaluable(value_context, &left);

                if right_as_key && left_as_value {
                    let join_executor = JoinExecutor::Hash {
                        key_expr: *right,
                        value_expr: *left,
                        where_clause: None,
                    };

                    return (join_executor, None);
                }

                let expr = Expr::BinaryOp {
                    left,
                    op: BinaryOperator::Eq,
                    right,
                };

                (JoinExecutor::NestedLoop, Some(expr))
            }
            Expr::BinaryOp {
                left,
                op: BinaryOperator::And,
                right,
            } => {
                let (join_executor, left) = self.join_expr(
                    outer_context.as_ref().map(Rc::clone),
                    inner_context.as_ref().map(Rc::clone),
                    current_context.as_ref().map(Rc::clone),
                    *left,
                );

                if let JoinExecutor::Hash {
                    key_expr,
                    value_expr,
                    where_clause,
                } = join_executor
                {
                    let context = {
                        let current = current_context.as_ref().map(Rc::clone);
                        let outer = outer_context.as_ref().map(Rc::clone);

                        Some(Rc::new(Context::concat(current, outer)))
                    };

                    let expr = match left {
                        Some(left) => Expr::BinaryOp {
                            left: Box::new(left),
                            op: BinaryOperator::And,
                            right,
                        },
                        None => *right,
                    };

                    let (evaluable_expr, expr) = find_evaluable(context, expr);

                    let where_clause = match (where_clause, evaluable_expr) {
                        (Some(expr), Some(expr2)) => Some(Expr::BinaryOp {
                            left: Box::new(expr),
                            op: BinaryOperator::And,
                            right: Box::new(expr2),
                        }),
                        (Some(expr), None) | (None, Some(expr)) => Some(expr),
                        (None, None) => None,
                    };

                    let join_executor = JoinExecutor::Hash {
                        key_expr,
                        value_expr,
                        where_clause,
                    };

                    return (join_executor, expr);
                }

                let (join_executor, right) = self.join_expr(
                    outer_context.as_ref().map(Rc::clone),
                    inner_context,
                    current_context.as_ref().map(Rc::clone),
                    *right,
                );

                let expr = match (left, right) {
                    (Some(left), Some(right)) => Some(Expr::BinaryOp {
                        left: Box::new(left),
                        op: BinaryOperator::And,
                        right: Box::new(right),
                    }),
                    (expr @ Some(_), None) | (None, expr @ Some(_)) => expr,
                    // (None,None) -> unreachable
                    // To resolve this,
                    // join_expr should return an enum of
                    //   Consumed(Option<Expr>) or NotConsumed(Expr)
                    (None, None) => None,
                };

                match join_executor {
                    JoinExecutor::NestedLoop => (join_executor, expr),
                    JoinExecutor::Hash {
                        key_expr,
                        value_expr,
                        where_clause,
                    } => {
                        let context = Rc::new(Context::concat(current_context, outer_context));
                        let (evaluable_expr, expr) = expr
                            .map(|expr| find_evaluable(Some(context), expr))
                            .unwrap_or((None, None));

                        let where_clause = match (where_clause, evaluable_expr) {
                            (Some(expr), Some(expr2)) => Some(Expr::BinaryOp {
                                left: Box::new(expr),
                                op: BinaryOperator::And,
                                right: Box::new(expr2),
                            }),
                            (Some(expr), None) | (None, Some(expr)) => Some(expr),
                            (None, None) => None,
                        };

                        let join_executor = JoinExecutor::Hash {
                            key_expr,
                            value_expr,
                            where_clause,
                        };

                        (join_executor, expr)
                    }
                }
            }
            Expr::Nested(expr) => {
                self.join_expr(outer_context, inner_context, current_context, *expr)
            }
            Expr::Subquery(query) => {
                let context = Context::concat(current_context, inner_context);
                let context = Context::concat(Some(Rc::new(context)), outer_context);
                let context = Some(Rc::new(context));

                let query = self.query(context, *query);
                let expr = Some(Expr::Subquery(Box::new(query)));

                (JoinExecutor::NestedLoop, expr)
            }
            _ => (JoinExecutor::NestedLoop, Some(expr)),
        }
    }
}

type EvaluableExpr = Option<Expr>;
type RemainderExpr = Option<Expr>;

fn find_evaluable(context: Option<Rc<Context<'_>>>, expr: Expr) -> (EvaluableExpr, RemainderExpr) {
    match expr {
        Expr::BinaryOp {
            left,
            op: BinaryOperator::And,
            right,
        } => {
            let (evaluable, remainder) = find_evaluable(context.as_ref().map(Rc::clone), *left);
            let (evaluable2, remainder2) = find_evaluable(context, *right);

            let merge = |expr, expr2| match (expr, expr2) {
                (Some(expr), Some(expr2)) => Some(Expr::BinaryOp {
                    left: Box::new(expr),
                    op: BinaryOperator::And,
                    right: Box::new(expr2),
                }),
                (Some(expr), None) | (None, Some(expr)) => Some(expr),
                (None, None) => None,
            };

            let evaluable_expr = merge(evaluable, evaluable2);
            let remainder_expr = merge(remainder, remainder2);

            (evaluable_expr, remainder_expr)
        }
        _ if check_evaluable(context, &expr) => (Some(expr), None),
        _ => (None, Some(expr)),
    }
}

#[cfg(test)]
mod tests {
    use {
        super::plan,
        crate::{
            ast::{
                BinaryOperator, DataType, DateTimeField, Expr, Join, JoinConstraint, JoinExecutor,
                JoinOperator, Query, Select, SelectItem, SetExpr, Statement, TableAlias,
                TableFactor, TableWithJoins, UnaryOperator,
            },
            parse_sql::{parse, parse_expr},
            plan::{
                fetch_schema_map,
                mock::{run, MockStorage},
            },
            translate::{translate, translate_expr},
        },
        futures::executor::block_on,
    };

    fn plan_join(storage: &MockStorage, sql: &str) -> Statement {
        let parsed = parse(sql).expect(sql).into_iter().next().unwrap();
        let statement = translate(&parsed).unwrap();
        let schema_map = block_on(fetch_schema_map(storage, &statement)).unwrap();

        plan(&schema_map, statement)
    }

    fn expr(sql: &str) -> Expr {
        let parsed = parse_expr(sql).expect(sql);

        translate_expr(&parsed).expect(sql)
    }

    fn inner(constraint: Option<&str>) -> JoinOperator {
        let constraint = match constraint {
            Some(constraint) => JoinConstraint::On(expr(constraint)),
            None => JoinConstraint::None,
        };

        JoinOperator::Inner(constraint)
    }

    fn left_outer(constraint: Option<&str>) -> JoinOperator {
        let constraint = match constraint {
            Some(constraint) => JoinConstraint::On(expr(constraint)),
            None => JoinConstraint::None,
        };

        JoinOperator::LeftOuter(constraint)
    }

    fn table_factor(name: &str, alias: Option<&str>) -> TableFactor {
        TableFactor::Table {
            name: name.to_owned(),
            alias: alias.map(|alias| TableAlias {
                name: alias.to_owned(),
                columns: Vec::new(),
            }),
            index: None,
        }
    }

    fn select(select: Select) -> Statement {
        Statement::Query(Query {
            body: SetExpr::Select(Box::new(select)),
            order_by: vec![],
            limit: None,
            offset: None,
        })
    }

    #[test]
    fn basic() {
        let storage = run("
            CREATE TABLE Player (
                id INTEGER,
                name TEXT
            );
            CREATE TABLE PlayerItem (
                user_id INTEGER,
                item_id INTEGER,
                amount INTEGER
            );
        ");

        let sql = "SELECT * FROM Player;";
        let actual = plan_join(&storage, sql);
        let expected = select(Select {
            projection: vec![SelectItem::Wildcard],
            from: TableWithJoins {
                relation: table_factor("Player", None),
                joins: Vec::new(),
            },
            selection: None,
            group_by: Vec::new(),
            having: None,
        });
        assert_eq!(actual, expected, "basic select:\n{sql}");

        let sql = "DELETE FROM Player WHERE id = 1;";
        let actual = plan_join(&storage, sql);
        let expected = Statement::Delete {
            table_name: "Player".to_owned(),
            selection: Some(expr("id = 1")),
        };
        assert_eq!(actual, expected, "plan not covered:\n{sql}");

        let sql = "
            SELECT *
            FROM Player
            JOIN PlayerItem ON PlayerItem.user_id != Player.id
        ";
        let actual = plan_join(&storage, sql);
        let expected = select(Select {
            projection: vec![SelectItem::Wildcard],
            from: TableWithJoins {
                relation: table_factor("Player", None),
                joins: vec![Join {
                    relation: table_factor("PlayerItem", None),
                    join_operator: inner(Some("PlayerItem.user_id != Player.id")),
                    join_executor: JoinExecutor::NestedLoop,
                }],
            },
            selection: None,
            group_by: Vec::new(),
            having: None,
        });
        assert_eq!(actual, expected, "basic nested loop join:\n{sql}");

        let sql = "
            SELECT *
            FROM Player
            LEFT JOIN PlayerItem ON PlayerItem.amount > 2
        ";
        let actual = plan_join(&storage, sql);
        let expected = select(Select {
            projection: vec![SelectItem::Wildcard],
            from: TableWithJoins {
                relation: table_factor("Player", None),
                joins: vec![Join {
                    relation: table_factor("PlayerItem", None),
                    join_operator: left_outer(Some("PlayerItem.amount > 2")),
                    join_executor: JoinExecutor::NestedLoop,
                }],
            },
            selection: None,
            group_by: Vec::new(),
            having: None,
        });
        assert_eq!(actual, expected, "basic nested loop join 2:\n{sql}");

        let sql = "
            SELECT *
            FROM Player
            JOIN Empty u2
            LEFT JOIN Player u3;
        ";
        let actual = plan_join(&storage, sql);
        let expected = select(Select {
            projection: vec![SelectItem::Wildcard],
            from: TableWithJoins {
                relation: table_factor("Player", None),
                joins: vec![
                    Join {
                        relation: table_factor("Empty", Some("u2")),
                        join_operator: inner(None),
                        join_executor: JoinExecutor::NestedLoop,
                    },
                    Join {
                        relation: table_factor("Player", Some("u3")),
                        join_operator: left_outer(None),
                        join_executor: JoinExecutor::NestedLoop,
                    },
                ],
            },
            selection: None,
            group_by: Vec::new(),
            having: None,
        });
        assert_eq!(actual, expected, "self multiple joins:\n{sql}");

        let sql = "
            SELECT *
            FROM Player
            JOIN PlayerItem ON PlayerItem.user_id = Player.id
        ";
        let actual = plan_join(&storage, sql);
        let expected = {
            let join_executor = JoinExecutor::Hash {
                key_expr: expr("PlayerItem.user_id"),
                value_expr: expr("Player.id"),
                where_clause: None,
            };

            select(Select {
                projection: vec![SelectItem::Wildcard],
                from: TableWithJoins {
                    relation: table_factor("Player", None),
                    joins: vec![Join {
                        relation: table_factor("PlayerItem", None),
                        join_operator: inner(None),
                        join_executor,
                    }],
                },
                selection: None,
                group_by: Vec::new(),
                having: None,
            })
        };
        assert_eq!(actual, expected, "basic hash join query:\n{sql}");

        let sql = "
            SELECT *
            FROM Player
            JOIN PlayerItem ON PlayerItem.user_id = Player.id
        ";
        let actual = plan_join(&storage, sql);
        let actual = {
            let schema_map = block_on(fetch_schema_map(&storage, &actual)).unwrap();

            plan(&schema_map, actual)
        };
        let expected = {
            let join_executor = JoinExecutor::Hash {
                key_expr: expr("PlayerItem.user_id"),
                value_expr: expr("Player.id"),
                where_clause: None,
            };

            select(Select {
                projection: vec![SelectItem::Wildcard],
                from: TableWithJoins {
                    relation: table_factor("Player", None),
                    joins: vec![Join {
                        relation: table_factor("PlayerItem", None),
                        join_operator: inner(None),
                        join_executor,
                    }],
                },
                selection: None,
                group_by: Vec::new(),
                having: None,
            })
        };
        assert_eq!(
            actual, expected,
            "redundant plan does not change the plan result:\n{sql}"
        );

        let sql = "
            SELECT * FROM Player
            JOIN PlayerItem ON (SELECT * FROM Player u2)
        ";
        let actual = plan_join(&storage, sql);
        let expected = {
            select(Select {
                projection: vec![SelectItem::Wildcard],
                from: TableWithJoins {
                    relation: table_factor("Player", None),
                    joins: vec![Join {
                        relation: table_factor("PlayerItem", None),
                        join_operator: inner(Some("(SELECT * FROM Player u2)")),
                        join_executor: JoinExecutor::NestedLoop,
                    }],
                },
                selection: None,
                group_by: Vec::new(),
                having: None,
            })
        };
        assert_eq!(actual, expected, "subquery in join_constraint:\n{sql}");
    }

    #[test]
    fn hash_join() {
        let storage = run("
            CREATE TABLE Player (
                id INTEGER,
                name TEXT
            );
            CREATE TABLE Item (
                id INTEGER,
                name TEXT
            );
            CREATE TABLE PlayerItem (
                user_id INTEGER,
                item_id INTEGER,
                amount INTEGER
            );
        ");

        let sql = "
            SELECT *
            FROM Player
            LEFT JOIN PlayerItem ON
                PlayerItem.amount > 10 AND
                PlayerItem.user_id = Player.id
            WHERE True;
        ";
        let actual = plan_join(&storage, sql);
        let expected = {
            let join_executor = JoinExecutor::Hash {
                key_expr: expr("PlayerItem.user_id"),
                value_expr: expr("Player.id"),
                where_clause: Some(expr("PlayerItem.amount > 10")),
            };

            select(Select {
                projection: vec![SelectItem::Wildcard],
                from: TableWithJoins {
                    relation: table_factor("Player", None),
                    joins: vec![Join {
                        relation: table_factor("PlayerItem", None),
                        join_operator: left_outer(None),
                        join_executor,
                    }],
                },
                selection: Some(expr("True")),
                group_by: Vec::new(),
                having: None,
            })
        };
        assert_eq!(actual, expected, "where_clause AND hash_join expr:\n{sql}");

        let sql = "
            SELECT *
            FROM Player
            JOIN PlayerItem ON
                (PlayerItem.user_id = Player.id) AND
                Player.name = 'abcd' AND
                Player.name != 'barcode'
        ";
        let actual = plan_join(&storage, sql);
        let expected = {
            let join_executor = JoinExecutor::Hash {
                key_expr: expr("PlayerItem.user_id"),
                value_expr: expr("Player.id"),
                where_clause: None,
            };

            select(Select {
                projection: vec![SelectItem::Wildcard],
                from: TableWithJoins {
                    relation: table_factor("Player", None),
                    joins: vec![Join {
                        relation: table_factor("PlayerItem", None),
                        join_operator: inner(Some(
                            "Player.name = 'abcd' AND Player.name != 'barcode'",
                        )),
                        join_executor,
                    }],
                },
                selection: None,
                group_by: Vec::new(),
                having: None,
            })
        };
        assert_eq!(
            actual, expected,
            "nested expr & remaining join constraint:\n{sql}"
        );

        let sql = "
            SELECT *
            FROM Player
            LEFT JOIN PlayerItem ON
                PlayerItem.amount > 10 AND
                PlayerItem.amount * 3 <= 2 AND
                PlayerItem.user_id = Player.id
            WHERE True;
        ";
        let actual = plan_join(&storage, sql);
        let expected = {
            let join_executor = JoinExecutor::Hash {
                key_expr: expr("PlayerItem.user_id"),
                value_expr: expr("Player.id"),
                where_clause: Some(expr(
                    "PlayerItem.amount > 10 AND PlayerItem.amount * 3 <= 2",
                )),
            };

            select(Select {
                projection: vec![SelectItem::Wildcard],
                from: TableWithJoins {
                    relation: table_factor("Player", None),
                    joins: vec![Join {
                        relation: table_factor("PlayerItem", None),
                        join_operator: left_outer(None),
                        join_executor,
                    }],
                },
                selection: Some(expr("True")),
                group_by: Vec::new(),
                having: None,
            })
        };
        assert_eq!(actual, expected, "complex where_clause:\n{sql}");

        let sql = "
            SELECT *
            FROM Player
            JOIN PlayerItem ON
                Player.id = PlayerItem.user_id AND
                PlayerItem.amount > 10
            WHERE True;
        ";
        let actual = plan_join(&storage, sql);
        let expected = {
            let join_executor = JoinExecutor::Hash {
                key_expr: expr("PlayerItem.user_id"),
                value_expr: expr("Player.id"),
                where_clause: Some(expr("PlayerItem.amount > 10")),
            };

            select(Select {
                projection: vec![SelectItem::Wildcard],
                from: TableWithJoins {
                    relation: table_factor("Player", None),
                    joins: vec![Join {
                        relation: table_factor("PlayerItem", None),
                        join_operator: inner(None),
                        join_executor,
                    }],
                },
                selection: Some(expr("True")),
                group_by: Vec::new(),
                having: None,
            })
        };
        assert_eq!(actual, expected, "hash_join expr AND where_clause:\n{sql}");

        let sql = "
            SELECT *
            FROM Player u1
            LEFT OUTER JOIN Player u2
            WHERE u2.id = (
                SELECT u3.id
                FROM Player u3
                JOIN Player u4 ON
                    u4.id = u3.id AND
                    u4.id = u1.id
            );
        ";
        let actual = plan_join(&storage, sql);
        let expected = {
            let subquery = Query {
                body: SetExpr::Select(Box::new(Select {
                    projection: vec![SelectItem::Expr {
                        expr: expr("u3.id"),
                        label: "id".to_owned(),
                    }],
                    from: TableWithJoins {
                        relation: table_factor("Player", Some("u3")),
                        joins: vec![Join {
                            relation: table_factor("Player", Some("u4")),
                            join_operator: inner(None),
                            join_executor: JoinExecutor::Hash {
                                key_expr: expr("u4.id"),
                                value_expr: expr("u3.id"),
                                where_clause: Some(expr("u4.id = u1.id")),
                            },
                        }],
                    },
                    selection: None,
                    group_by: Vec::new(),
                    having: None,
                })),
                order_by: vec![],
                limit: None,
                offset: None,
            };

            select(Select {
                projection: vec![SelectItem::Wildcard],
                from: TableWithJoins {
                    relation: table_factor("Player", Some("u1")),
                    joins: vec![Join {
                        relation: table_factor("Player", Some("u2")),
                        join_operator: left_outer(None),
                        join_executor: JoinExecutor::NestedLoop,
                    }],
                },
                selection: Some(Expr::BinaryOp {
                    left: Box::new(expr("u2.id")),
                    op: BinaryOperator::Eq,
                    right: Box::new(Expr::Subquery(Box::new(subquery))),
                }),
                group_by: Vec::new(),
                having: None,
            })
        };
        assert_eq!(actual, expected, "hash join in subquery:\n{sql}");

        let sql = "
            SELECT * FROM Player u1
            WHERE u1.id = (
                SELECT * FROM Player u2
                WHERE u2.id = (
                    SELECT * FROM Player u3
                    JOIN Player u4 ON
                        u4.id = u3.id + u1.id
                )
            );
        ";
        let actual = plan_join(&storage, sql);
        let expected = {
            let join_executor = JoinExecutor::Hash {
                key_expr: expr("u4.id"),
                value_expr: expr("u3.id + u1.id"),
                where_clause: None,
            };

            let join_subquery = Query {
                body: SetExpr::Select(Box::new(Select {
                    projection: vec![SelectItem::Wildcard],
                    from: TableWithJoins {
                        relation: table_factor("Player", Some("u3")),
                        joins: vec![Join {
                            relation: table_factor("Player", Some("u4")),
                            join_operator: inner(None),
                            join_executor,
                        }],
                    },
                    selection: None,
                    group_by: Vec::new(),
                    having: None,
                })),
                order_by: vec![],
                limit: None,
                offset: None,
            };

            let subquery = Query {
                body: SetExpr::Select(Box::new(Select {
                    projection: vec![SelectItem::Wildcard],
                    from: TableWithJoins {
                        relation: table_factor("Player", Some("u2")),
                        joins: Vec::new(),
                    },
                    selection: Some(Expr::BinaryOp {
                        left: Box::new(expr("u2.id")),
                        op: BinaryOperator::Eq,
                        right: Box::new(Expr::Subquery(Box::new(join_subquery))),
                    }),
                    group_by: Vec::new(),
                    having: None,
                })),
                order_by: vec![],
                limit: None,
                offset: None,
            };

            select(Select {
                projection: vec![SelectItem::Wildcard],
                from: TableWithJoins {
                    relation: table_factor("Player", Some("u1")),
                    joins: Vec::new(),
                },
                selection: Some(Expr::BinaryOp {
                    left: Box::new(expr("u1.id")),
                    op: BinaryOperator::Eq,
                    right: Box::new(Expr::Subquery(Box::new(subquery))),
                }),
                group_by: Vec::new(),
                having: None,
            })
        };
        assert_eq!(actual, expected, "hash join in nested subquery:\n{sql}");

        let sql = "
            SELECT *
            FROM Player
            JOIN PlayerItem ON
                Player.id = PlayerItem.user_id AND
                Player.id > 10 AND
                PlayerItem.item_id IS NOT NULL AND
                PlayerItem.amount > 10
            WHERE True;
        ";
        let actual = plan_join(&storage, sql);
        let expected = {
            let join_executor = JoinExecutor::Hash {
                key_expr: expr("PlayerItem.user_id"),
                value_expr: expr("Player.id"),
                where_clause: Some(expr(
                    "PlayerItem.item_id IS NOT NULL AND PlayerItem.amount > 10",
                )),
            };

            select(Select {
                projection: vec![SelectItem::Wildcard],
                from: TableWithJoins {
                    relation: table_factor("Player", None),
                    joins: vec![Join {
                        relation: table_factor("PlayerItem", None),
                        join_operator: inner(Some("Player.id > 10")),
                        join_executor,
                    }],
                },
                selection: Some(expr("True")),
                group_by: Vec::new(),
                having: None,
            })
        };
        assert_eq!(
            actual, expected,
            "hash join with join_constraint AND where_clause:\n{sql}"
        );

        let sql = "
            SELECT *
            FROM Player
            JOIN PlayerItem ON
                Player.id > Player.id + PlayerItem.user_id AND
                Player.id = PlayerItem.user_id AND
                PlayerItem.item_id IS NOT NULL AND
                PlayerItem.amount > 10
            WHERE True;
        ";
        let actual = plan_join(&storage, sql);
        let expected = {
            let join_executor = JoinExecutor::Hash {
                key_expr: expr("PlayerItem.user_id"),
                value_expr: expr("Player.id"),
                where_clause: Some(expr(
                    "PlayerItem.item_id IS NOT NULL AND PlayerItem.amount > 10",
                )),
            };

            select(Select {
                projection: vec![SelectItem::Wildcard],
                from: TableWithJoins {
                    relation: table_factor("Player", None),
                    joins: vec![Join {
                        relation: table_factor("PlayerItem", None),
                        join_operator: inner(Some("Player.id > Player.id + PlayerItem.user_id")),
                        join_executor,
                    }],
                },
                selection: Some(expr("True")),
                group_by: Vec::new(),
                having: None,
            })
        };
        assert_eq!(
            actual, expected,
            "hash join with join_constraint AND where_clause 2:\n{sql}"
        );
    }

    #[test]
    fn hash_join_in_subquery() {
        let storage = run("
            CREATE TABLE Player (
                id INTEGER,
                name TEXT
            );
            CREATE TABLE Flag (
                id INTEGER,
                user_id INTEGER,
                name TEXT
            );
        ");

        let subquery_sql = "
            SELECT u.id
            FROM Player u
            JOIN Flag f ON f.user_id = u.id
        ";
        let subquery = || {
            let join_executor = JoinExecutor::Hash {
                key_expr: expr("f.user_id"),
                value_expr: expr("u.id"),
                where_clause: None,
            };

            Box::new(Query {
                body: SetExpr::Select(Box::new(Select {
                    projection: vec![SelectItem::Expr {
                        expr: expr("u.id"),
                        label: "id".to_owned(),
                    }],
                    from: TableWithJoins {
                        relation: table_factor("Player", Some("u")),
                        joins: vec![Join {
                            relation: table_factor("Flag", Some("f")),
                            join_operator: inner(None),
                            join_executor,
                        }],
                    },
                    selection: None,
                    group_by: Vec::new(),
                    having: None,
                })),
                order_by: vec![],
                limit: None,
                offset: None,
            })
        };
        let subquery_expr = || Box::new(Expr::Subquery(subquery()));

        let gen_expected = |selection| {
            select(Select {
                projection: vec![SelectItem::Wildcard],
                from: TableWithJoins {
                    relation: table_factor("Player", None),
                    joins: Vec::new(),
                },
                selection: Some(selection),
                group_by: Vec::new(),
                having: None,
            })
        };

        let sql = format!("SELECT * FROM Player WHERE id = ({subquery_sql})");
        let actual = plan_join(&storage, &sql);
        let expected = gen_expected(Expr::BinaryOp {
            left: Box::new(expr("id")),
            op: BinaryOperator::Eq,
            right: subquery_expr(),
        });
        assert_eq!(actual, expected, "binary operator:\n{sql}");

        let sql = format!("SELECT * FROM Player WHERE -({subquery_sql}) IN ({subquery_sql})");
        let actual = plan_join(&storage, &sql);
        let expected = gen_expected(Expr::InSubquery {
            expr: Box::new(Expr::UnaryOp {
                op: UnaryOperator::Minus,
                expr: subquery_expr(),
            }),
            subquery: subquery(),
            negated: false,
        });
        assert_eq!(actual, expected, "unary operator and in subquery:\n{sql}");

        let sql = format!(
            "
            SELECT * FROM Player
            WHERE -({subquery_sql}) IN ({subquery_sql})
        "
        );
        let actual = plan_join(&storage, &sql);
        let expected = gen_expected(Expr::InSubquery {
            expr: Box::new(Expr::UnaryOp {
                op: UnaryOperator::Minus,
                expr: subquery_expr(),
            }),
            subquery: subquery(),
            negated: false,
        });
        assert_eq!(actual, expected, "unary operator and in subquery:\n{sql}");

        let sql = format!(
            "
            SELECT * FROM Player
            WHERE
                CAST(({subquery_sql}) AS INTEGER) IN (1, 2, 3)
        "
        );
        let actual = plan_join(&storage, &sql);
        let expected = gen_expected(Expr::InList {
            expr: Box::new(Expr::Cast {
                expr: subquery_expr(),
                data_type: DataType::Int,
            }),
            list: vec![expr("1"), expr("2"), expr("3")],
            negated: false,
        });
        assert_eq!(actual, expected, "cast and in list:\n{sql}");

        let sql = format!(
            "
            SELECT * FROM Player
            WHERE 
                ({subquery_sql}) IS NULL
                OR
                ({subquery_sql}) IS NOT NULL
        "
        );
        let actual = plan_join(&storage, &sql);
        let expected = gen_expected(Expr::BinaryOp {
            left: Box::new(Expr::IsNull(subquery_expr())),
            op: BinaryOperator::Or,
            right: Box::new(Expr::IsNotNull(subquery_expr())),
        });
        assert_eq!(actual, expected, "is null and is not null:\n{sql}");

        let sql = format!("SELECT * FROM Player WHERE EXISTS({subquery_sql})");
        let actual = plan_join(&storage, &sql);
        let expected = gen_expected(Expr::Exists {
            subquery: subquery(),
            negated: false,
        });
        assert_eq!(actual, expected, "exists:\n{sql}");

        let sql = format!(
            "
            SELECT * FROM Player
            WHERE ({subquery_sql}) BETWEEN ({subquery_sql}) AND 100;
        "
        );
        let actual = plan_join(&storage, &sql);
        let expected = gen_expected(Expr::Between {
            expr: subquery_expr(),
            negated: false,
            low: subquery_expr(),
            high: Box::new(expr("100")),
        });
        assert_eq!(actual, expected, "between:\n{sql}");

        let sql = format!(
            "
            SELECT * FROM Player
            WHERE EXTRACT(HOUR FROM (({subquery_sql}))) IS NULL
        "
        );
        let actual = plan_join(&storage, &sql);
        let expected = gen_expected(Expr::IsNull(Box::new(Expr::Extract {
            field: DateTimeField::Hour,
            expr: Box::new(Expr::Nested(subquery_expr())),
        })));
        assert_eq!(actual, expected, "extract and nested:\n{sql}");

        let sql = format!(
            "
            SELECT * FROM Player
            WHERE
                CASE ({subquery_sql})
                    WHEN 10 THEN True
                    WHEN 20 THEN ({subquery_sql}) IS NULL
                    ELSE col3
                END
        "
        );
        let actual = plan_join(&storage, &sql);
        let expected = gen_expected(Expr::Case {
            operand: Some(subquery_expr()),
            when_then: vec![
                (expr("10"), expr("True")),
                (expr("20"), Expr::IsNull(subquery_expr())),
            ],
            else_result: Some(Box::new(expr("col3"))),
        });
        assert_eq!(actual, expected, "case expr:\n{sql}");
    }
}
