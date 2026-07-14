use {
    self::lookup::PrimaryKeyLookupCandidate,
    super::{context::Context, planner::Planner},
    crate::{
        ast::BinaryOperator,
        data::Schema,
        plan::{
            ExprPlan, IndexItemPlan, QueryPlan, SelectPlan, SetExprPlan, StatementPlan,
            TableFactorPlan, TableWithJoinsPlan, expr::evaluable::check_expr as check_evaluable,
        },
    },
    std::{collections::HashMap, hash::BuildHasher, rc::Rc},
};

mod lookup;

pub fn plan<S: BuildHasher>(
    schema_map: &HashMap<String, Schema, S>,
    statement: StatementPlan,
) -> StatementPlan {
    let planner = PrimaryKeyPlanner { schema_map };

    match statement {
        StatementPlan::Query(query) => {
            let query = planner.query(None, query);

            StatementPlan::Query(query)
        }
        _ => statement,
    }
}

struct PrimaryKeyPlanner<'a, S> {
    schema_map: &'a HashMap<String, Schema, S>,
}

impl<'a, S: BuildHasher> Planner<'a> for PrimaryKeyPlanner<'a, S> {
    fn query(&self, outer_context: Option<Rc<Context<'a>>>, query: QueryPlan) -> QueryPlan {
        let body = match query.body {
            SetExprPlan::Select(select) => {
                let select = self.select(outer_context, *select);

                SetExprPlan::Select(Box::new(select))
            }
            SetExprPlan::Values(_) => query.body,
        };

        QueryPlan { body, ..query }
    }

    fn get_schema(&self, name: &str) -> Option<&'a Schema> {
        self.schema_map.get(name)
    }
}

enum PrimaryKey {
    Found {
        index_item: IndexItemPlan,
        expr: Option<ExprPlan>,
    },
    NotFound(ExprPlan),
}

impl<'a, S: BuildHasher> PrimaryKeyPlanner<'a, S> {
    fn select(&self, outer_context: Option<Rc<Context<'a>>>, select: SelectPlan) -> SelectPlan {
        let first_relation_context = self.update_context(None, &select.from.relation);
        let lookup_candidate = PrimaryKeyLookupCandidate::new(self.schema_map, &select.from);
        let current_context = select
            .from
            .joins
            .iter()
            .fold(first_relation_context, |context, join| {
                self.update_context(context, &join.relation)
            });

        let (index, selection) = select
            .selection
            .map(|expr| {
                self.expr(
                    outer_context,
                    current_context,
                    lookup_candidate.as_ref(),
                    expr,
                )
            })
            .map_or((None, None), |primary_key| match primary_key {
                PrimaryKey::Found { index_item, expr } => (Some(index_item), expr),
                PrimaryKey::NotFound(expr) => (None, Some(expr)),
            });

        if let TableFactorPlan::Table {
            name,
            alias,
            index: None,
        } = select.from.relation
        {
            let from = TableWithJoinsPlan {
                relation: TableFactorPlan::Table { name, alias, index },
                ..select.from
            };

            SelectPlan {
                from,
                selection,
                ..select
            }
        } else {
            SelectPlan {
                selection,
                ..select
            }
        }
    }

    fn expr(
        &self,
        outer_context: Option<Rc<Context<'a>>>,
        current_context: Option<Rc<Context<'a>>>,
        lookup_candidate: Option<&PrimaryKeyLookupCandidate>,
        expr: ExprPlan,
    ) -> PrimaryKey {
        match expr {
            ExprPlan::BinaryOp {
                left: key,
                op: BinaryOperator::Eq,
                right: value,
            }
            | ExprPlan::BinaryOp {
                left: value,
                op: BinaryOperator::Eq,
                right: key,
            } if lookup_candidate.is_some_and(|candidate| candidate.contains(key.as_ref()))
                && check_evaluable(None, &value) =>
            {
                let index_item = IndexItemPlan::PrimaryKey(*value);

                PrimaryKey::Found {
                    index_item,
                    expr: None,
                }
            }
            ExprPlan::BinaryOp {
                left,
                op: BinaryOperator::And,
                right,
            } => {
                let primary_key = self.expr(
                    outer_context.as_ref().map(Rc::clone),
                    current_context.as_ref().map(Rc::clone),
                    lookup_candidate,
                    *left,
                );

                let left = match primary_key {
                    PrimaryKey::Found { index_item, expr } => {
                        let expr = match expr {
                            Some(left) => ExprPlan::BinaryOp {
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

                match self.expr(outer_context, current_context, lookup_candidate, *right) {
                    PrimaryKey::Found { index_item, expr } => {
                        let expr = match expr {
                            Some(right) => ExprPlan::BinaryOp {
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
                        let expr = ExprPlan::BinaryOp {
                            left: Box::new(left),
                            op: BinaryOperator::And,
                            right: Box::new(expr),
                        };

                        PrimaryKey::NotFound(expr)
                    }
                }
            }
            ExprPlan::Nested(expr) => {
                match self.expr(outer_context, current_context, lookup_candidate, *expr) {
                    PrimaryKey::Found { index_item, expr } => {
                        let expr = expr.map(Box::new).map(ExprPlan::Nested);

                        PrimaryKey::Found { index_item, expr }
                    }
                    PrimaryKey::NotFound(expr) => {
                        PrimaryKey::NotFound(ExprPlan::Nested(Box::new(expr)))
                    }
                }
            }
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
                BinaryOperator, Expr, Join, JoinConstraint, JoinOperator, Literal, Projection,
                Query, Select, SelectItem, SetExpr, Statement, TableFactor, TableWithJoins, Values,
            },
            mock::{MockStorage, run},
            parse_sql::{parse, parse_expr},
            plan::{
                ExprPlan, IndexItemPlan, QueryPlan, SetExprPlan, StatementPlan, TableAliasPlan,
                TableFactorPlan, fetch_schema_map,
            },
            query_builder::{Build, col, primary_key, table},
            translate::{NO_PARAMS, translate, translate_expr},
        },
    };

    fn statement(sql: &str) -> StatementPlan {
        let parsed = parse(sql).expect(sql).into_iter().next().unwrap();
        StatementPlan::from(translate(&parsed).unwrap())
    }

    fn plan(storage: &MockStorage, sql: &str) -> StatementPlan {
        let statement = statement(sql);
        let schema_map = fetch_schema_map(storage, &statement).unwrap();

        plan_primary_key(&schema_map, statement)
    }

    fn select(select: Select) -> StatementPlan {
        StatementPlan::from(Statement::Query(Query {
            body: SetExpr::Select(Box::new(select)),
            limit: None,
            offset: None,
            order_by: Vec::new(),
        }))
    }

    fn expr(sql: &str) -> Expr {
        let parsed = parse_expr(sql).expect(sql);

        translate_expr(&parsed, NO_PARAMS).expect(sql)
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
        let expected = table("Player")
            .index_by(primary_key().eq("1"))
            .select()
            .build()
            .unwrap();
        assert_eq!(actual, expected, "primary key in lhs:\n{sql}");

        let sql = "SELECT * FROM Player WHERE 1 = id;";
        let actual = plan(&storage, sql);
        let expected = table("Player")
            .index_by(primary_key().eq("1"))
            .select()
            .build()
            .unwrap();
        assert_eq!(actual, expected, "primary key in rhs:\n{sql}");

        let sql = "SELECT * FROM Player WHERE id = 1 AND True;";
        let actual = plan(&storage, sql);
        let expected = table("Player")
            .index_by(primary_key().eq("1"))
            .select()
            .filter("True")
            .build()
            .unwrap();
        assert_eq!(actual, expected, "AND binary op:\n{sql}");

        let sql = "
            SELECT * FROM Player
            WHERE
                name IS NOT NULL
                AND id = 1
                AND True;
        ";
        let actual = plan(&storage, sql);
        let expected = table("Player")
            .index_by(primary_key().eq("1"))
            .select()
            .filter("name IS NOT NULL AND True")
            .build()
            .unwrap();
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
        let expected = table("Player")
            .index_by(primary_key().eq("1"))
            .select()
            .filter("name IS NOT NULL AND (True)")
            .build()
            .unwrap();
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
        let expected = table("Player")
            .index_by(primary_key().eq("1"))
            .select()
            .join("Badge")
            .build()
            .unwrap();
        assert_eq!(actual, expected, "basic inner join:\n{sql}");

        let sql = "SELECT * FROM Player JOIN Badge WHERE id = 1";
        let actual = plan(&storage, sql);
        assert_eq!(
            actual, expected,
            "unqualified primary key on first relation:\n{sql}"
        );

        let sql = "SELECT * FROM Player p JOIN Badge b WHERE p.id = 1";
        let actual = plan(&storage, sql);
        let expected_relation = TableFactorPlan::Table {
            name: "Player".to_owned(),
            alias: Some(TableAliasPlan {
                name: "p".to_owned(),
                columns: Vec::new(),
            }),
            index: Some(IndexItemPlan::PrimaryKey(ExprPlan::Literal(
                Literal::Number(1.into()),
            ))),
        };
        assert!(
            matches!(
                actual,
                StatementPlan::Query(QueryPlan {
                    body: SetExprPlan::Select(select_plan),
                    ..
                }) if select_plan.from.relation == expected_relation
                    && select_plan.selection.is_none()
            ),
            "aliased primary key should be installed and removed from selection:\n{sql}"
        );

        let sql = "SELECT * FROM Player JOIN Badge WHERE Player.id = Badge.user_id";
        let actual = plan(&storage, sql);
        let expected = select(Select {
            distinct: false,
            projection: Projection::SelectItems(vec![SelectItem::Wildcard]),
            from: TableWithJoins {
                relation: TableFactor::Table {
                    name: "Player".to_owned(),
                    alias: None,
                },
                joins: vec![Join {
                    relation: TableFactor::Table {
                        name: "Badge".to_owned(),
                        alias: None,
                    },
                    join_operator: JoinOperator::Inner(JoinConstraint::None),
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
        let expected = table("Player")
            .select()
            .filter(col("name").in_list(table("Player").index_by(primary_key().eq("1")).select()))
            .build()
            .unwrap();
        assert_eq!(actual, expected, "nested select:\n{sql}");
    }

    #[test]
    fn joined_relation_primary_key() {
        let storage = run("
            CREATE TABLE Tasks (
                task_id INTEGER PRIMARY KEY,
                project_id INTEGER,
                done BOOLEAN NOT NULL
            );
            CREATE TABLE Projects (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL
            );
        ");

        let sql = "
            SELECT *
            FROM Tasks t
            JOIN Projects p ON p.id = t.project_id
            WHERE p.id = 1 AND t.done = FALSE;
        ";
        let actual = plan(&storage, sql);
        let expected = table("Tasks")
            .alias_as("t")
            .select()
            .join_as("Projects", "p")
            .on("p.id = t.project_id")
            .filter("p.id = 1 AND t.done = FALSE")
            .build()
            .unwrap();
        assert_eq!(actual, expected, "qualified joined relation:\n{sql}");

        let sql = "
            SELECT *
            FROM Tasks t
            JOIN Projects p ON p.id = t.project_id
            WHERE id = 1 AND t.done = FALSE;
        ";
        let actual = plan(&storage, sql);
        let expected = table("Tasks")
            .alias_as("t")
            .select()
            .join_as("Projects", "p")
            .on("p.id = t.project_id")
            .filter("id = 1 AND t.done = FALSE")
            .build()
            .unwrap();
        assert_eq!(actual, expected, "unqualified joined relation:\n{sql}");
    }

    #[test]
    fn positional_column_aliases() {
        let storage = run("
            CREATE TABLE Tasks (
                task_id INTEGER PRIMARY KEY,
                project_id INTEGER,
                done BOOLEAN NOT NULL
            );
            CREATE TABLE Projects (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL
            );
        ");

        let sql = "
            SELECT *
            FROM Tasks AS t(id, project_id, done)
            WHERE t.id = 1;
        ";
        let actual = plan(&storage, sql);
        let select = match actual {
            StatementPlan::Query(QueryPlan {
                body: SetExprPlan::Select(select),
                ..
            }) => Some(select),
            _ => None,
        }
        .expect("expected select plan");
        let index = match &select.from.relation {
            TableFactorPlan::Table { index, .. } => Some(index),
            _ => None,
        }
        .expect("expected table relation");

        assert!(
            matches!(index, Some(IndexItemPlan::PrimaryKey(_))),
            "effective primary key alias should install a lookup:\n{sql}"
        );
        assert!(
            select.selection.is_none(),
            "effective primary key alias should remove the selection:\n{sql}"
        );

        let sql = "
            SELECT t.id
            FROM Tasks AS t(id, project_id, done)
            JOIN Projects AS p(task_id, name)
              ON p.task_id = t.project_id
            WHERE task_id = 1
            ORDER BY t.id;
        ";
        let actual = plan(&storage, sql);
        let expected = statement(sql);

        assert_eq!(
            actual, expected,
            "joined positional alias should preserve selection:\n{sql}"
        );

        let storage = run("
            CREATE TABLE Tasks (
                project_id INTEGER,
                task_id INTEGER PRIMARY KEY
            );
        ");
        let sql = "
            SELECT *
            FROM Tasks AS t(id, id)
            WHERE t.id = 1;
        ";
        let actual = plan(&storage, sql);
        let expected = statement(sql);

        assert_eq!(
            actual, expected,
            "shadowed primary key alias should preserve selection:\n{sql}"
        );
    }

    #[test]
    fn existing_access_path_preserves_selection() {
        let storage = run("
            CREATE TABLE Player (
                id INTEGER PRIMARY KEY,
                name TEXT
            );
        ");
        let statement = table("Player")
            .index_by(primary_key().eq("2"))
            .select()
            .filter("id = 1")
            .build()
            .unwrap();
        let schema_map = fetch_schema_map(&storage, &statement).unwrap();
        let actual = plan_primary_key(&schema_map, statement.clone());

        assert_eq!(actual, statement);
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
                    distinct: false,
                    projection: Projection::SelectItems(vec![SelectItem::Expr {
                        expr: Expr::Identifier("name".to_owned()),
                        label: "name".to_owned(),
                    }]),
                    from: TableWithJoins {
                        relation: TableFactor::Table {
                            name: "Player".to_owned(),
                            alias: None,
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
                distinct: false,
                projection: Projection::SelectItems(vec![SelectItem::Wildcard]),
                from: TableWithJoins {
                    relation: TableFactor::Table {
                        name: "Player".to_owned(),
                        alias: None,
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
                    distinct: false,
                    projection: Projection::SelectItems(vec![SelectItem::Expr {
                        expr: Expr::Identifier("id".to_owned()),
                        label: "id".to_owned(),
                    }]),
                    from: TableWithJoins {
                        relation: TableFactor::Table {
                            name: "Player".to_owned(),
                            alias: None,
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
                distinct: false,
                projection: Projection::SelectItems(vec![SelectItem::Wildcard]),
                from: TableWithJoins {
                    relation: TableFactor::Table {
                        name: "Player".to_owned(),
                        alias: None,
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
        let expected = StatementPlan::from(Statement::Delete {
            table_name: "Player".to_owned(),
            selection: Some(Expr::BinaryOp {
                left: Box::new(Expr::Identifier("id".to_owned())),
                op: BinaryOperator::Eq,
                right: Box::new(Expr::Literal(Literal::Number(1.into()))),
            }),
        });
        assert_eq!(actual, expected, "delete statement:\n{sql}");

        let sql = "VALUES (1), (2);";
        let actual = plan(&storage, sql);
        let expected = StatementPlan::from(Statement::Query(Query {
            body: SetExpr::Values(Values(vec![
                vec![Expr::Literal(Literal::Number(1.into()))],
                vec![Expr::Literal(Literal::Number(2.into()))],
            ])),
            limit: None,
            offset: None,
            order_by: Vec::new(),
        }));
        assert_eq!(actual, expected, "values:\n{sql}");

        let sql = "SELECT * FROM Player WHERE (name);";
        let actual = plan(&storage, sql);
        let expected = select(Select {
            distinct: false,
            projection: Projection::SelectItems(vec![SelectItem::Wildcard]),
            from: TableWithJoins {
                relation: TableFactor::Table {
                    name: "Player".to_owned(),
                    alias: None,
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
