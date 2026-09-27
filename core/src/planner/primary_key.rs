use {
    self::lookup::PrimaryKeyLookupCandidate,
    super::{context::Context, expr::evaluable::check_expr as check_evaluable, query::Planner},
    crate::{
        ast::BinaryOperator,
        data::Schema,
        plan::{
            AggregationInputPlan, DistinctInputPlan, DistinctPlan, ExprPlan, FilterInputPlan,
            FilterPlan, HashJoinInputPlan, HashJoinPlan, InnerJoinInputPlan, InnerJoinPlan,
            JoinConditionInputPlan, JoinConditionPlan, LeftOuterJoinInputPlan, LeftOuterJoinPlan,
            LimitInputPlan, LimitPlan, NestedLoopJoinInputPlan, NestedLoopJoinPlan,
            OffsetInputPlan, OffsetPlan, ProjectInputPlan, ProjectPlan, QueryPlan, SourcePlan,
            StatementPlan, TableAccessPlan,
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
        match query {
            QueryPlan::Project(project) => {
                QueryPlan::Project(self.project(outer_context.as_ref(), project))
            }
            QueryPlan::Values(values) => QueryPlan::Values(values),
            QueryPlan::SelectOrderBy(mut order_by) => {
                order_by.input = self.project(outer_context.as_ref(), order_by.input);
                QueryPlan::SelectOrderBy(order_by)
            }
            QueryPlan::ValuesOrderBy(order_by) => QueryPlan::ValuesOrderBy(order_by),
            QueryPlan::Distinct(distinct) => {
                QueryPlan::Distinct(self.distinct(outer_context.as_ref(), distinct))
            }
            QueryPlan::Offset(OffsetPlan { input, count }) => QueryPlan::Offset(OffsetPlan {
                input: match input {
                    OffsetInputPlan::Project(project) => {
                        OffsetInputPlan::Project(self.project(outer_context.as_ref(), project))
                    }
                    OffsetInputPlan::Values(values) => OffsetInputPlan::Values(values),
                    OffsetInputPlan::SelectOrderBy(mut order_by) => {
                        order_by.input = self.project(outer_context.as_ref(), order_by.input);
                        OffsetInputPlan::SelectOrderBy(order_by)
                    }
                    OffsetInputPlan::ValuesOrderBy(order_by) => {
                        OffsetInputPlan::ValuesOrderBy(order_by)
                    }
                    OffsetInputPlan::Distinct(distinct) => {
                        OffsetInputPlan::Distinct(self.distinct(outer_context.as_ref(), distinct))
                    }
                },
                count,
            }),
            QueryPlan::Limit(LimitPlan { input, count }) => {
                let input = match input {
                    LimitInputPlan::Project(project) => {
                        LimitInputPlan::Project(self.project(outer_context.as_ref(), project))
                    }
                    LimitInputPlan::Values(values) => LimitInputPlan::Values(values),
                    LimitInputPlan::SelectOrderBy(mut order_by) => {
                        order_by.input = self.project(outer_context.as_ref(), order_by.input);
                        LimitInputPlan::SelectOrderBy(order_by)
                    }
                    LimitInputPlan::ValuesOrderBy(order_by) => {
                        LimitInputPlan::ValuesOrderBy(order_by)
                    }
                    LimitInputPlan::Distinct(distinct) => {
                        LimitInputPlan::Distinct(self.distinct(outer_context.as_ref(), distinct))
                    }
                    LimitInputPlan::Offset(OffsetPlan { input, count }) => {
                        LimitInputPlan::Offset(OffsetPlan {
                            input: match input {
                                OffsetInputPlan::Project(project) => OffsetInputPlan::Project(
                                    self.project(outer_context.as_ref(), project),
                                ),
                                OffsetInputPlan::Values(values) => OffsetInputPlan::Values(values),
                                OffsetInputPlan::SelectOrderBy(mut order_by) => {
                                    order_by.input =
                                        self.project(outer_context.as_ref(), order_by.input);
                                    OffsetInputPlan::SelectOrderBy(order_by)
                                }
                                OffsetInputPlan::ValuesOrderBy(order_by) => {
                                    OffsetInputPlan::ValuesOrderBy(order_by)
                                }
                                OffsetInputPlan::Distinct(distinct) => OffsetInputPlan::Distinct(
                                    self.distinct(outer_context.as_ref(), distinct),
                                ),
                            },
                            count,
                        })
                    }
                };

                QueryPlan::Limit(LimitPlan { input, count })
            }
        }
    }

    fn get_schema(&self, name: &str) -> Option<&'a Schema> {
        self.schema_map.get(name)
    }
}

enum PrimaryKey {
    Found {
        access: TableAccessPlan,
        expr: Option<ExprPlan>,
    },
    NotFound(ExprPlan),
}

impl<'a, S: BuildHasher> PrimaryKeyPlanner<'a, S> {
    fn project(
        &self,
        outer_context: Option<&Rc<Context<'a>>>,
        mut project: ProjectPlan,
    ) -> ProjectPlan {
        project.input = match project.input {
            ProjectInputPlan::Source(relation) => ProjectInputPlan::Source(relation),
            ProjectInputPlan::InnerJoin(join) => {
                ProjectInputPlan::InnerJoin(Box::new(self.inner_join(outer_context, *join)))
            }
            ProjectInputPlan::LeftOuterJoin(join) => ProjectInputPlan::LeftOuterJoin(Box::new(
                self.left_outer_join(outer_context, *join),
            )),
            ProjectInputPlan::Filter(filter) => {
                let (input, expr) = self.filter(outer_context.map(Rc::clone), filter);
                match expr {
                    Some(expr) => ProjectInputPlan::Filter(FilterPlan { input, expr }),
                    None => match input {
                        FilterInputPlan::Source(relation) => ProjectInputPlan::Source(relation),
                        FilterInputPlan::InnerJoin(join) => ProjectInputPlan::InnerJoin(join),
                        FilterInputPlan::LeftOuterJoin(join) => {
                            ProjectInputPlan::LeftOuterJoin(join)
                        }
                    },
                }
            }
            ProjectInputPlan::Aggregation(mut aggregation) => {
                aggregation.input =
                    self.aggregation_input(outer_context.map(Rc::clone), aggregation.input);
                ProjectInputPlan::Aggregation(aggregation)
            }
            ProjectInputPlan::Having(mut having) => {
                having.input.input =
                    self.aggregation_input(outer_context.map(Rc::clone), having.input.input);
                ProjectInputPlan::Having(having)
            }
        };

        project
    }

    fn distinct(
        &self,
        outer_context: Option<&Rc<Context<'a>>>,
        DistinctPlan { input }: DistinctPlan,
    ) -> DistinctPlan {
        let input = match input {
            DistinctInputPlan::Project(project) => {
                DistinctInputPlan::Project(self.project(outer_context, project))
            }
            DistinctInputPlan::SelectOrderBy(mut order_by) => {
                order_by.input = self.project(outer_context, order_by.input);
                DistinctInputPlan::SelectOrderBy(order_by)
            }
        };

        DistinctPlan { input }
    }

    fn aggregation_input(
        &self,
        outer_context: Option<Rc<Context<'a>>>,
        input: AggregationInputPlan,
    ) -> AggregationInputPlan {
        match input {
            AggregationInputPlan::Source(relation) => AggregationInputPlan::Source(relation),
            AggregationInputPlan::InnerJoin(join) => AggregationInputPlan::InnerJoin(Box::new(
                self.inner_join(outer_context.as_ref(), *join),
            )),
            AggregationInputPlan::LeftOuterJoin(join) => AggregationInputPlan::LeftOuterJoin(
                Box::new(self.left_outer_join(outer_context.as_ref(), *join)),
            ),
            AggregationInputPlan::Filter(filter) => {
                let (input, expr) = self.filter(outer_context, filter);
                match expr {
                    Some(expr) => AggregationInputPlan::Filter(FilterPlan { input, expr }),
                    None => match input {
                        FilterInputPlan::Source(relation) => AggregationInputPlan::Source(relation),
                        FilterInputPlan::InnerJoin(join) => AggregationInputPlan::InnerJoin(join),
                        FilterInputPlan::LeftOuterJoin(join) => {
                            AggregationInputPlan::LeftOuterJoin(join)
                        }
                    },
                }
            }
        }
    }

    fn filter(
        &self,
        outer_context: Option<Rc<Context<'a>>>,
        filter: FilterPlan,
    ) -> (FilterInputPlan, Option<ExprPlan>) {
        let FilterPlan { mut input, expr } = filter;
        input = match input {
            FilterInputPlan::Source(source) => FilterInputPlan::Source(source),
            FilterInputPlan::InnerJoin(join) => {
                FilterInputPlan::InnerJoin(Box::new(self.inner_join(outer_context.as_ref(), *join)))
            }
            FilterInputPlan::LeftOuterJoin(join) => FilterInputPlan::LeftOuterJoin(Box::new(
                self.left_outer_join(outer_context.as_ref(), *join),
            )),
        };
        let current_context = self.input_context(&input);
        let lookup_candidate = PrimaryKeyLookupCandidate::new(self.schema_map, &input);

        let (access, expr) = match self.expr(
            outer_context,
            current_context,
            lookup_candidate.as_ref(),
            expr,
        ) {
            PrimaryKey::Found { access, expr } => (Some(access), expr),
            PrimaryKey::NotFound(expr) => (None, Some(expr)),
        };

        if let SourcePlan::Table(table) = input.base_source_mut()
            && table.access == TableAccessPlan::FullScan
            && let Some(access) = access
        {
            table.access = access;
        }

        (input, expr)
    }

    fn input_context(&self, input: &FilterInputPlan) -> Option<Rc<Context<'a>>> {
        self.source_context(input.base_source(), input.joined_sources())
    }

    fn source_context(
        &self,
        base_source: &SourcePlan,
        joined_sources: Vec<&SourcePlan>,
    ) -> Option<Rc<Context<'a>>> {
        joined_sources
            .into_iter()
            .fold(self.update_context(None, base_source), |context, source| {
                self.update_context(context, source)
            })
    }

    fn inner_join(
        &self,
        outer_context: Option<&Rc<Context<'a>>>,
        join: InnerJoinPlan,
    ) -> InnerJoinPlan {
        let input = match join.input {
            InnerJoinInputPlan::NestedLoop(join) => {
                InnerJoinInputPlan::NestedLoop(self.nested_loop(outer_context, join))
            }
            InnerJoinInputPlan::Hash(join) => {
                InnerJoinInputPlan::Hash(self.hash(outer_context, join))
            }
            InnerJoinInputPlan::Condition(condition) => {
                InnerJoinInputPlan::Condition(self.join_condition(outer_context, condition))
            }
        };

        InnerJoinPlan { input }
    }

    fn left_outer_join(
        &self,
        outer_context: Option<&Rc<Context<'a>>>,
        join: LeftOuterJoinPlan,
    ) -> LeftOuterJoinPlan {
        let input = match join.input {
            LeftOuterJoinInputPlan::NestedLoop(join) => {
                LeftOuterJoinInputPlan::NestedLoop(self.nested_loop(outer_context, join))
            }
            LeftOuterJoinInputPlan::Hash(join) => {
                LeftOuterJoinInputPlan::Hash(self.hash(outer_context, join))
            }
            LeftOuterJoinInputPlan::Condition(condition) => {
                LeftOuterJoinInputPlan::Condition(self.join_condition(outer_context, condition))
            }
        };

        LeftOuterJoinPlan { input }
    }

    fn join_condition(
        &self,
        outer_context: Option<&Rc<Context<'a>>>,
        condition: JoinConditionPlan,
    ) -> JoinConditionPlan {
        let JoinConditionPlan { input, expr } = condition;
        let input = match input {
            JoinConditionInputPlan::NestedLoop(join) => {
                JoinConditionInputPlan::NestedLoop(self.nested_loop(outer_context, join))
            }
            JoinConditionInputPlan::Hash(join) => {
                JoinConditionInputPlan::Hash(self.hash(outer_context, join))
            }
        };
        let current_context = match &input {
            JoinConditionInputPlan::NestedLoop(join) => {
                self.source_context(join.base_source(), join.joined_sources())
            }
            JoinConditionInputPlan::Hash(join) => {
                self.source_context(join.base_source(), join.joined_sources())
            }
        };
        let expr_context = Context::concat(current_context, outer_context.map(Rc::clone));
        let expr = self.subquery_expr(expr_context, expr);

        JoinConditionPlan { input, expr }
    }

    fn nested_loop(
        &self,
        outer_context: Option<&Rc<Context<'a>>>,
        join: NestedLoopJoinPlan,
    ) -> NestedLoopJoinPlan {
        let input = match join.input {
            NestedLoopJoinInputPlan::Source(source) => NestedLoopJoinInputPlan::Source(source),
            NestedLoopJoinInputPlan::InnerJoin(join) => {
                NestedLoopJoinInputPlan::InnerJoin(Box::new(self.inner_join(outer_context, *join)))
            }
            NestedLoopJoinInputPlan::LeftOuterJoin(join) => NestedLoopJoinInputPlan::LeftOuterJoin(
                Box::new(self.left_outer_join(outer_context, *join)),
            ),
        };

        NestedLoopJoinPlan {
            input,
            right: join.right,
        }
    }

    fn hash(&self, outer_context: Option<&Rc<Context<'a>>>, join: HashJoinPlan) -> HashJoinPlan {
        let HashJoinPlan {
            input,
            right,
            input_key,
            right_key,
            right_filter,
        } = join;
        let input = match input {
            HashJoinInputPlan::Source(source) => HashJoinInputPlan::Source(source),
            HashJoinInputPlan::InnerJoin(join) => {
                HashJoinInputPlan::InnerJoin(Box::new(self.inner_join(outer_context, *join)))
            }
            HashJoinInputPlan::LeftOuterJoin(join) => HashJoinInputPlan::LeftOuterJoin(Box::new(
                self.left_outer_join(outer_context, *join),
            )),
        };
        let input_context = match &input {
            HashJoinInputPlan::Source(source) => self.update_context(None, source),
            HashJoinInputPlan::InnerJoin(join) => {
                self.source_context(join.base_source(), join.joined_sources())
            }
            HashJoinInputPlan::LeftOuterJoin(join) => {
                self.source_context(join.base_source(), join.joined_sources())
            }
        };
        let input_context = Context::concat(input_context, outer_context.map(Rc::clone));
        let right_context = Context::concat(
            self.update_context(None, &right),
            outer_context.map(Rc::clone),
        );
        let input_key = self.subquery_expr(input_context, input_key);
        let right_key = self.subquery_expr(right_context.as_ref().map(Rc::clone), right_key);
        let right_filter = right_filter
            .map(|expr| self.subquery_expr(right_context.as_ref().map(Rc::clone), expr));

        HashJoinPlan {
            input,
            right,
            input_key,
            right_key,
            right_filter,
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
                let access = TableAccessPlan::PrimaryKey { expr: *value };

                PrimaryKey::Found { access, expr: None }
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
                    PrimaryKey::Found { access, expr } => {
                        let context = Context::concat(current_context, outer_context);
                        let right = self.subquery_expr(context, *right);
                        let expr = match expr {
                            Some(left) => ExprPlan::BinaryOp {
                                left: Box::new(left),
                                op: BinaryOperator::And,
                                right: Box::new(right),
                            },
                            None => right,
                        };

                        return PrimaryKey::Found {
                            access,
                            expr: Some(expr),
                        };
                    }
                    PrimaryKey::NotFound(expr) => expr,
                };

                match self.expr(outer_context, current_context, lookup_candidate, *right) {
                    PrimaryKey::Found { access, expr } => {
                        let expr = match expr {
                            Some(right) => ExprPlan::BinaryOp {
                                left: Box::new(left),
                                op: BinaryOperator::And,
                                right: Box::new(right),
                            },
                            None => left,
                        };

                        PrimaryKey::Found {
                            access,
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
                    PrimaryKey::Found { access, expr } => {
                        let expr = expr.map(Box::new).map(ExprPlan::Nested);

                        PrimaryKey::Found { access, expr }
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
                ExprPlan, ProjectInputPlan, QueryPlan, SourcePlan, StatementPlan, TableAccessPlan,
                TableAliasPlan, TableSourcePlan,
            },
            planner::fetch_schema_map,
            query_builder::{Build, col, exists, primary_key, subquery, table},
            translate::{NO_PARAMS, translate, translate_expr},
        },
    };

    fn statement(sql: &str) -> StatementPlan {
        let parsed = parse(sql).expect(sql).into_iter().next().unwrap();
        StatementPlan::from(translate(&parsed).unwrap())
    }

    fn direct_project_base_source(statement: &StatementPlan) -> Option<&SourcePlan> {
        match statement {
            StatementPlan::Query(QueryPlan::Project(project)) => match &project.input {
                ProjectInputPlan::Source(_)
                | ProjectInputPlan::InnerJoin(_)
                | ProjectInputPlan::LeftOuterJoin(_) => Some(project.input.base_source()),
                ProjectInputPlan::Filter(_)
                | ProjectInputPlan::Aggregation(_)
                | ProjectInputPlan::Having(_) => None,
            },
            _ => None,
        }
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

        let sql = "SELECT * FROM Player ORDER BY id OFFSET 1";
        let actual = plan(&storage, sql);
        let expected = table("Player")
            .select()
            .order_by("id")
            .offset(1)
            .build()
            .unwrap();
        assert_eq!(actual, expected, "preserves order by before offset:\n{sql}");

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

        let sql = "SELECT id FROM Player WHERE id = 1 GROUP BY id";
        let actual = plan(&storage, sql);
        let expected = table("Player")
            .index_by(primary_key().eq("1"))
            .select()
            .group_by("id")
            .project("id")
            .build()
            .unwrap();
        assert_eq!(actual, expected, "preserves aggregation wrapper:\n{sql}");

        let sql = "SELECT id FROM Player WHERE id = 1 GROUP BY id HAVING TRUE";
        let actual = plan(&storage, sql);
        let expected = table("Player")
            .index_by(primary_key().eq("1"))
            .select()
            .group_by("id")
            .having("TRUE")
            .project("id")
            .build()
            .unwrap();
        assert_eq!(actual, expected, "preserves having wrapper:\n{sql}");
    }

    #[test]
    fn typed_terminal_inputs() {
        let storage = run("
            CREATE TABLE Player (
                id INTEGER PRIMARY KEY,
                name TEXT
            );
        ");

        macro_rules! test_unchanged {
            ($sql: literal) => {
                let actual = plan(&storage, $sql);
                let expected = statement($sql);
                assert_eq!(actual, expected);
            };
        }

        test_unchanged!("VALUES (1)");
        test_unchanged!("VALUES (1) ORDER BY column1");
        test_unchanged!("VALUES (1) OFFSET 2");
        test_unchanged!("VALUES (1) ORDER BY column1 OFFSET 2");
        test_unchanged!("VALUES (1) LIMIT 3");
        test_unchanged!("VALUES (1) ORDER BY column1 LIMIT 3");
        test_unchanged!("VALUES (1) OFFSET 2 LIMIT 3");
        test_unchanged!("VALUES (1) ORDER BY column1 OFFSET 2 LIMIT 3");

        let sql = "SELECT DISTINCT * FROM Player WHERE id = 1";
        let actual = plan(&storage, sql);
        let expected = table("Player")
            .index_by(primary_key().eq("1"))
            .select()
            .distinct()
            .build()
            .unwrap();
        assert_eq!(actual, expected, "distinct project:\n{sql}");

        let sql = "SELECT * FROM Player WHERE id = 1 OFFSET 2";
        let actual = plan(&storage, sql);
        let expected = table("Player")
            .index_by(primary_key().eq("1"))
            .select()
            .offset(2)
            .build()
            .unwrap();
        assert_eq!(actual, expected, "offset project:\n{sql}");

        let sql = "SELECT DISTINCT * FROM Player WHERE id = 1 OFFSET 2";
        let actual = plan(&storage, sql);
        let expected = table("Player")
            .index_by(primary_key().eq("1"))
            .select()
            .distinct()
            .offset(2)
            .build()
            .unwrap();
        assert_eq!(actual, expected, "offset distinct:\n{sql}");

        let sql = "SELECT * FROM Player WHERE id = 1 ORDER BY id LIMIT 3";
        let actual = plan(&storage, sql);
        let expected = table("Player")
            .index_by(primary_key().eq("1"))
            .select()
            .order_by("id")
            .limit(3)
            .build()
            .unwrap();
        assert_eq!(actual, expected, "limit order by:\n{sql}");

        let sql = "SELECT DISTINCT * FROM Player WHERE id = 1 LIMIT 3";
        let actual = plan(&storage, sql);
        let expected = table("Player")
            .index_by(primary_key().eq("1"))
            .select()
            .distinct()
            .limit(3)
            .build()
            .unwrap();
        assert_eq!(actual, expected, "limit distinct:\n{sql}");

        let sql = "SELECT * FROM Player WHERE id = 1 OFFSET 2 LIMIT 3";
        let actual = plan(&storage, sql);
        let expected = table("Player")
            .index_by(primary_key().eq("1"))
            .select()
            .offset(2)
            .limit(3)
            .build()
            .unwrap();
        assert_eq!(actual, expected, "limit offset project:\n{sql}");

        let sql = "SELECT * FROM Player WHERE id = 1 ORDER BY id OFFSET 2 LIMIT 3";
        let actual = plan(&storage, sql);
        let expected = table("Player")
            .index_by(primary_key().eq("1"))
            .select()
            .order_by("id")
            .offset(2)
            .limit(3)
            .build()
            .unwrap();
        assert_eq!(actual, expected, "limit offset order by:\n{sql}");

        let sql = "SELECT DISTINCT * FROM Player WHERE id = 1 ORDER BY id OFFSET 2 LIMIT 3";
        let actual = plan(&storage, sql);
        let expected = table("Player")
            .index_by(primary_key().eq("1"))
            .select()
            .order_by("id")
            .distinct()
            .offset(2)
            .limit(3)
            .build()
            .unwrap();
        assert_eq!(actual, expected, "limit offset distinct:\n{sql}");
    }

    #[test]
    fn typed_source_inputs() {
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

        let sql = "SELECT * FROM Player JOIN Badge";
        let actual = plan(&storage, sql);
        let expected = table("Player").select().join("Badge").build().unwrap();
        assert_eq!(actual, expected, "inner join project:\n{sql}");

        let sql = "SELECT * FROM Player LEFT JOIN Badge";
        let actual = plan(&storage, sql);
        let expected = table("Player").select().left_join("Badge").build().unwrap();
        assert_eq!(actual, expected, "left outer join project:\n{sql}");

        let sql = "SELECT id FROM Player GROUP BY id";
        let actual = plan(&storage, sql);
        let expected = table("Player")
            .select()
            .group_by("id")
            .project("id")
            .build()
            .unwrap();
        assert_eq!(actual, expected, "source aggregation:\n{sql}");

        let sql = "SELECT Player.id FROM Player JOIN Badge GROUP BY Player.id";
        let actual = plan(&storage, sql);
        let expected = table("Player")
            .select()
            .join("Badge")
            .group_by("Player.id")
            .project("Player.id")
            .build()
            .unwrap();
        assert_eq!(actual, expected, "inner join aggregation:\n{sql}");

        let sql = "SELECT Player.id FROM Player LEFT JOIN Badge GROUP BY Player.id";
        let actual = plan(&storage, sql);
        let expected = table("Player")
            .select()
            .left_join("Badge")
            .group_by("Player.id")
            .project("Player.id")
            .build()
            .unwrap();
        assert_eq!(actual, expected, "left outer join aggregation:\n{sql}");

        let sql = "SELECT id FROM Player WHERE name = 'Alice' GROUP BY id";
        let actual = plan(&storage, sql);
        let expected = table("Player")
            .select()
            .filter("name = 'Alice'")
            .group_by("id")
            .project("id")
            .build()
            .unwrap();
        assert_eq!(
            actual, expected,
            "aggregation preserves residual filter:\n{sql}"
        );

        let sql = "
            SELECT Player.id
            FROM Player JOIN Badge
            WHERE Player.id = 1
            GROUP BY Player.id
        ";
        let actual = plan(&storage, sql);
        let expected = table("Player")
            .index_by(primary_key().eq("1"))
            .select()
            .join("Badge")
            .group_by("Player.id")
            .project("Player.id")
            .build()
            .unwrap();
        assert_eq!(
            actual, expected,
            "inner join aggregation consumes filter:\n{sql}"
        );

        let sql = "
            SELECT Player.id
            FROM Player LEFT JOIN Badge
            WHERE Player.id = 1
            GROUP BY Player.id
        ";
        let actual = plan(&storage, sql);
        let expected = table("Player")
            .index_by(primary_key().eq("1"))
            .select()
            .left_join("Badge")
            .group_by("Player.id")
            .project("Player.id")
            .build()
            .unwrap();
        assert_eq!(
            actual, expected,
            "left outer join aggregation consumes filter:\n{sql}"
        );
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
            CREATE TABLE Team (
                id INTEGER PRIMARY KEY
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
        let expected_relation = SourcePlan::Table(TableSourcePlan {
            name: "Player".to_owned(),
            alias: Some(TableAliasPlan {
                name: "p".to_owned(),
                columns: Vec::new(),
            }),
            access: TableAccessPlan::PrimaryKey {
                expr: ExprPlan::Literal(Literal::Number(1.into())),
            },
        });
        let actual_relation = direct_project_base_source(&actual).expect("expected relation");
        assert!(
            actual_relation == &expected_relation,
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

        let sql = "
            SELECT *
            FROM Player
            WHERE id = 1
              AND EXISTS (
                SELECT * FROM Player WHERE id = 2
              )
        ";
        let actual = plan(&storage, sql);
        let expected = table("Player")
            .index_by(primary_key().eq("1"))
            .select()
            .filter(exists(
                table("Player").index_by(primary_key().eq("2")).select(),
            ))
            .build()
            .unwrap();
        assert_eq!(
            actual, expected,
            "right conjunct plans nested primary key lookup:\n{sql}"
        );

        let sql = "
            SELECT *
            FROM Player
            WHERE (id = 1 AND name IS NOT NULL)
              AND EXISTS (
                SELECT * FROM Player WHERE id = 2
              )
        ";
        let actual = plan(&storage, sql);
        let expected = table("Player")
            .index_by(primary_key().eq("1"))
            .select()
            .filter(col("name").is_not_null().nested().and(exists(
                table("Player").index_by(primary_key().eq("2")).select(),
            )))
            .build()
            .unwrap();
        assert_eq!(
            actual, expected,
            "preserves left residual and plans right conjunct subquery:\n{sql}"
        );

        let sql = "
            SELECT *
            FROM Player
            JOIN Badge ON Badge.user_id = (
                SELECT id FROM Player WHERE id = 1
            )
        ";
        let actual = plan(&storage, sql);
        let expected = table("Player")
            .select()
            .join("Badge")
            .on(col("Badge.user_id").eq(subquery(
                table("Player")
                    .index_by(primary_key().eq("1"))
                    .select()
                    .project("id"),
            )))
            .build()
            .unwrap();
        assert_eq!(
            actual, expected,
            "join condition plans nested primary key lookup:\n{sql}"
        );

        let sql = "
            SELECT *
            FROM Player
            JOIN Badge
            JOIN Team ON EXISTS (
                SELECT * FROM Player WHERE id = 1
            )
        ";
        let actual = plan(&storage, sql);
        let expected = table("Player")
            .select()
            .join("Badge")
            .join("Team")
            .on(exists(
                table("Player").index_by(primary_key().eq("1")).select(),
            ))
            .build()
            .unwrap();
        assert_eq!(
            actual, expected,
            "nested loop plans through inner join input:\n{sql}"
        );

        let sql = "
            SELECT *
            FROM Player
            LEFT JOIN Badge
            JOIN Team ON EXISTS (
                SELECT * FROM Player WHERE id = 1
            )
        ";
        let actual = plan(&storage, sql);
        let expected = table("Player")
            .select()
            .left_join("Badge")
            .join("Team")
            .on(exists(
                table("Player").index_by(primary_key().eq("1")).select(),
            ))
            .build()
            .unwrap();
        assert_eq!(
            actual, expected,
            "nested loop plans through left outer join input:\n{sql}"
        );
    }

    #[test]
    fn hash_join_nested_queries() {
        let storage = run("
            CREATE TABLE Player (
                id INTEGER PRIMARY KEY,
                name TEXT
            );
            CREATE TABLE Badge (
                title TEXT PRIMARY KEY,
                user_id INTEGER
            );
            CREATE TABLE Team (
                id INTEGER PRIMARY KEY
            );
        ");
        let statement = table("Player")
            .select()
            .join("Badge")
            .hash_executor(
                subquery(table("Player").select().filter("id = 1").project("id")),
                subquery(table("Player").select().filter("id = 2").project("id")),
            )
            .hash_filter(exists(table("Player").select().filter("id = 3")))
            .on(exists(table("Player").select().filter("id = 4")))
            .build()
            .unwrap();
        let schema_map = fetch_schema_map(&storage, &statement).unwrap();
        let actual = plan_primary_key(&schema_map, statement);
        let expected = table("Player")
            .select()
            .join("Badge")
            .hash_executor(
                subquery(
                    table("Player")
                        .index_by(primary_key().eq("1"))
                        .select()
                        .project("id"),
                ),
                subquery(
                    table("Player")
                        .index_by(primary_key().eq("2"))
                        .select()
                        .project("id"),
                ),
            )
            .hash_filter(exists(
                table("Player").index_by(primary_key().eq("3")).select(),
            ))
            .on(exists(
                table("Player").index_by(primary_key().eq("4")).select(),
            ))
            .build()
            .unwrap();

        assert_eq!(actual, expected);

        let statement = table("Player")
            .select()
            .join("Badge")
            .join("Team")
            .hash_executor(
                subquery(table("Player").select().filter("id = 1").project("id")),
                subquery(table("Player").select().filter("id = 2").project("id")),
            )
            .build()
            .unwrap();
        let schema_map = fetch_schema_map(&storage, &statement).unwrap();
        let actual = plan_primary_key(&schema_map, statement);
        let expected = table("Player")
            .select()
            .join("Badge")
            .join("Team")
            .hash_executor(
                subquery(
                    table("Player")
                        .index_by(primary_key().eq("1"))
                        .select()
                        .project("id"),
                ),
                subquery(
                    table("Player")
                        .index_by(primary_key().eq("2"))
                        .select()
                        .project("id"),
                ),
            )
            .build()
            .unwrap();
        assert_eq!(actual, expected);

        let statement = table("Player")
            .select()
            .left_join("Badge")
            .left_join("Team")
            .hash_executor(
                subquery(table("Player").select().filter("id = 1").project("id")),
                subquery(table("Player").select().filter("id = 2").project("id")),
            )
            .build()
            .unwrap();
        let schema_map = fetch_schema_map(&storage, &statement).unwrap();
        let actual = plan_primary_key(&schema_map, statement);
        let expected = table("Player")
            .select()
            .left_join("Badge")
            .left_join("Team")
            .hash_executor(
                subquery(
                    table("Player")
                        .index_by(primary_key().eq("1"))
                        .select()
                        .project("id"),
                ),
                subquery(
                    table("Player")
                        .index_by(primary_key().eq("2"))
                        .select()
                        .project("id"),
                ),
            )
            .build()
            .unwrap();
        assert_eq!(actual, expected);
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
    fn left_outer_join_installs_lookup_on_first_relation() {
        let storage = run("
            CREATE TABLE Tasks (
                task_id INTEGER PRIMARY KEY,
                project_id INTEGER
            );
            CREATE TABLE Projects (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL
            );
        ");
        let sql = "
            SELECT *
            FROM Tasks t
            LEFT JOIN Projects p ON p.id = t.project_id
            WHERE t.task_id = 1;
        ";

        let actual = plan(&storage, sql);
        let relation = direct_project_base_source(&actual).expect("expected relation");
        let expected = SourcePlan::Table(TableSourcePlan {
            name: "Tasks".to_owned(),
            alias: Some(TableAliasPlan {
                name: "t".to_owned(),
                columns: Vec::new(),
            }),
            access: TableAccessPlan::PrimaryKey {
                expr: ExprPlan::Literal(Literal::Number(1.into())),
            },
        });

        assert_eq!(relation, &expected, "{sql}");
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
        let relation = direct_project_base_source(&actual).expect("expected relation");
        let expected = SourcePlan::Table(TableSourcePlan {
            name: "Tasks".to_owned(),
            alias: Some(TableAliasPlan {
                name: "t".to_owned(),
                columns: vec!["id".to_owned(), "project_id".to_owned(), "done".to_owned()],
            }),
            access: TableAccessPlan::PrimaryKey {
                expr: ExprPlan::Literal(Literal::Number(1.into())),
            },
        });

        assert_eq!(relation, &expected, "{sql}");

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
    fn direct_project_base_source_rejects_values() {
        assert!(direct_project_base_source(&statement("VALUES (1)")).is_none());
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
