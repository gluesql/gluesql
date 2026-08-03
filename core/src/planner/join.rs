use {
    super::{context::Context, expr::evaluable::check_expr as check_evaluable, query::Planner},
    crate::{
        ast::BinaryOperator,
        data::Schema,
        plan::{
            AggregationInputPlan, DistinctInputPlan, DistinctPlan, ExprPlan, FilterInputPlan,
            FilterPlan, HashJoinInputPlan, HashJoinPlan, InnerJoinInputPlan, InnerJoinPlan,
            JoinConditionInputPlan, JoinConditionPlan, LeftOuterJoinInputPlan, LeftOuterJoinPlan,
            LimitInputPlan, LimitPlan, NestedLoopJoinInputPlan, NestedLoopJoinPlan,
            OffsetInputPlan, OffsetPlan, ProjectInputPlan, ProjectPlan, QueryPlan, StatementPlan,
        },
    },
    std::{collections::HashMap, hash::BuildHasher, rc::Rc},
};

pub fn plan<S: BuildHasher>(
    schema_map: &HashMap<String, Schema, S>,
    statement: StatementPlan,
) -> StatementPlan {
    let planner = JoinPlanner { schema_map };

    match statement {
        StatementPlan::Query(query) => {
            let query = planner.query(None, query);

            StatementPlan::Query(query)
        }
        _ => statement,
    }
}

struct JoinPlanner<'a, S> {
    schema_map: &'a HashMap<String, Schema, S>,
}

impl<'a, S: BuildHasher> Planner<'a> for JoinPlanner<'a, S> {
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

impl<'a, S: BuildHasher> JoinPlanner<'a, S> {
    fn project(
        &self,
        outer_context: Option<&Rc<Context<'a>>>,
        mut project: ProjectPlan,
    ) -> ProjectPlan {
        project.input = match project.input {
            ProjectInputPlan::Source(relation) => ProjectInputPlan::Source(relation),
            ProjectInputPlan::InnerJoin(join) => {
                let (_, join) = self.inner_join(outer_context, *join);
                ProjectInputPlan::InnerJoin(Box::new(join))
            }
            ProjectInputPlan::LeftOuterJoin(join) => {
                let (_, join) = self.left_outer_join(outer_context, *join);
                ProjectInputPlan::LeftOuterJoin(Box::new(join))
            }
            ProjectInputPlan::Filter(filter) => {
                ProjectInputPlan::Filter(self.filter(outer_context.map(Rc::clone), filter))
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
            AggregationInputPlan::InnerJoin(join) => {
                let (_, join) = self.inner_join(outer_context.as_ref(), *join);
                AggregationInputPlan::InnerJoin(Box::new(join))
            }
            AggregationInputPlan::LeftOuterJoin(join) => {
                let (_, join) = self.left_outer_join(outer_context.as_ref(), *join);
                AggregationInputPlan::LeftOuterJoin(Box::new(join))
            }
            AggregationInputPlan::Filter(filter) => {
                AggregationInputPlan::Filter(self.filter(outer_context, filter))
            }
        }
    }

    fn filter(&self, outer_context: Option<Rc<Context<'a>>>, filter: FilterPlan) -> FilterPlan {
        let FilterPlan { input, expr } = filter;
        let (context, input) = self.filter_input(outer_context.as_ref(), input);
        let context = Context::concat(context, outer_context);
        let expr = self.subquery_expr(context, expr);

        FilterPlan { input, expr }
    }

    fn filter_input(
        &self,
        outer_context: Option<&Rc<Context<'a>>>,
        input: FilterInputPlan,
    ) -> (Option<Rc<Context<'a>>>, FilterInputPlan) {
        match input {
            FilterInputPlan::Source(relation) => {
                let context = self.update_context(None, &relation);
                (context, FilterInputPlan::Source(relation))
            }
            FilterInputPlan::InnerJoin(join) => {
                let (context, join) = self.inner_join(outer_context, *join);
                (context, FilterInputPlan::InnerJoin(Box::new(join)))
            }
            FilterInputPlan::LeftOuterJoin(join) => {
                let (context, join) = self.left_outer_join(outer_context, *join);
                (context, FilterInputPlan::LeftOuterJoin(Box::new(join)))
            }
        }
    }

    fn inner_join(
        &self,
        outer_context: Option<&Rc<Context<'a>>>,
        plan: InnerJoinPlan,
    ) -> (Option<Rc<Context<'a>>>, InnerJoinPlan) {
        let (context, input) = match plan.input {
            InnerJoinInputPlan::NestedLoop(plan) => {
                let (context, plan) = self.nested_loop(outer_context, plan);
                (context, InnerJoinInputPlan::NestedLoop(plan))
            }
            InnerJoinInputPlan::Hash(plan) => {
                let (context, plan) = self.hash(outer_context, plan);
                (context, InnerJoinInputPlan::Hash(plan))
            }
            InnerJoinInputPlan::Condition(condition) => {
                self.inner_condition(outer_context, condition)
            }
        };

        (context, InnerJoinPlan { input })
    }

    fn left_outer_join(
        &self,
        outer_context: Option<&Rc<Context<'a>>>,
        plan: LeftOuterJoinPlan,
    ) -> (Option<Rc<Context<'a>>>, LeftOuterJoinPlan) {
        let (context, input) = match plan.input {
            LeftOuterJoinInputPlan::NestedLoop(plan) => {
                let (context, plan) = self.nested_loop(outer_context, plan);
                (context, LeftOuterJoinInputPlan::NestedLoop(plan))
            }
            LeftOuterJoinInputPlan::Hash(plan) => {
                let (context, plan) = self.hash(outer_context, plan);
                (context, LeftOuterJoinInputPlan::Hash(plan))
            }
            LeftOuterJoinInputPlan::Condition(condition) => {
                self.left_outer_condition(outer_context, condition)
            }
        };

        (context, LeftOuterJoinPlan { input })
    }

    fn nested_loop(
        &self,
        outer_context: Option<&Rc<Context<'a>>>,
        plan: NestedLoopJoinPlan,
    ) -> (Option<Rc<Context<'a>>>, NestedLoopJoinPlan) {
        let NestedLoopJoinPlan { input, right } = plan;
        let (input_context, input) = self.nested_loop_input(outer_context, input);
        let context = self.update_context(input_context, &right);

        (context, NestedLoopJoinPlan { input, right })
    }

    fn nested_loop_input(
        &self,
        outer_context: Option<&Rc<Context<'a>>>,
        input: NestedLoopJoinInputPlan,
    ) -> (Option<Rc<Context<'a>>>, NestedLoopJoinInputPlan) {
        match input {
            NestedLoopJoinInputPlan::Source(relation) => {
                let context = self.update_context(None, &relation);
                (context, NestedLoopJoinInputPlan::Source(relation))
            }
            NestedLoopJoinInputPlan::InnerJoin(join) => {
                let (context, join) = self.inner_join(outer_context, *join);
                (context, NestedLoopJoinInputPlan::InnerJoin(Box::new(join)))
            }
            NestedLoopJoinInputPlan::LeftOuterJoin(join) => {
                let (context, join) = self.left_outer_join(outer_context, *join);
                (
                    context,
                    NestedLoopJoinInputPlan::LeftOuterJoin(Box::new(join)),
                )
            }
        }
    }

    fn hash(
        &self,
        outer_context: Option<&Rc<Context<'a>>>,
        plan: HashJoinPlan,
    ) -> (Option<Rc<Context<'a>>>, HashJoinPlan) {
        let HashJoinPlan {
            input,
            right,
            input_key,
            right_key,
            right_filter,
        } = plan;
        let (input_context, input) = self.hash_input(outer_context, input);
        let context = self.update_context(input_context, &right);
        let plan = HashJoinPlan {
            input,
            right,
            input_key,
            right_key,
            right_filter,
        };

        (context, plan)
    }

    fn hash_input(
        &self,
        outer_context: Option<&Rc<Context<'a>>>,
        input: HashJoinInputPlan,
    ) -> (Option<Rc<Context<'a>>>, HashJoinInputPlan) {
        match input {
            HashJoinInputPlan::Source(relation) => {
                let context = self.update_context(None, &relation);
                (context, HashJoinInputPlan::Source(relation))
            }
            HashJoinInputPlan::InnerJoin(join) => {
                let (context, join) = self.inner_join(outer_context, *join);
                (context, HashJoinInputPlan::InnerJoin(Box::new(join)))
            }
            HashJoinInputPlan::LeftOuterJoin(join) => {
                let (context, join) = self.left_outer_join(outer_context, *join);
                (context, HashJoinInputPlan::LeftOuterJoin(Box::new(join)))
            }
        }
    }

    fn inner_condition(
        &self,
        outer_context: Option<&Rc<Context<'a>>>,
        condition: JoinConditionPlan,
    ) -> (Option<Rc<Context<'a>>>, InnerJoinInputPlan) {
        let JoinConditionPlan { input, expr } = condition;
        match input {
            JoinConditionInputPlan::NestedLoop(nested_loop) => {
                self.plan_inner_nested_loop_condition(outer_context, nested_loop, expr)
            }
            JoinConditionInputPlan::Hash(hash) => {
                let (context, hash) = self.hash(outer_context, hash);
                let expr_context = Context::concat(
                    context.as_ref().map(Rc::clone),
                    outer_context.map(Rc::clone),
                );
                let expr = self.subquery_expr(expr_context, expr);
                let condition = JoinConditionPlan {
                    input: JoinConditionInputPlan::Hash(hash),
                    expr,
                };

                (context, InnerJoinInputPlan::Condition(condition))
            }
        }
    }

    fn plan_inner_nested_loop_condition(
        &self,
        outer_context: Option<&Rc<Context<'a>>>,
        nested_loop: NestedLoopJoinPlan,
        expr: ExprPlan,
    ) -> (Option<Rc<Context<'a>>>, InnerJoinInputPlan) {
        let NestedLoopJoinPlan { input, right } = nested_loop;
        let (input_context, input) = self.nested_loop_input(outer_context, input);
        let current_context = self.update_context(None, &right);
        let key_context = {
            Context::concat(
                current_context.as_ref().map(Rc::clone),
                outer_context.map(Rc::clone),
            )
        };
        let value_context = {
            let context = Context::concat(
                current_context.as_ref().map(Rc::clone),
                input_context.as_ref().map(Rc::clone),
            );

            Context::concat(context, outer_context.map(Rc::clone))
        };
        let original_expr = expr.clone();

        let mut candidate = None;
        let mut before_candidate = Vec::new();
        let mut after_candidate = Vec::new();

        for expr in split_conjuncts(expr) {
            if candidate.is_none() {
                match match_hash_join_candidate(key_context.as_ref(), value_context.as_ref(), expr)
                {
                    HashJoinCandidateMatch::Matched(hash_candidate) => {
                        candidate = Some(hash_candidate);
                    }
                    HashJoinCandidateMatch::Unmatched(expr) => before_candidate.push(expr),
                }
            } else {
                after_candidate.push(expr);
            }
        }

        let Some(HashJoinCandidate {
            key_expr,
            value_expr,
        }) = candidate
        else {
            let expr = merge_conjuncts(before_candidate).unwrap_or(original_expr);
            let expr = self.subquery_expr(value_context, expr);
            let context = self.update_context(input_context, &right);
            let condition = JoinConditionPlan {
                input: JoinConditionInputPlan::NestedLoop(NestedLoopJoinPlan { input, right }),
                expr,
            };

            return (context, InnerJoinInputPlan::Condition(condition));
        };

        let remaining = after_candidate
            .into_iter()
            .chain(before_candidate)
            .collect();
        let (where_clause, remainder) = merge_conjuncts(remaining).map_or((None, None), |expr| {
            find_evaluable(key_context.as_ref().map(Rc::clone), expr)
        });
        let right_key = self.subquery_expr(key_context.as_ref().map(Rc::clone), key_expr);
        let input_key = self.subquery_expr(value_context.as_ref().map(Rc::clone), value_expr);
        let right_filter =
            where_clause.map(|expr| self.subquery_expr(key_context.as_ref().map(Rc::clone), expr));
        let hash = HashJoinPlan {
            input: nested_loop_to_hash_input(input),
            right,
            input_key,
            right_key,
            right_filter,
        };
        let context = self.update_context(input_context, &hash.right);

        match remainder {
            Some(expr) => {
                let expr = self.subquery_expr(value_context, expr);
                let condition = JoinConditionPlan {
                    input: JoinConditionInputPlan::Hash(hash),
                    expr,
                };

                (context, InnerJoinInputPlan::Condition(condition))
            }
            None => (context, InnerJoinInputPlan::Hash(hash)),
        }
    }

    fn left_outer_condition(
        &self,
        outer_context: Option<&Rc<Context<'a>>>,
        condition: JoinConditionPlan,
    ) -> (Option<Rc<Context<'a>>>, LeftOuterJoinInputPlan) {
        let JoinConditionPlan { input, expr } = condition;
        match input {
            JoinConditionInputPlan::NestedLoop(nested_loop) => {
                self.plan_left_outer_nested_loop_condition(outer_context, nested_loop, expr)
            }
            JoinConditionInputPlan::Hash(hash) => {
                let (context, hash) = self.hash(outer_context, hash);
                let expr_context = Context::concat(
                    context.as_ref().map(Rc::clone),
                    outer_context.map(Rc::clone),
                );
                let expr = self.subquery_expr(expr_context, expr);
                let condition = JoinConditionPlan {
                    input: JoinConditionInputPlan::Hash(hash),
                    expr,
                };

                (context, LeftOuterJoinInputPlan::Condition(condition))
            }
        }
    }

    fn plan_left_outer_nested_loop_condition(
        &self,
        outer_context: Option<&Rc<Context<'a>>>,
        nested_loop: NestedLoopJoinPlan,
        expr: ExprPlan,
    ) -> (Option<Rc<Context<'a>>>, LeftOuterJoinInputPlan) {
        let NestedLoopJoinPlan { input, right } = nested_loop;
        let (input_context, input) = self.nested_loop_input(outer_context, input);
        let current_context = self.update_context(None, &right);
        let key_context = {
            Context::concat(
                current_context.as_ref().map(Rc::clone),
                outer_context.map(Rc::clone),
            )
        };
        let value_context = {
            let context = Context::concat(
                current_context.as_ref().map(Rc::clone),
                input_context.as_ref().map(Rc::clone),
            );

            Context::concat(context, outer_context.map(Rc::clone))
        };
        let original_expr = expr.clone();

        let mut candidate = None;
        let mut before_candidate = Vec::new();
        let mut after_candidate = Vec::new();

        for expr in split_conjuncts(expr) {
            if candidate.is_none() {
                match match_hash_join_candidate(key_context.as_ref(), value_context.as_ref(), expr)
                {
                    HashJoinCandidateMatch::Matched(hash_candidate) => {
                        candidate = Some(hash_candidate);
                    }
                    HashJoinCandidateMatch::Unmatched(expr) => before_candidate.push(expr),
                }
            } else {
                after_candidate.push(expr);
            }
        }

        let Some(HashJoinCandidate {
            key_expr,
            value_expr,
        }) = candidate
        else {
            let expr = merge_conjuncts(before_candidate).unwrap_or(original_expr);
            let expr = self.subquery_expr(value_context, expr);
            let context = self.update_context(input_context, &right);
            let condition = JoinConditionPlan {
                input: JoinConditionInputPlan::NestedLoop(NestedLoopJoinPlan { input, right }),
                expr,
            };

            return (context, LeftOuterJoinInputPlan::Condition(condition));
        };

        let remaining = after_candidate
            .into_iter()
            .chain(before_candidate)
            .collect();
        let (where_clause, remainder) = merge_conjuncts(remaining).map_or((None, None), |expr| {
            find_evaluable(key_context.as_ref().map(Rc::clone), expr)
        });
        let right_key = self.subquery_expr(key_context.as_ref().map(Rc::clone), key_expr);
        let input_key = self.subquery_expr(value_context.as_ref().map(Rc::clone), value_expr);
        let right_filter =
            where_clause.map(|expr| self.subquery_expr(key_context.as_ref().map(Rc::clone), expr));
        let hash = HashJoinPlan {
            input: nested_loop_to_hash_input(input),
            right,
            input_key,
            right_key,
            right_filter,
        };
        let context = self.update_context(input_context, &hash.right);

        match remainder {
            Some(expr) => {
                let expr = self.subquery_expr(value_context, expr);
                let condition = JoinConditionPlan {
                    input: JoinConditionInputPlan::Hash(hash),
                    expr,
                };

                (context, LeftOuterJoinInputPlan::Condition(condition))
            }
            None => (context, LeftOuterJoinInputPlan::Hash(hash)),
        }
    }
}

type EvaluableExpr = Option<ExprPlan>;
type RemainderExpr = Option<ExprPlan>;

struct HashJoinCandidate {
    key_expr: ExprPlan,
    value_expr: ExprPlan,
}

enum HashJoinCandidateMatch {
    Matched(HashJoinCandidate),
    Unmatched(ExprPlan),
}

fn split_conjuncts(expr: ExprPlan) -> Vec<ExprPlan> {
    match expr {
        ExprPlan::BinaryOp {
            left,
            op: BinaryOperator::And,
            right,
        } => split_conjuncts(*left)
            .into_iter()
            .chain(split_conjuncts(*right))
            .collect(),
        ExprPlan::Nested(expr) => split_conjuncts(*expr),
        expr => vec![expr],
    }
}

fn match_hash_join_candidate(
    key_context: Option<&Rc<Context<'_>>>,
    value_context: Option<&Rc<Context<'_>>>,
    expr: ExprPlan,
) -> HashJoinCandidateMatch {
    let ExprPlan::BinaryOp { left, op, right } = expr else {
        return HashJoinCandidateMatch::Unmatched(expr);
    };

    if op != BinaryOperator::Eq {
        return HashJoinCandidateMatch::Unmatched(ExprPlan::BinaryOp { left, op, right });
    }

    let left_as_key = check_evaluable(key_context.map(Rc::clone), &left);
    let right_as_value = check_evaluable(value_context.map(Rc::clone), &right);

    if left_as_key && right_as_value {
        return HashJoinCandidateMatch::Matched(HashJoinCandidate {
            key_expr: *left,
            value_expr: *right,
        });
    }

    let right_as_key = check_evaluable(key_context.map(Rc::clone), &right);
    let left_as_value = left_as_key || check_evaluable(value_context.map(Rc::clone), &left);

    if right_as_key && left_as_value {
        return HashJoinCandidateMatch::Matched(HashJoinCandidate {
            key_expr: *right,
            value_expr: *left,
        });
    }

    HashJoinCandidateMatch::Unmatched(ExprPlan::BinaryOp { left, op, right })
}

fn merge_conjuncts(exprs: Vec<ExprPlan>) -> Option<ExprPlan> {
    exprs.into_iter().reduce(|left, right| ExprPlan::BinaryOp {
        left: Box::new(left),
        op: BinaryOperator::And,
        right: Box::new(right),
    })
}

fn find_evaluable(
    context: Option<Rc<Context<'_>>>,
    expr: ExprPlan,
) -> (EvaluableExpr, RemainderExpr) {
    match expr {
        ExprPlan::BinaryOp {
            left,
            op: BinaryOperator::And,
            right,
        } => {
            let (evaluable, remainder) = find_evaluable(context.as_ref().map(Rc::clone), *left);
            let (evaluable2, remainder2) = find_evaluable(context, *right);

            let merge = |expr, expr2| match (expr, expr2) {
                (Some(expr), Some(expr2)) => Some(ExprPlan::BinaryOp {
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

fn nested_loop_to_hash_input(input: NestedLoopJoinInputPlan) -> HashJoinInputPlan {
    match input {
        NestedLoopJoinInputPlan::Source(source) => HashJoinInputPlan::Source(source),
        NestedLoopJoinInputPlan::InnerJoin(join) => HashJoinInputPlan::InnerJoin(join),
        NestedLoopJoinInputPlan::LeftOuterJoin(join) => HashJoinInputPlan::LeftOuterJoin(join),
    }
}

#[cfg(test)]
mod tests {
    use {
        super::plan,
        crate::{
            ast::DateTimeField,
            mock::{MockStorage, run},
            parse_sql::parse,
            plan::StatementPlan,
            planner::fetch_schema_map,
            query_builder::{Build, QueryNode, col, exists, num, subquery, table},
            translate::translate,
        },
    };

    fn plan_join(storage: &MockStorage, sql: &str) -> StatementPlan {
        let parsed = parse(sql).expect(sql).into_iter().next().unwrap();
        let statement = StatementPlan::from(translate(&parsed).unwrap());
        let schema_map = fetch_schema_map(storage, &statement).unwrap();

        plan(&schema_map, statement)
    }

    fn plan_builder(storage: &MockStorage, builder: impl Build) -> StatementPlan {
        let statement = builder.build().unwrap();
        let schema_map = fetch_schema_map(storage, &statement).unwrap();

        plan(&schema_map, statement)
    }

    macro_rules! test {
        ($actual: expr, $expected: expr, $name: literal) => {
            let expected = $expected.build().unwrap();

            assert_eq!($actual, expected, $name);
        };
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
            CREATE TABLE Item (
                id INTEGER,
                name TEXT
            );
        ");

        let sql = "SELECT * FROM Player;";
        let actual = plan_join(&storage, sql);
        let expected = table("Player").select();
        test!(actual, expected, "basic select:\n{sql}");

        let sql = "SELECT * FROM Player ORDER BY id OFFSET 1";
        let actual = plan_join(&storage, sql);
        let expected = table("Player").select().order_by("id").offset(1);
        test!(actual, expected, "preserves order by before offset:\n{sql}");

        let sql = "DELETE FROM Player WHERE id = 1;";
        let actual = plan_join(&storage, sql);
        let expected = table("Player").delete().filter("id = 1");
        test!(actual, expected, "plan not covered:\n{sql}");

        let sql = "
            SELECT *
            FROM Player
            JOIN PlayerItem ON PlayerItem.user_id != Player.id
        ";
        let actual = plan_join(&storage, sql);
        let expected = table("Player")
            .select()
            .join("PlayerItem")
            .on("PlayerItem.user_id != Player.id");
        test!(actual, expected, "basic nested loop join:\n{sql}");

        let sql = "
            SELECT *
            FROM Player
            LEFT JOIN PlayerItem ON PlayerItem.amount > 2
        ";
        let actual = plan_join(&storage, sql);
        let expected = table("Player")
            .select()
            .left_join("PlayerItem")
            .on("PlayerItem.amount > 2");
        test!(actual, expected, "basic nested loop join 2:\n{sql}");

        let sql = "
            SELECT *
            FROM Player
            JOIN Empty u2
            LEFT JOIN Player u3;
        ";
        let actual = plan_join(&storage, sql);
        let expected = table("Player")
            .select()
            .join_as("Empty", "u2")
            .left_join_as("Player", "u3");
        test!(actual, expected, "self multiple joins:\n{sql}");

        let sql = "
            SELECT *
            FROM Player
            JOIN PlayerItem ON PlayerItem.user_id = Player.id
        ";
        let actual = plan_join(&storage, sql);
        let expected = table("Player")
            .select()
            .join("PlayerItem")
            .hash_executor("PlayerItem.user_id", "Player.id");
        test!(actual, expected, "basic hash join query:\n{sql}");

        let sql = "
            SELECT *
            FROM Player
            JOIN PlayerItem ON PlayerItem.user_id = Player.id
        ";
        let actual = plan_join(&storage, sql);
        let expected = table("Player")
            .select()
            .join("PlayerItem")
            .hash_executor("PlayerItem.user_id", "Player.id");
        test!(
            actual,
            expected,
            "redundant plan does not change the plan result:\n{sql}"
        );

        let sql = "
            SELECT *
            FROM Player
            JOIN PlayerItem ON PlayerItem.user_id = Player.id
            JOIN Item ON Item.id = PlayerItem.item_id
        ";
        let actual = plan_join(&storage, sql);
        let expected = table("Player")
            .select()
            .join("PlayerItem")
            .hash_executor("PlayerItem.user_id", "Player.id")
            .join("Item")
            .hash_executor("Item.id", "PlayerItem.item_id");
        test!(
            actual,
            expected,
            "later join uses accumulated left context:\n{sql}"
        );

        let sql = "
            SELECT Player.id
            FROM Player
            JOIN PlayerItem ON PlayerItem.user_id = Player.id
            GROUP BY Player.id
        ";
        let actual = plan_join(&storage, sql);
        let expected = table("Player")
            .select()
            .join("PlayerItem")
            .hash_executor("PlayerItem.user_id", "Player.id")
            .group_by("Player.id")
            .project("Player.id");
        test!(actual, expected, "preserves aggregation wrapper:\n{sql}");

        let sql = "
            SELECT Player.id
            FROM Player
            JOIN PlayerItem ON PlayerItem.user_id = Player.id
            GROUP BY Player.id
            HAVING TRUE
        ";
        let actual = plan_join(&storage, sql);
        let expected = table("Player")
            .select()
            .join("PlayerItem")
            .hash_executor("PlayerItem.user_id", "Player.id")
            .group_by("Player.id")
            .having("TRUE")
            .project("Player.id");
        test!(actual, expected, "preserves having wrapper:\n{sql}");

        let statement = actual;
        let schema_map = fetch_schema_map(&storage, &statement).unwrap();
        let actual = plan(&schema_map, statement.clone());
        assert_eq!(actual, statement, "planned hash join remains unchanged");

        let sql = "
            SELECT * FROM Player
            JOIN PlayerItem ON (SELECT * FROM Player u2)
        ";
        let actual = plan_join(&storage, sql);
        let expected = table("Player")
            .select()
            .join("PlayerItem")
            .on("(SELECT * FROM Player u2)");
        test!(actual, expected, "subquery in join_constraint:\n{sql}");
    }

    #[test]
    fn explicit_hash_plans_are_preserved() {
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
            CREATE TABLE Item (
                id INTEGER,
                name TEXT
            );
        ");
        let actual = plan_builder(
            &storage,
            table("Player")
                .select()
                .left_join("PlayerItem")
                .hash_executor("PlayerItem.user_id", "Player.id"),
        );
        let expected = table("Player")
            .select()
            .left_join("PlayerItem")
            .hash_executor("PlayerItem.user_id", "Player.id");
        test!(actual, expected, "left outer hash");

        let actual = plan_builder(
            &storage,
            table("Player")
                .select()
                .join("PlayerItem")
                .hash_executor("PlayerItem.user_id", "Player.id")
                .on("Player.name IS NOT NULL"),
        );
        let expected = table("Player")
            .select()
            .join("PlayerItem")
            .hash_executor("PlayerItem.user_id", "Player.id")
            .on("Player.name IS NOT NULL");
        test!(actual, expected, "inner hash with condition");

        let actual = plan_builder(
            &storage,
            table("Player")
                .select()
                .left_join("PlayerItem")
                .hash_executor("PlayerItem.user_id", "Player.id")
                .on("Player.name IS NOT NULL"),
        );
        let expected = table("Player")
            .select()
            .left_join("PlayerItem")
            .hash_executor("PlayerItem.user_id", "Player.id")
            .on("Player.name IS NOT NULL");
        test!(actual, expected, "left outer hash with condition");

        let actual = plan_builder(
            &storage,
            table("Player")
                .select()
                .join("PlayerItem")
                .hash_executor("PlayerItem.user_id", "Player.id")
                .join("Item")
                .hash_executor("Item.id", "PlayerItem.item_id"),
        );
        let expected = table("Player")
            .select()
            .join("PlayerItem")
            .hash_executor("PlayerItem.user_id", "Player.id")
            .join("Item")
            .hash_executor("Item.id", "PlayerItem.item_id");
        test!(actual, expected, "inner hash feeds another hash");

        let actual = plan_builder(
            &storage,
            table("Player")
                .select()
                .left_join("PlayerItem")
                .hash_executor("PlayerItem.user_id", "Player.id")
                .join("Item")
                .hash_executor("Item.id", "PlayerItem.item_id"),
        );
        let expected = table("Player")
            .select()
            .left_join("PlayerItem")
            .hash_executor("PlayerItem.user_id", "Player.id")
            .join("Item")
            .hash_executor("Item.id", "PlayerItem.item_id");
        test!(actual, expected, "left outer hash feeds another hash");
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
        let expected = table("Player")
            .select()
            .left_join("PlayerItem")
            .hash_executor("PlayerItem.user_id", "Player.id")
            .hash_filter("PlayerItem.amount > 10")
            .filter(true);
        test!(actual, expected, "where_clause AND hash_join expr:\n{sql}");

        let sql = "
            SELECT *
            FROM Player
            JOIN PlayerItem ON
                (PlayerItem.user_id = Player.id) AND
                Player.name = 'abcd' AND
                Player.name != 'barcode'
        ";
        let actual = plan_join(&storage, sql);
        let expected = table("Player")
            .select()
            .join("PlayerItem")
            .hash_executor("PlayerItem.user_id", "Player.id")
            .on("Player.name = 'abcd' AND Player.name != 'barcode'");
        test!(
            actual,
            expected,
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
        let expected = table("Player")
            .select()
            .left_join("PlayerItem")
            .hash_executor("PlayerItem.user_id", "Player.id")
            .hash_filter("PlayerItem.amount > 10 AND PlayerItem.amount * 3 <= 2")
            .filter(true);
        test!(actual, expected, "complex where_clause:\n{sql}");

        let sql = "
            SELECT *
            FROM Player
            JOIN PlayerItem ON
                Player.id = PlayerItem.user_id AND
                PlayerItem.amount > 10
            WHERE True;
        ";
        let actual = plan_join(&storage, sql);
        let expected = table("Player")
            .select()
            .join("PlayerItem")
            .hash_executor("PlayerItem.user_id", "Player.id")
            .hash_filter("PlayerItem.amount > 10")
            .filter(true);
        test!(actual, expected, "hash_join expr AND where_clause:\n{sql}");

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
        let expected = table("Player")
            .alias_as("u1")
            .select()
            .left_join_as("Player", "u2")
            .filter(
                col("u2.id").eq(subquery(
                    table("Player")
                        .alias_as("u3")
                        .select()
                        .join_as("Player", "u4")
                        .hash_executor("u4.id", "u3.id")
                        .hash_filter("u4.id = u1.id")
                        .project("u3.id"),
                )),
            );
        test!(actual, expected, "hash join in subquery:\n{sql}");

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
        let expected = table("Player").alias_as("u1").select().filter(
            col("u1.id").eq(subquery(
                table("Player").alias_as("u2").select().filter(
                    col("u2.id").eq(subquery(
                        table("Player")
                            .alias_as("u3")
                            .select()
                            .join_as("Player", "u4")
                            .hash_executor(col("u4.id"), col("u3.id").add("u1.id")),
                    )),
                ),
            )),
        );
        test!(actual, expected, "hash join in nested subquery:\n{sql}");

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
        let expected = table("Player")
            .select()
            .join("PlayerItem")
            .hash_executor("PlayerItem.user_id", "Player.id")
            .hash_filter("PlayerItem.item_id IS NOT NULL")
            .hash_filter("PlayerItem.amount > 10")
            .on("Player.id > 10")
            .filter(true);
        test!(
            actual,
            expected,
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
        let expected = table("Player")
            .select()
            .join("PlayerItem")
            .hash_executor("PlayerItem.user_id", "Player.id")
            .hash_filter("PlayerItem.item_id IS NOT NULL")
            .hash_filter("PlayerItem.amount > 10")
            .on("Player.id > Player.id + PlayerItem.user_id")
            .filter(true);
        test!(
            actual,
            expected,
            "hash join with join_constraint AND where_clause 2:\n{sql}"
        );

        let sql = "
            SELECT *
            FROM Player
            JOIN PlayerItem ON
                PlayerItem.amount > 10 AND
                (
                    Player.id = PlayerItem.user_id AND
                    PlayerItem.item_id IS NOT NULL
                )
            WHERE True;
        ";
        let actual = plan_join(&storage, sql);
        let expected = table("Player")
            .select()
            .join("PlayerItem")
            .hash_executor("PlayerItem.user_id", "Player.id")
            .hash_filter("PlayerItem.item_id IS NOT NULL")
            .hash_filter("PlayerItem.amount > 10")
            .filter(true);
        test!(
            actual,
            expected,
            "hash join merges existing where_clause with current-table filter:\n{sql}"
        );

        let sql = "
            SELECT *
            FROM Player
            JOIN PlayerItem ON
                (SELECT * FROM Player JOIN PlayerItem ON Player.id = PlayerItem.user_id)
        ";
        let actual = plan_join(&storage, sql);
        let expected = table("Player").select().join("PlayerItem").on(subquery(
            table("Player")
                .select()
                .join("PlayerItem")
                .hash_executor("Player.id", "PlayerItem.user_id"),
        ));
        test!(
            actual,
            expected,
            "hash join with join_constraint subquery:\n{sql}"
        );

        let sql = "
            SELECT *
            FROM Player
            JOIN PlayerItem ON
                1 IN (SELECT * FROM PlayerItem JOIN Player ON PlayerItem.user_id = Player.id)
            WHERE True
        ";
        let actual = plan_join(&storage, sql);
        let expected = table("Player")
            .select()
            .join("PlayerItem")
            .on(num(1).in_list(
                table("PlayerItem")
                    .select()
                    .join("Player")
                    .hash_executor("PlayerItem.user_id", "Player.id"),
            ))
            .filter(true);
        test!(
            actual,
            expected,
            "hash join with join constraint in subquery"
        );

        let sql = "
            SELECT *
            FROM Player
            JOIN PlayerItem ON
                EXISTS (SELECT * FROM PlayerItem JOIN Player ON PlayerItem.user_id = Player.id WHERE Player.id > 3)
            WHERE True
        ";
        let actual = plan_join(&storage, sql);
        let expected = table("Player")
            .select()
            .join("PlayerItem")
            .on(exists(
                table("PlayerItem")
                    .select()
                    .join("Player")
                    .hash_executor("PlayerItem.user_id", "Player.id")
                    .filter("Player.id > 3"),
            ))
            .filter(true);
        test!(
            actual,
            expected,
            "hash join with join constraint in subquery"
        );
    }

    #[test]
    fn hash_join_boundaries() {
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
            CREATE TABLE Item (
                id INTEGER,
                name TEXT
            );
        ");

        let sql = "
            SELECT *
            FROM Player
            LEFT JOIN PlayerItem ON
                PlayerItem.user_id = Player.id AND
                PlayerItem.amount > 10
        ";
        let actual = plan_join(&storage, sql);
        let expected = table("Player")
            .select()
            .left_join("PlayerItem")
            .hash_executor("PlayerItem.user_id", "Player.id")
            .hash_filter("PlayerItem.amount > 10");
        test!(
            actual,
            expected,
            "left outer hash join extracts a following right filter:\n{sql}"
        );

        let sql = "
            SELECT *
            FROM Player
            LEFT JOIN PlayerItem ON
                PlayerItem.user_id = Player.id AND
                Player.name = 'Alice'
        ";
        let actual = plan_join(&storage, sql);
        let expected = table("Player")
            .select()
            .left_join("PlayerItem")
            .hash_executor("PlayerItem.user_id", "Player.id")
            .on("Player.name = 'Alice'");
        test!(
            actual,
            expected,
            "left outer hash join preserves a residual condition:\n{sql}"
        );

        let sql = "
            SELECT *
            FROM Player
            JOIN PlayerItem ON Player.id = Player.id
        ";
        let actual = plan_join(&storage, sql);
        let expected = table("Player")
            .select()
            .join("PlayerItem")
            .on("Player.id = Player.id");
        test!(
            actual,
            expected,
            "same-side equality remains a nested loop condition:\n{sql}"
        );

        let sql = "
            SELECT *
            FROM Player
            LEFT JOIN PlayerItem ON PlayerItem.user_id = Player.id
            JOIN Item ON Item.id = PlayerItem.item_id
        ";
        let actual = plan_join(&storage, sql);
        let expected = table("Player")
            .select()
            .left_join("PlayerItem")
            .hash_executor("PlayerItem.user_id", "Player.id")
            .join("Item")
            .hash_executor("Item.id", "PlayerItem.item_id");
        test!(
            actual,
            expected,
            "left outer hash join feeds a later planned hash join:\n{sql}"
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
        let subquery_node = || -> QueryNode {
            table("Player")
                .alias_as("u")
                .select()
                .join_as("Flag", "f")
                .hash_executor("f.user_id", "u.id")
                .project("u.id")
                .into()
        };

        let sql = format!("SELECT * FROM Player WHERE id = ({subquery_sql})");
        let actual = plan_join(&storage, &sql);
        let expected = table("Player")
            .select()
            .filter(col("id").eq(subquery_node()));
        test!(actual, expected, "binary operator:\n{sql}");

        let sql = format!("SELECT * FROM Player WHERE -({subquery_sql}) IN ({subquery_sql})");
        let actual = plan_join(&storage, &sql);
        let expected = table("Player")
            .select()
            .filter(subquery(subquery_node()).minus().in_list(subquery_node()));
        test!(actual, expected, "unary operator and in subquery:\n{sql}");

        let sql = format!(
            "
            SELECT * FROM Player
            WHERE
                CAST(({subquery_sql}) AS INTEGER) IN (1, 2, 3)
        "
        );
        let actual = plan_join(&storage, &sql);
        let expected = table("Player")
            .select()
            .filter(subquery(subquery_node()).cast("INTEGER").in_list("1, 2, 3"));
        test!(actual, expected, "cast and in list:\n{sql}");

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
        let expected = table("Player").select().filter(
            subquery(subquery_node())
                .is_null()
                .or(subquery(subquery_node()).is_not_null()),
        );
        test!(actual, expected, "is null and is not null:\n{sql}");

        let sql = format!("SELECT * FROM Player WHERE EXISTS({subquery_sql})");
        let actual = plan_join(&storage, &sql);
        let expected = table("Player").select().filter(exists(subquery_node()));
        test!(actual, expected, "exists:\n{sql}");

        let sql = format!(
            "
            SELECT * FROM Player
            WHERE ({subquery_sql}) BETWEEN ({subquery_sql}) AND 100;
        "
        );
        let actual = plan_join(&storage, &sql);
        let expected = table("Player")
            .select()
            .filter(subquery(subquery_node()).between(subquery_node(), num(100)));
        test!(actual, expected, "between:\n{sql}");

        let sql = format!(
            "
            SELECT * FROM Player
            WHERE EXTRACT(HOUR FROM (({subquery_sql}))) IS NULL
        "
        );
        let actual = plan_join(&storage, &sql);
        let expected = table("Player").select().filter(
            subquery(subquery_node())
                .nested()
                .extract(DateTimeField::Hour)
                .is_null(),
        );
        test!(actual, expected, "extract and nested:\n{sql}");

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
        let expected = table("Player").select().filter(
            subquery(subquery_node())
                .case()
                .when_then(10, true)
                .when_then(20, subquery(subquery_node()).is_null())
                .or_else("col3"),
        );
        test!(actual, expected, "case expr:\n{sql}");
    }
}
