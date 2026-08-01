use {
    super::{
        context::Context,
        expr::{deterministic::is_deterministic, nullability::may_return_null},
        query::Planner,
    },
    crate::{
        ast::{BinaryOperator, IndexOperator},
        data::{Schema, SchemaIndex, SchemaIndexOrd, Value},
        plan::{
            AggregationInputPlan, DistinctInputPlan, DistinctPlan, ExprPlan, FilterInputPlan,
            FilterPlan, HashJoinInputPlan, HashJoinPlan, IndexPredicatePlan, InnerJoinInputPlan,
            InnerJoinPlan, JoinConditionInputPlan, JoinConditionPlan, LeftOuterJoinInputPlan,
            LeftOuterJoinPlan, LimitInputPlan, LimitPlan, NestedLoopJoinInputPlan,
            NestedLoopJoinPlan, OffsetInputPlan, OffsetPlan, OrderByExprPlan, ProjectInputPlan,
            ProjectPlan, QueryPlan, SelectOrderByPlan, SourcePlan, StatementPlan, TableAccessPlan,
            plan_scalar_expr,
        },
    },
    std::{collections::HashMap, hash::BuildHasher, rc::Rc},
};

pub fn plan<S: BuildHasher>(
    schema_map: &HashMap<String, Schema, S>,
    statement: StatementPlan,
) -> StatementPlan {
    let planner = IndexPlanner { schema_map };

    match statement {
        StatementPlan::Query(query) => {
            let query = planner.query(None, query);

            StatementPlan::Query(query)
        }
        _ => statement,
    }
}

struct IndexPlanner<'a, S> {
    schema_map: &'a HashMap<String, Schema, S>,
}

impl<'a, S: BuildHasher> Planner<'a> for IndexPlanner<'a, S> {
    fn query(&self, outer_context: Option<Rc<Context<'a>>>, query: QueryPlan) -> QueryPlan {
        match query {
            QueryPlan::Project(input) => {
                QueryPlan::Project(self.project(outer_context.as_ref(), input, Vec::new()).0)
            }
            QueryPlan::Values(values) => QueryPlan::Values(values),
            QueryPlan::SelectOrderBy(SelectOrderByPlan { input, exprs }) => {
                let (input, exprs) = self.project(outer_context.as_ref(), input, exprs);
                if exprs.is_empty() {
                    QueryPlan::Project(input)
                } else {
                    QueryPlan::SelectOrderBy(SelectOrderByPlan { input, exprs })
                }
            }
            QueryPlan::ValuesOrderBy(values) => QueryPlan::ValuesOrderBy(values),
            QueryPlan::Distinct(distinct) => {
                QueryPlan::Distinct(self.distinct(outer_context.as_ref(), distinct))
            }
            QueryPlan::Offset(OffsetPlan { input, count }) => {
                let input = match input {
                    OffsetInputPlan::Project(input) => OffsetInputPlan::Project(
                        self.project(outer_context.as_ref(), input, Vec::new()).0,
                    ),
                    OffsetInputPlan::Values(values) => OffsetInputPlan::Values(values),
                    OffsetInputPlan::SelectOrderBy(SelectOrderByPlan { input, exprs }) => {
                        let (input, exprs) = self.project(outer_context.as_ref(), input, exprs);
                        if exprs.is_empty() {
                            OffsetInputPlan::Project(input)
                        } else {
                            OffsetInputPlan::SelectOrderBy(SelectOrderByPlan { input, exprs })
                        }
                    }
                    OffsetInputPlan::ValuesOrderBy(values) => {
                        OffsetInputPlan::ValuesOrderBy(values)
                    }
                    OffsetInputPlan::Distinct(distinct) => {
                        OffsetInputPlan::Distinct(self.distinct(outer_context.as_ref(), distinct))
                    }
                };

                QueryPlan::Offset(OffsetPlan { input, count })
            }
            QueryPlan::Limit(LimitPlan { input, count }) => {
                let input = match input {
                    LimitInputPlan::Project(input) => LimitInputPlan::Project(
                        self.project(outer_context.as_ref(), input, Vec::new()).0,
                    ),
                    LimitInputPlan::Values(values) => LimitInputPlan::Values(values),
                    LimitInputPlan::SelectOrderBy(SelectOrderByPlan { input, exprs }) => {
                        let (input, exprs) = self.project(outer_context.as_ref(), input, exprs);
                        if exprs.is_empty() {
                            LimitInputPlan::Project(input)
                        } else {
                            LimitInputPlan::SelectOrderBy(SelectOrderByPlan { input, exprs })
                        }
                    }
                    LimitInputPlan::ValuesOrderBy(values) => LimitInputPlan::ValuesOrderBy(values),
                    LimitInputPlan::Distinct(distinct) => {
                        LimitInputPlan::Distinct(self.distinct(outer_context.as_ref(), distinct))
                    }
                    LimitInputPlan::Offset(OffsetPlan { input, count }) => {
                        let input = match input {
                            OffsetInputPlan::Project(input) => OffsetInputPlan::Project(
                                self.project(outer_context.as_ref(), input, Vec::new()).0,
                            ),
                            OffsetInputPlan::Values(values) => OffsetInputPlan::Values(values),
                            OffsetInputPlan::SelectOrderBy(SelectOrderByPlan { input, exprs }) => {
                                let (input, exprs) =
                                    self.project(outer_context.as_ref(), input, exprs);
                                if exprs.is_empty() {
                                    OffsetInputPlan::Project(input)
                                } else {
                                    OffsetInputPlan::SelectOrderBy(SelectOrderByPlan {
                                        input,
                                        exprs,
                                    })
                                }
                            }
                            OffsetInputPlan::ValuesOrderBy(values) => {
                                OffsetInputPlan::ValuesOrderBy(values)
                            }
                            OffsetInputPlan::Distinct(distinct) => OffsetInputPlan::Distinct(
                                self.distinct(outer_context.as_ref(), distinct),
                            ),
                        };
                        LimitInputPlan::Offset(OffsetPlan { input, count })
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

impl<'a, S: BuildHasher> IndexPlanner<'a, S> {
    fn project(
        &self,
        outer_context: Option<&Rc<Context<'a>>>,
        mut project: ProjectPlan,
        order_by: Vec<OrderByExprPlan>,
    ) -> (ProjectPlan, Vec<OrderByExprPlan>) {
        let (input, order_by) = match project.input {
            ProjectInputPlan::Source(relation) => {
                let source = FilterInputPlan::Source(relation);
                let (source, _, order_by) = self.source(outer_context, source, None, order_by);
                let input = match source {
                    FilterInputPlan::Source(relation) => ProjectInputPlan::Source(relation),
                    FilterInputPlan::InnerJoin(join) => ProjectInputPlan::InnerJoin(join),
                    FilterInputPlan::LeftOuterJoin(join) => ProjectInputPlan::LeftOuterJoin(join),
                };
                (input, order_by)
            }
            ProjectInputPlan::InnerJoin(join) => {
                let source = FilterInputPlan::InnerJoin(join);
                let (source, _, order_by) = self.source(outer_context, source, None, order_by);
                let input = match source {
                    FilterInputPlan::Source(relation) => ProjectInputPlan::Source(relation),
                    FilterInputPlan::InnerJoin(join) => ProjectInputPlan::InnerJoin(join),
                    FilterInputPlan::LeftOuterJoin(join) => ProjectInputPlan::LeftOuterJoin(join),
                };
                (input, order_by)
            }
            ProjectInputPlan::LeftOuterJoin(join) => {
                let source = FilterInputPlan::LeftOuterJoin(join);
                let (source, _, order_by) = self.source(outer_context, source, None, order_by);
                let input = match source {
                    FilterInputPlan::Source(relation) => ProjectInputPlan::Source(relation),
                    FilterInputPlan::InnerJoin(join) => ProjectInputPlan::InnerJoin(join),
                    FilterInputPlan::LeftOuterJoin(join) => ProjectInputPlan::LeftOuterJoin(join),
                };
                (input, order_by)
            }
            ProjectInputPlan::Filter(FilterPlan {
                input: source,
                expr,
            }) => {
                let (source, expr, order_by) =
                    self.source(outer_context, source, Some(expr), order_by);
                let input = match expr {
                    Some(expr) => ProjectInputPlan::Filter(FilterPlan {
                        input: source,
                        expr,
                    }),
                    None => match source {
                        FilterInputPlan::Source(relation) => ProjectInputPlan::Source(relation),
                        FilterInputPlan::InnerJoin(join) => ProjectInputPlan::InnerJoin(join),
                        FilterInputPlan::LeftOuterJoin(join) => {
                            ProjectInputPlan::LeftOuterJoin(join)
                        }
                    },
                };

                (input, order_by)
            }
            ProjectInputPlan::Aggregation(mut aggregation) => {
                let (input, order_by) =
                    self.aggregation_input(outer_context, aggregation.input, order_by);
                aggregation.input = input;
                (ProjectInputPlan::Aggregation(aggregation), order_by)
            }
            ProjectInputPlan::Having(mut having) => {
                let (input, order_by) =
                    self.aggregation_input(outer_context, having.input.input, order_by);
                having.input.input = input;
                (ProjectInputPlan::Having(having), order_by)
            }
        };
        project.input = input;

        (project, order_by)
    }

    fn distinct(
        &self,
        outer_context: Option<&Rc<Context<'a>>>,
        DistinctPlan { input }: DistinctPlan,
    ) -> DistinctPlan {
        let input = match input {
            DistinctInputPlan::Project(input) => {
                DistinctInputPlan::Project(self.project(outer_context, input, Vec::new()).0)
            }
            DistinctInputPlan::SelectOrderBy(SelectOrderByPlan { input, exprs }) => {
                let (input, exprs) = self.project(outer_context, input, exprs);
                if exprs.is_empty() {
                    DistinctInputPlan::Project(input)
                } else {
                    DistinctInputPlan::SelectOrderBy(SelectOrderByPlan { input, exprs })
                }
            }
        };

        DistinctPlan { input }
    }

    fn aggregation_input(
        &self,
        outer_context: Option<&Rc<Context<'a>>>,
        input: AggregationInputPlan,
        order_by: Vec<OrderByExprPlan>,
    ) -> (AggregationInputPlan, Vec<OrderByExprPlan>) {
        match input {
            AggregationInputPlan::Source(relation) => {
                let source = FilterInputPlan::Source(relation);
                let (source, _, order_by) = self.source(outer_context, source, None, order_by);
                let input = match source {
                    FilterInputPlan::Source(relation) => AggregationInputPlan::Source(relation),
                    FilterInputPlan::InnerJoin(join) => AggregationInputPlan::InnerJoin(join),
                    FilterInputPlan::LeftOuterJoin(join) => {
                        AggregationInputPlan::LeftOuterJoin(join)
                    }
                };
                (input, order_by)
            }
            AggregationInputPlan::InnerJoin(join) => {
                let source = FilterInputPlan::InnerJoin(join);
                let (source, _, order_by) = self.source(outer_context, source, None, order_by);
                let input = match source {
                    FilterInputPlan::Source(relation) => AggregationInputPlan::Source(relation),
                    FilterInputPlan::InnerJoin(join) => AggregationInputPlan::InnerJoin(join),
                    FilterInputPlan::LeftOuterJoin(join) => {
                        AggregationInputPlan::LeftOuterJoin(join)
                    }
                };
                (input, order_by)
            }
            AggregationInputPlan::LeftOuterJoin(join) => {
                let source = FilterInputPlan::LeftOuterJoin(join);
                let (source, _, order_by) = self.source(outer_context, source, None, order_by);
                let input = match source {
                    FilterInputPlan::Source(relation) => AggregationInputPlan::Source(relation),
                    FilterInputPlan::InnerJoin(join) => AggregationInputPlan::InnerJoin(join),
                    FilterInputPlan::LeftOuterJoin(join) => {
                        AggregationInputPlan::LeftOuterJoin(join)
                    }
                };
                (input, order_by)
            }
            AggregationInputPlan::Filter(FilterPlan {
                input: source,
                expr,
            }) => {
                let (source, expr, order_by) =
                    self.source(outer_context, source, Some(expr), order_by);
                let input = match expr {
                    Some(expr) => AggregationInputPlan::Filter(FilterPlan {
                        input: source,
                        expr,
                    }),
                    None => match source {
                        FilterInputPlan::Source(relation) => AggregationInputPlan::Source(relation),
                        FilterInputPlan::InnerJoin(join) => AggregationInputPlan::InnerJoin(join),
                        FilterInputPlan::LeftOuterJoin(join) => {
                            AggregationInputPlan::LeftOuterJoin(join)
                        }
                    },
                };

                (input, order_by)
            }
        }
    }

    fn source(
        &self,
        outer_context: Option<&Rc<Context<'a>>>,
        mut input: FilterInputPlan,
        filter_expr: Option<ExprPlan>,
        mut order_by: Vec<OrderByExprPlan>,
    ) -> (FilterInputPlan, Option<ExprPlan>, Vec<OrderByExprPlan>) {
        let indexes = self.indexes(Self::base_source(&input));

        if let (Some(indexes), Some(order_expr)) = (indexes.as_ref(), order_by.last())
            && let SourcePlan::Table(table) = Self::base_source_mut(&mut input)
            && table.access == TableAccessPlan::FullScan
            && let Some(index_name) = indexes.find_ordered(order_expr)
        {
            table.access = TableAccessPlan::Index {
                name: index_name,
                asc: order_expr.asc,
                predicate: None,
            };
            order_by.pop();

            return (input, filter_expr, order_by);
        }

        let filter_expr = filter_expr.and_then(|expr| {
            if let (Some(indexes), SourcePlan::Table(table)) =
                (indexes.as_ref(), Self::base_source(&input))
                && table.access == TableAccessPlan::FullScan
            {
                match self.plan_index_expr(outer_context.map(Rc::clone), indexes, expr) {
                    Planned::IndexedExpr {
                        index_name,
                        index_op,
                        index_value_expr,
                        residual,
                    } => {
                        if let SourcePlan::Table(table) = Self::base_source_mut(&mut input) {
                            table.access = TableAccessPlan::Index {
                                name: index_name,
                                asc: None,
                                predicate: Some(IndexPredicatePlan {
                                    operator: index_op,
                                    expr: index_value_expr,
                                }),
                            };
                        }

                        residual
                    }
                    Planned::Expr(expr) => Some(expr),
                }
            } else {
                Some(self.subquery_expr(outer_context.map(Rc::clone), expr))
            }
        });

        (input, filter_expr, order_by)
    }

    fn base_source(input: &FilterInputPlan) -> &SourcePlan {
        match input {
            FilterInputPlan::Source(relation) => relation,
            FilterInputPlan::InnerJoin(join) => Self::inner_join_base_source(join),
            FilterInputPlan::LeftOuterJoin(join) => Self::left_outer_join_base_source(join),
        }
    }

    fn inner_join_base_source(join: &InnerJoinPlan) -> &SourcePlan {
        match &join.input {
            InnerJoinInputPlan::NestedLoop(join) => Self::nested_loop_base_source(join),
            InnerJoinInputPlan::Hash(join) => Self::hash_base_source(join),
            InnerJoinInputPlan::Condition(condition) => Self::condition_base_source(condition),
        }
    }

    fn left_outer_join_base_source(join: &LeftOuterJoinPlan) -> &SourcePlan {
        match &join.input {
            LeftOuterJoinInputPlan::NestedLoop(join) => Self::nested_loop_base_source(join),
            LeftOuterJoinInputPlan::Hash(join) => Self::hash_base_source(join),
            LeftOuterJoinInputPlan::Condition(condition) => Self::condition_base_source(condition),
        }
    }

    fn condition_base_source(condition: &JoinConditionPlan) -> &SourcePlan {
        match &condition.input {
            JoinConditionInputPlan::NestedLoop(join) => Self::nested_loop_base_source(join),
            JoinConditionInputPlan::Hash(join) => Self::hash_base_source(join),
        }
    }

    fn nested_loop_base_source(join: &NestedLoopJoinPlan) -> &SourcePlan {
        match &join.input {
            NestedLoopJoinInputPlan::Source(source) => source,
            NestedLoopJoinInputPlan::InnerJoin(join) => Self::inner_join_base_source(join),
            NestedLoopJoinInputPlan::LeftOuterJoin(join) => Self::left_outer_join_base_source(join),
        }
    }

    fn hash_base_source(join: &HashJoinPlan) -> &SourcePlan {
        match &join.input {
            HashJoinInputPlan::Source(source) => source,
            HashJoinInputPlan::InnerJoin(join) => Self::inner_join_base_source(join),
            HashJoinInputPlan::LeftOuterJoin(join) => Self::left_outer_join_base_source(join),
        }
    }

    fn base_source_mut(input: &mut FilterInputPlan) -> &mut SourcePlan {
        match input {
            FilterInputPlan::Source(relation) => relation,
            FilterInputPlan::InnerJoin(join) => Self::inner_join_base_source_mut(join),
            FilterInputPlan::LeftOuterJoin(join) => Self::left_outer_join_base_source_mut(join),
        }
    }

    fn inner_join_base_source_mut(join: &mut InnerJoinPlan) -> &mut SourcePlan {
        match &mut join.input {
            InnerJoinInputPlan::NestedLoop(join) => Self::nested_loop_base_source_mut(join),
            InnerJoinInputPlan::Hash(join) => Self::hash_base_source_mut(join),
            InnerJoinInputPlan::Condition(condition) => Self::condition_base_source_mut(condition),
        }
    }

    fn left_outer_join_base_source_mut(join: &mut LeftOuterJoinPlan) -> &mut SourcePlan {
        match &mut join.input {
            LeftOuterJoinInputPlan::NestedLoop(join) => Self::nested_loop_base_source_mut(join),
            LeftOuterJoinInputPlan::Hash(join) => Self::hash_base_source_mut(join),
            LeftOuterJoinInputPlan::Condition(condition) => {
                Self::condition_base_source_mut(condition)
            }
        }
    }

    fn condition_base_source_mut(condition: &mut JoinConditionPlan) -> &mut SourcePlan {
        match &mut condition.input {
            JoinConditionInputPlan::NestedLoop(join) => Self::nested_loop_base_source_mut(join),
            JoinConditionInputPlan::Hash(join) => Self::hash_base_source_mut(join),
        }
    }

    fn nested_loop_base_source_mut(join: &mut NestedLoopJoinPlan) -> &mut SourcePlan {
        match &mut join.input {
            NestedLoopJoinInputPlan::Source(source) => source,
            NestedLoopJoinInputPlan::InnerJoin(join) => Self::inner_join_base_source_mut(join),
            NestedLoopJoinInputPlan::LeftOuterJoin(join) => {
                Self::left_outer_join_base_source_mut(join)
            }
        }
    }

    fn hash_base_source_mut(join: &mut HashJoinPlan) -> &mut SourcePlan {
        match &mut join.input {
            HashJoinInputPlan::Source(source) => source,
            HashJoinInputPlan::InnerJoin(join) => Self::inner_join_base_source_mut(join),
            HashJoinInputPlan::LeftOuterJoin(join) => Self::left_outer_join_base_source_mut(join),
        }
    }

    fn plan_index_expr(
        &self,
        outer_context: Option<Rc<Context<'a>>>,
        indexes: &Indexes<'a>,
        expr: ExprPlan,
    ) -> Planned {
        match expr {
            ExprPlan::Nested(expr) => self.plan_index_expr(outer_context, indexes, *expr),
            ExprPlan::IsNull(expr) => self.search_is_null(outer_context, indexes, true, *expr),
            ExprPlan::IsNotNull(expr) => self.search_is_null(outer_context, indexes, false, *expr),
            ExprPlan::Subquery(query) => {
                let query = self.query(outer_context, *query);

                Planned::Expr(ExprPlan::Subquery(Box::new(query)))
            }
            ExprPlan::Exists { subquery, negated } => {
                let subquery = self.query(outer_context.as_ref().map(Rc::clone), *subquery);

                Planned::Expr(ExprPlan::Exists {
                    subquery: Box::new(subquery),
                    negated,
                })
            }
            ExprPlan::InSubquery {
                expr,
                subquery,
                negated,
            } => {
                let expr = self.subquery_expr(outer_context.as_ref().map(Rc::clone), *expr);
                let subquery = self.query(outer_context, *subquery);

                Planned::Expr(ExprPlan::InSubquery {
                    expr: Box::new(expr),
                    subquery: Box::new(subquery),
                    negated,
                })
            }
            ExprPlan::BinaryOp {
                left,
                op: BinaryOperator::And,
                right,
            } => {
                let left = match self.plan_index_expr(
                    outer_context.as_ref().map(Rc::clone),
                    indexes,
                    *left,
                ) {
                    Planned::Expr(expr) => expr,
                    Planned::IndexedExpr {
                        index_name,
                        index_op,
                        index_value_expr,
                        residual,
                    } => {
                        let residual = match residual {
                            Some(expr) => ExprPlan::BinaryOp {
                                left: Box::new(expr),
                                op: BinaryOperator::And,
                                right,
                            },
                            None => *right,
                        };

                        return Planned::IndexedExpr {
                            index_name,
                            index_op,
                            index_value_expr,
                            residual: Some(residual),
                        };
                    }
                };

                match self.plan_index_expr(outer_context, indexes, *right) {
                    Planned::Expr(expr) => Planned::Expr(ExprPlan::BinaryOp {
                        left: Box::new(left),
                        op: BinaryOperator::And,
                        right: Box::new(expr),
                    }),
                    Planned::IndexedExpr {
                        index_name,
                        index_op,
                        index_value_expr,
                        residual,
                    } => {
                        let residual = match residual {
                            Some(expr) => ExprPlan::BinaryOp {
                                left: Box::new(left),
                                op: BinaryOperator::And,
                                right: Box::new(expr),
                            },
                            None => left,
                        };

                        Planned::IndexedExpr {
                            index_name,
                            index_op,
                            index_value_expr,
                            residual: Some(residual),
                        }
                    }
                }
            }
            ExprPlan::BinaryOp {
                left,
                op: BinaryOperator::Gt,
                right,
            } => self.search_index_op(outer_context, indexes, IndexOperator::Gt, *left, *right),
            ExprPlan::BinaryOp {
                left,
                op: BinaryOperator::Lt,
                right,
            } => self.search_index_op(outer_context, indexes, IndexOperator::Lt, *left, *right),
            ExprPlan::BinaryOp {
                left,
                op: BinaryOperator::GtEq,
                right,
            } => self.search_index_op(outer_context, indexes, IndexOperator::GtEq, *left, *right),
            ExprPlan::BinaryOp {
                left,
                op: BinaryOperator::LtEq,
                right,
            } => self.search_index_op(outer_context, indexes, IndexOperator::LtEq, *left, *right),
            ExprPlan::BinaryOp {
                left,
                op: BinaryOperator::Eq,
                right,
            } => self.search_index_op(outer_context, indexes, IndexOperator::Eq, *left, *right),
            expr => {
                let expr = self.subquery_expr(outer_context, expr);

                Planned::Expr(expr)
            }
        }
    }

    fn indexes(&self, relation: &SourcePlan) -> Option<Indexes<'_>> {
        match relation {
            SourcePlan::Table(table) => self
                .schema_map
                .get(&table.name)
                .map(|schema| Indexes::new(&schema.indexes)),
            _ => None,
        }
    }

    fn search_is_null(
        &self,
        outer_context: Option<Rc<Context<'a>>>,
        indexes: &Indexes<'a>,
        null: bool,
        expr: ExprPlan,
    ) -> Planned {
        if let Some(index_name) = indexes.find(&expr) {
            let index_op = if null {
                IndexOperator::Eq
            } else {
                IndexOperator::Lt
            };

            return Planned::IndexedExpr {
                index_name,
                index_op,
                index_value_expr: ExprPlan::Value(Value::Null),
                residual: None,
            };
        }

        let expr = self.subquery_expr(outer_context, expr);
        let expr = if null {
            ExprPlan::IsNull(Box::new(expr))
        } else {
            ExprPlan::IsNotNull(Box::new(expr))
        };

        Planned::Expr(expr)
    }

    fn search_index_op(
        &self,
        outer_context: Option<Rc<Context<'a>>>,
        indexes: &Indexes<'a>,
        index_op: IndexOperator,
        left: ExprPlan,
        right: ExprPlan,
    ) -> Planned {
        if let Some(index_name) = indexes
            .find(&left)
            .filter(|_| is_deterministic(&right) && !may_return_null(&right))
        {
            let value_expr = self.subquery_expr(outer_context.clone(), right);

            return Planned::IndexedExpr {
                index_name,
                index_op,
                index_value_expr: value_expr,
                residual: None,
            };
        }

        if let Some(index_name) = indexes
            .find(&right)
            .filter(|_| is_deterministic(&left) && !may_return_null(&left))
        {
            let value_expr = self.subquery_expr(outer_context.clone(), left);

            return Planned::IndexedExpr {
                index_name,
                index_op: index_op.reverse(),
                index_value_expr: value_expr,
                residual: None,
            };
        }

        if let ExprPlan::Nested(left) = left {
            return self.search_index_op(outer_context, indexes, index_op, *left, right);
        }

        if let ExprPlan::Nested(right) = right {
            return self.search_index_op(outer_context, indexes, index_op, left, *right);
        }

        let left = self.subquery_expr(outer_context.clone(), left);
        let right = self.subquery_expr(outer_context, right);

        Planned::Expr(ExprPlan::BinaryOp {
            left: Box::new(left),
            op: index_op.into(),
            right: Box::new(right),
        })
    }
}

struct PlannedSchemaIndex<'a> {
    expr: ExprPlan,
    index: &'a SchemaIndex,
}

struct Indexes<'a>(Vec<PlannedSchemaIndex<'a>>);

impl<'a> Indexes<'a> {
    fn new(indexes: &'a [SchemaIndex]) -> Self {
        Self(
            indexes
                .iter()
                .map(|index| PlannedSchemaIndex {
                    expr: plan_scalar_expr(index.expr.clone()),
                    index,
                })
                .collect(),
        )
    }

    fn find(&self, target: &ExprPlan) -> Option<String> {
        self.0
            .iter()
            .find(|PlannedSchemaIndex { expr, .. }| expr == target)
            .map(|PlannedSchemaIndex { index, .. }| index.name.clone())
    }

    fn find_ordered(&self, target: &OrderByExprPlan) -> Option<String> {
        self.0
            .iter()
            .find(|PlannedSchemaIndex { expr, index }| {
                if expr != &target.expr {
                    return false;
                }

                matches!(
                    (target.asc, index.order),
                    (_, SchemaIndexOrd::Both)
                        | (Some(true) | None, SchemaIndexOrd::Asc)
                        | (Some(false), SchemaIndexOrd::Desc)
                )
            })
            .map(|PlannedSchemaIndex { index, .. }| index.name.clone())
    }
}

enum Planned {
    IndexedExpr {
        index_name: String,
        index_op: IndexOperator,
        index_value_expr: ExprPlan,
        residual: Option<ExprPlan>,
    },
    Expr(ExprPlan),
}

#[cfg(test)]
mod tests {
    use {
        super::plan,
        crate::{
            mock::{MockStorage, run},
            parse_sql::parse,
            plan::{
                DistinctInputPlan, DistinctPlan, LimitInputPlan, LimitPlan, OffsetInputPlan,
                OffsetPlan, ProjectInputPlan, QueryPlan, StatementPlan,
            },
            planner::fetch_schema_map,
            query_builder::{
                Build, col, exists, nested, non_clustered, null, num, primary_key, table, text,
            },
            result::{Error, Result},
            translate::translate,
        },
    };

    fn plan_index(storage: &MockStorage, sql: &str) -> Result<crate::plan::StatementPlan> {
        let parsed = parse(sql)?;
        let parsed = parsed
            .into_iter()
            .next()
            .ok_or_else(|| Error::StorageMsg(format!("no statement parsed from: {sql}")))?;
        let statement = StatementPlan::from(translate(&parsed)?);
        let schema_map = fetch_schema_map(storage, &statement)?;

        Ok(plan(&schema_map, statement))
    }

    fn storage_with_indexes() -> MockStorage {
        run("
CREATE TABLE Test (
    id INTEGER,
    flag BOOLEAN,
    name TEXT
);
CREATE INDEX idx_id ON Test (id);
CREATE INDEX idx_flag ON Test (flag);
CREATE INDEX idx_name ON Test (name);
")
    }

    #[test]
    fn index_planning_scenarios() {
        let storage = storage_with_indexes();

        let sql = "SELECT * FROM Test WHERE id = 1";
        let actual = plan_index(&storage, sql);
        let expected = table("Test")
            .index_by(non_clustered("idx_id".to_owned()).eq(num(1)))
            .select()
            .build();
        assert_eq!(actual, expected, "uses index for eq constant:\n{sql}");

        let sql = "SELECT * FROM Test WHERE id = NULL";
        let actual = plan_index(&storage, sql);
        let expected = table("Test").select().filter("id = NULL").build();
        assert_eq!(actual, expected, "skips index for nullable value:\n{sql}");

        let sql = "SELECT * FROM Test WHERE flag = ('ABC' IS NULL)";
        let actual = plan_index(&storage, sql);
        let expected = table("Test")
            .index_by(non_clustered("idx_flag".to_owned()).eq(nested(text("ABC").is_null())))
            .select()
            .build();
        assert_eq!(
            actual, expected,
            "uses index for deterministic expression:\n{sql}"
        );

        let sql = "SELECT * FROM Test ORDER BY name";
        let actual = plan_index(&storage, sql);
        let expected = table("Test")
            .index_by(non_clustered("idx_name".to_owned()))
            .select()
            .build();
        assert_eq!(actual, expected, "applies order by index:\n{sql}");

        let sql = "SELECT * FROM Test WHERE flag IS NULL";
        let actual = plan_index(&storage, sql);
        let expected = table("Test")
            .index_by(non_clustered("idx_flag".to_owned()).eq(null()))
            .select()
            .build();
        assert_eq!(actual, expected, "uses index for is null filter:\n{sql}");

        let sql = "SELECT * FROM Test WHERE flag IS NOT NULL";
        let actual = plan_index(&storage, sql);
        let expected = table("Test")
            .index_by(non_clustered("idx_flag".to_owned()).lt(null()))
            .select()
            .build();
        assert_eq!(
            actual, expected,
            "uses index for is not null filter:\n{sql}"
        );

        let sql = "SELECT * FROM Test WHERE id = flag";
        let actual = plan_index(&storage, sql);
        let expected = table("Test").select().filter("id = flag").build();
        assert_eq!(
            actual, expected,
            "skips index for non constant expression:\n{sql}"
        );

        let sql = "SELECT * FROM Test WHERE id = 1 AND name = 'Alice'";
        let actual = plan_index(&storage, sql);
        let expected = table("Test")
            .index_by(non_clustered("idx_id".to_owned()).eq(num(1)))
            .select()
            .filter("name = 'Alice'")
            .build();
        assert_eq!(actual, expected, "keeps residual filter:\n{sql}");

        let sql = "SELECT * FROM Test WHERE id = 1 ORDER BY name";
        let actual = plan_index(&storage, sql);
        let expected = table("Test")
            .index_by(non_clustered("idx_name".to_owned()))
            .select()
            .filter("id = 1")
            .build();
        assert_eq!(
            actual, expected,
            "keeps filter when order by owns the access path:\n{sql}"
        );

        let sql = "SELECT id FROM Test WHERE id = 1 GROUP BY id";
        let actual = plan_index(&storage, sql);
        let expected = table("Test")
            .index_by(non_clustered("idx_id".to_owned()).eq(num(1)))
            .select()
            .group_by("id")
            .project("id")
            .build();
        assert_eq!(actual, expected, "preserves aggregation wrapper:\n{sql}");

        let sql = "SELECT id FROM Test WHERE id = 1 GROUP BY id HAVING TRUE";
        let actual = plan_index(&storage, sql);
        let expected = table("Test")
            .index_by(non_clustered("idx_id".to_owned()).eq(num(1)))
            .select()
            .group_by("id")
            .having("TRUE")
            .project("id")
            .build();
        assert_eq!(actual, expected, "preserves having wrapper:\n{sql}");
    }

    #[test]
    fn index_planning_removes_or_keeps_typed_order_by_stage() {
        let storage = storage_with_indexes();

        let root_preserved = plan_index(&storage, "SELECT * FROM Test ORDER BY id + name");
        assert!(matches!(
            root_preserved,
            Ok(StatementPlan::Query(QueryPlan::SelectOrderBy(_)))
        ));

        let distinct_consumed = plan_index(&storage, "SELECT DISTINCT * FROM Test ORDER BY name");
        assert!(matches!(
            distinct_consumed,
            Ok(StatementPlan::Query(QueryPlan::Distinct(DistinctPlan {
                input: DistinctInputPlan::Project(_),
            })))
        ));

        let distinct_preserved =
            plan_index(&storage, "SELECT DISTINCT * FROM Test ORDER BY id + name");
        assert!(matches!(
            distinct_preserved,
            Ok(StatementPlan::Query(QueryPlan::Distinct(DistinctPlan {
                input: DistinctInputPlan::SelectOrderBy(_),
            })))
        ));

        let offset_body = plan_index(&storage, "SELECT * FROM Test OFFSET 1");
        assert!(matches!(
            offset_body,
            Ok(StatementPlan::Query(QueryPlan::Offset(OffsetPlan {
                input: OffsetInputPlan::Project(_),
                ..
            })))
        ));

        let offset_consumed = plan_index(&storage, "SELECT * FROM Test ORDER BY name OFFSET 1");
        assert!(matches!(
            offset_consumed,
            Ok(StatementPlan::Query(QueryPlan::Offset(OffsetPlan {
                input: OffsetInputPlan::Project(_),
                ..
            })))
        ));

        let offset_preserved =
            plan_index(&storage, "SELECT * FROM Test ORDER BY id + name OFFSET 1");
        assert!(matches!(
            offset_preserved,
            Ok(StatementPlan::Query(QueryPlan::Offset(OffsetPlan {
                input: OffsetInputPlan::SelectOrderBy(_),
                ..
            })))
        ));

        let limit_body = plan_index(&storage, "SELECT * FROM Test LIMIT 2");
        assert!(matches!(
            limit_body,
            Ok(StatementPlan::Query(QueryPlan::Limit(LimitPlan {
                input: LimitInputPlan::Project(_),
                ..
            })))
        ));

        let limit_consumed = plan_index(&storage, "SELECT * FROM Test ORDER BY name LIMIT 2");
        assert!(matches!(
            limit_consumed,
            Ok(StatementPlan::Query(QueryPlan::Limit(LimitPlan {
                input: LimitInputPlan::Project(_),
                ..
            })))
        ));

        let limit_preserved = plan_index(&storage, "SELECT * FROM Test ORDER BY id + name LIMIT 2");
        assert!(matches!(
            limit_preserved,
            Ok(StatementPlan::Query(QueryPlan::Limit(LimitPlan {
                input: LimitInputPlan::SelectOrderBy(_),
                ..
            })))
        ));

        let offset_limit_body = plan_index(&storage, "SELECT * FROM Test LIMIT 2 OFFSET 1");
        assert!(matches!(
            offset_limit_body,
            Ok(StatementPlan::Query(QueryPlan::Limit(LimitPlan {
                input: LimitInputPlan::Offset(OffsetPlan {
                    input: OffsetInputPlan::Project(_),
                    ..
                }),
                ..
            })))
        ));

        let consumed = plan_index(
            &storage,
            "SELECT * FROM Test ORDER BY name LIMIT 2 OFFSET 1",
        );
        assert!(matches!(
            consumed,
            Ok(StatementPlan::Query(QueryPlan::Limit(LimitPlan {
                input: LimitInputPlan::Offset(OffsetPlan {
                    input: OffsetInputPlan::Project(_),
                    ..
                }),
                ..
            })))
        ));

        let preserved = plan_index(
            &storage,
            "SELECT * FROM Test ORDER BY id + name LIMIT 2 OFFSET 1",
        );
        assert!(matches!(
            preserved,
            Ok(StatementPlan::Query(QueryPlan::Limit(LimitPlan {
                input: LimitInputPlan::Offset(OffsetPlan {
                    input: OffsetInputPlan::SelectOrderBy(_),
                    ..
                }),
                ..
            })))
        ));

        let aggregation_consumed =
            plan_index(&storage, "SELECT id FROM Test GROUP BY id ORDER BY name");
        assert!(matches!(
            aggregation_consumed,
            Ok(StatementPlan::Query(QueryPlan::Project(project)))
                if matches!(project.input, ProjectInputPlan::Aggregation(_))
        ));

        let aggregation_preserved = plan_index(
            &storage,
            "SELECT id FROM Test GROUP BY id ORDER BY id + name",
        );
        assert!(matches!(
            aggregation_preserved,
            Ok(StatementPlan::Query(QueryPlan::SelectOrderBy(order_by)))
                if matches!(order_by.input.input, ProjectInputPlan::Aggregation(_))
        ));

        let having_consumed = plan_index(
            &storage,
            "SELECT id FROM Test GROUP BY id HAVING TRUE ORDER BY name",
        );
        assert!(matches!(
            having_consumed,
            Ok(StatementPlan::Query(QueryPlan::Project(project)))
                if matches!(project.input, ProjectInputPlan::Having(_))
        ));
    }

    #[test]
    fn index_planning_keeps_existing_access_path() {
        let storage = storage_with_indexes();

        // Simulate the statement produced by the primary key planner, which runs
        // before the index planner: the table already carries an access path
        // (here a `PrimaryKey`) while the residual filter still references an
        // indexed column (`name`). The index planner must leave the existing
        // access path untouched instead of overwriting it with `idx_name`,
        // otherwise the primary key predicate would be silently dropped.
        let statement = table("Test")
            .index_by(primary_key().eq(num(1)))
            .select()
            .filter("name = 'x'")
            .build()
            .unwrap();

        let schema_map = fetch_schema_map(&storage, &statement).unwrap();
        let actual = plan(&schema_map, statement);

        let expected = table("Test")
            .index_by(primary_key().eq(num(1)))
            .select()
            .filter("name = 'x'")
            .build()
            .unwrap();
        assert_eq!(
            actual, expected,
            "keeps existing access path instead of clobbering it with a secondary index"
        );
    }

    #[test]
    fn index_planning_nested_queries() {
        let storage = storage_with_indexes();

        let sql = "
SELECT *
FROM Test
WHERE EXISTS (
    SELECT *
    FROM Test
    WHERE id = 1
);
";
        let actual = plan_index(&storage, sql);
        let expected = table("Test")
            .select()
            .filter(exists(
                table("Test")
                    .index_by(non_clustered("idx_id".to_owned()).eq(num(1)))
                    .select(),
            ))
            .build();
        assert_eq!(
            actual, expected,
            "uses index inside EXISTS subquery:\n{sql}"
        );

        let sql = "
SELECT *
FROM Test
WHERE id IN (
    SELECT id
    FROM Test
    WHERE flag = TRUE
);
";
        let actual = plan_index(&storage, sql);
        let expected = table("Test")
            .select()
            .filter(
                col("id").in_list(
                    table("Test")
                        .index_by(non_clustered("idx_flag".to_owned()).eq(true))
                        .select()
                        .project("id"),
                ),
            )
            .build();
        assert_eq!(actual, expected, "uses index inside IN subquery:\n{sql}");

        let sql = "
SELECT *
FROM Test
WHERE EXISTS (
    SELECT *
    FROM Test
    WHERE flag IS NULL
);
";
        let actual = plan_index(&storage, sql);
        let expected = table("Test")
            .select()
            .filter(exists(
                table("Test")
                    .index_by(non_clustered("idx_flag".to_owned()).eq(null()))
                    .select(),
            ))
            .build();
        assert_eq!(
            actual, expected,
            "uses index for NULL check inside subquery:\n{sql}"
        );
    }
}
