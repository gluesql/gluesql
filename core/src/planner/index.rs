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
            FilterPlan, IndexPredicatePlan, LimitInputPlan, LimitPlan, OffsetInputPlan, OffsetPlan,
            OrderByExprPlan, ProjectInputPlan, ProjectPlan, QueryPlan, SelectOrderByPlan,
            SourcePlan, StatementPlan, TableAccessPlan, plan_scalar_expr,
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
            ProjectInputPlan::Source(mut source) => {
                let (_, order_by) = self.source(outer_context, &mut source, None, order_by);

                (ProjectInputPlan::Source(source), order_by)
            }
            ProjectInputPlan::InnerJoin(mut join) => {
                let (_, order_by) =
                    self.source(outer_context, join.base_source_mut(), None, order_by);

                (ProjectInputPlan::InnerJoin(join), order_by)
            }
            ProjectInputPlan::LeftOuterJoin(mut join) => {
                let (_, order_by) =
                    self.source(outer_context, join.base_source_mut(), None, order_by);

                (ProjectInputPlan::LeftOuterJoin(join), order_by)
            }
            ProjectInputPlan::Filter(FilterPlan { mut input, expr }) => {
                let (expr, order_by) =
                    self.source(outer_context, input.base_source_mut(), Some(expr), order_by);
                let input = match expr {
                    Some(expr) => ProjectInputPlan::Filter(FilterPlan { input, expr }),
                    None => match input {
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
            AggregationInputPlan::Source(mut source) => {
                let (_, order_by) = self.source(outer_context, &mut source, None, order_by);

                (AggregationInputPlan::Source(source), order_by)
            }
            AggregationInputPlan::InnerJoin(mut join) => {
                let (_, order_by) =
                    self.source(outer_context, join.base_source_mut(), None, order_by);

                (AggregationInputPlan::InnerJoin(join), order_by)
            }
            AggregationInputPlan::LeftOuterJoin(mut join) => {
                let (_, order_by) =
                    self.source(outer_context, join.base_source_mut(), None, order_by);

                (AggregationInputPlan::LeftOuterJoin(join), order_by)
            }
            AggregationInputPlan::Filter(FilterPlan { mut input, expr }) => {
                let (expr, order_by) =
                    self.source(outer_context, input.base_source_mut(), Some(expr), order_by);
                let input = match expr {
                    Some(expr) => AggregationInputPlan::Filter(FilterPlan { input, expr }),
                    None => match input {
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
        source: &mut SourcePlan,
        filter_expr: Option<ExprPlan>,
        mut order_by: Vec<OrderByExprPlan>,
    ) -> (Option<ExprPlan>, Vec<OrderByExprPlan>) {
        let indexes = self.indexes(source);

        if let (Some(indexes), Some(order_expr)) = (indexes.as_ref(), order_by.last())
            && let SourcePlan::Table(table) = source
            && table.access == TableAccessPlan::FullScan
            && let Some(index_name) = indexes.find_ordered(order_expr)
        {
            table.access = TableAccessPlan::Index {
                name: index_name,
                asc: order_expr.asc,
                predicate: None,
            };
            order_by.pop();

            let filter_expr =
                filter_expr.map(|expr| self.subquery_expr(outer_context.map(Rc::clone), expr));

            return (filter_expr, order_by);
        }

        let filter_expr = filter_expr.and_then(|expr| {
            if let (Some(indexes), SourcePlan::Table(table)) = (indexes.as_ref(), &*source)
                && table.access == TableAccessPlan::FullScan
            {
                match self.plan_index_expr(outer_context.map(Rc::clone), indexes, expr) {
                    Planned::IndexedExpr {
                        index_name,
                        index_op,
                        index_value_expr,
                        residual,
                    } => {
                        if let SourcePlan::Table(table) = source {
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

        (filter_expr, order_by)
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
                        let right = self.subquery_expr(outer_context, *right);
                        let residual = match residual {
                            Some(expr) => ExprPlan::BinaryOp {
                                left: Box::new(expr),
                                op: BinaryOperator::And,
                                right: Box::new(right),
                            },
                            None => right,
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
            plan::StatementPlan,
            planner::fetch_schema_map,
            query_builder::{
                Build, TableAccessNode, col, exists, nested, non_clustered, null, num, primary_key,
                table, text, values,
            },
            result::{Error, Result},
            translate::translate,
        },
    };

    fn plan_index(storage: &MockStorage, sql: &str) -> Result<StatementPlan> {
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
    name TEXT,
    asc_name TEXT,
    desc_name TEXT
);
CREATE INDEX idx_id ON Test (id);
CREATE INDEX idx_flag ON Test (flag);
CREATE INDEX idx_name ON Test (name);
CREATE INDEX idx_asc_name ON Test (asc_name ASC);
CREATE INDEX idx_desc_name ON Test (desc_name DESC);
CREATE TABLE Other (other_id INTEGER);
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
    fn index_planning_respects_index_order() {
        let storage = storage_with_indexes();

        let sql = "SELECT * FROM Test ORDER BY asc_name";
        let actual = plan_index(&storage, sql);
        let expected = table("Test")
            .index_by(TableAccessNode::Index {
                name: "idx_asc_name".to_owned(),
                asc: None,
                predicate: None,
            })
            .select()
            .build();
        assert_eq!(actual, expected, "uses ascending index:\n{sql}");

        let sql = "SELECT * FROM Test ORDER BY desc_name DESC";
        let actual = plan_index(&storage, sql);
        let expected = table("Test")
            .index_by(TableAccessNode::Index {
                name: "idx_desc_name".to_owned(),
                asc: Some(false),
                predicate: None,
            })
            .select()
            .build();
        assert_eq!(actual, expected, "uses descending index:\n{sql}");

        let sql = "SELECT * FROM Test ORDER BY asc_name DESC";
        let actual = plan_index(&storage, sql);
        let expected = table("Test").select().order_by("asc_name DESC").build();
        assert_eq!(
            actual, expected,
            "keeps mismatched descending order:\n{sql}"
        );

        let sql = "SELECT * FROM Test ORDER BY desc_name";
        let actual = plan_index(&storage, sql);
        let expected = table("Test").select().order_by("desc_name").build();
        assert_eq!(actual, expected, "keeps mismatched ascending order:\n{sql}");
    }

    #[test]
    fn index_planning_removes_or_keeps_typed_order_by_stage() {
        let storage = storage_with_indexes();

        let sql = "SELECT * FROM Test ORDER BY id + name";
        let actual = plan_index(&storage, sql);
        let expected = table("Test").select().order_by("id + name").build();
        assert_eq!(actual, expected, "keeps unmatched order by:\n{sql}");

        let sql = "SELECT DISTINCT * FROM Test ORDER BY name";
        let actual = plan_index(&storage, sql);
        let expected = table("Test")
            .index_by(non_clustered("idx_name".to_owned()))
            .select()
            .distinct()
            .build();
        assert_eq!(
            actual, expected,
            "removes indexed distinct order by:\n{sql}"
        );

        let sql = "SELECT DISTINCT * FROM Test ORDER BY id + name";
        let actual = plan_index(&storage, sql);
        let expected = table("Test")
            .select()
            .order_by("id + name")
            .distinct()
            .build();
        assert_eq!(actual, expected, "keeps distinct order by:\n{sql}");

        let sql = "SELECT * FROM Test OFFSET 1";
        let actual = plan_index(&storage, sql);
        let expected = table("Test").select().offset(1).build();
        assert_eq!(actual, expected, "keeps offset input:\n{sql}");

        let sql = "SELECT * FROM Test ORDER BY name OFFSET 1";
        let actual = plan_index(&storage, sql);
        let expected = table("Test")
            .index_by(non_clustered("idx_name".to_owned()))
            .select()
            .offset(1)
            .build();
        assert_eq!(actual, expected, "removes indexed offset order by:\n{sql}");

        let sql = "SELECT * FROM Test ORDER BY id + name OFFSET 1";
        let actual = plan_index(&storage, sql);
        let expected = table("Test")
            .select()
            .order_by("id + name")
            .offset(1)
            .build();
        assert_eq!(actual, expected, "keeps offset order by:\n{sql}");

        let sql = "SELECT * FROM Test LIMIT 2";
        let actual = plan_index(&storage, sql);
        let expected = table("Test").select().limit(2).build();
        assert_eq!(actual, expected, "keeps limit input:\n{sql}");

        let sql = "SELECT * FROM Test ORDER BY name LIMIT 2";
        let actual = plan_index(&storage, sql);
        let expected = table("Test")
            .index_by(non_clustered("idx_name".to_owned()))
            .select()
            .limit(2)
            .build();
        assert_eq!(actual, expected, "removes indexed limit order by:\n{sql}");

        let sql = "SELECT * FROM Test ORDER BY id + name LIMIT 2";
        let actual = plan_index(&storage, sql);
        let expected = table("Test")
            .select()
            .order_by("id + name")
            .limit(2)
            .build();
        assert_eq!(actual, expected, "keeps limit order by:\n{sql}");

        let sql = "SELECT * FROM Test LIMIT 2 OFFSET 1";
        let actual = plan_index(&storage, sql);
        let expected = table("Test").select().offset(1).limit(2).build();
        assert_eq!(actual, expected, "keeps offset limit input:\n{sql}");

        let sql = "SELECT * FROM Test ORDER BY name LIMIT 2 OFFSET 1";
        let actual = plan_index(&storage, sql);
        let expected = table("Test")
            .index_by(non_clustered("idx_name".to_owned()))
            .select()
            .offset(1)
            .limit(2)
            .build();
        assert_eq!(
            actual, expected,
            "removes indexed terminal order by:\n{sql}"
        );

        let sql = "SELECT * FROM Test ORDER BY id + name LIMIT 2 OFFSET 1";
        let actual = plan_index(&storage, sql);
        let expected = table("Test")
            .select()
            .order_by("id + name")
            .offset(1)
            .limit(2)
            .build();
        assert_eq!(actual, expected, "keeps terminal order by:\n{sql}");

        let sql = "SELECT id FROM Test GROUP BY id ORDER BY name";
        let actual = plan_index(&storage, sql);
        let expected = table("Test")
            .index_by(non_clustered("idx_name".to_owned()))
            .select()
            .group_by("id")
            .project("id")
            .build();
        assert_eq!(actual, expected, "removes aggregate order by:\n{sql}");

        let sql = "SELECT id FROM Test GROUP BY id ORDER BY id + name";
        let actual = plan_index(&storage, sql);
        let expected = table("Test")
            .select()
            .group_by("id")
            .project("id")
            .order_by("id + name")
            .build();
        assert_eq!(actual, expected, "keeps aggregate order by:\n{sql}");

        let sql = "SELECT id FROM Test GROUP BY id HAVING TRUE ORDER BY name";
        let actual = plan_index(&storage, sql);
        let expected = table("Test")
            .index_by(non_clustered("idx_name".to_owned()))
            .select()
            .group_by("id")
            .having("TRUE")
            .project("id")
            .build();
        assert_eq!(actual, expected, "removes having order by:\n{sql}");

        let sql = "VALUES (1) ORDER BY column1 OFFSET 1";
        let actual = plan_index(&storage, sql);
        let expected = values(vec!["1"]).order_by("column1").offset(1).build();
        assert_eq!(actual, expected, "keeps values order by offset:\n{sql}");

        let sql = "VALUES (1) LIMIT 1 OFFSET 1";
        let actual = plan_index(&storage, sql);
        let expected = values(vec!["1"]).offset(1).limit(1).build();
        assert_eq!(actual, expected, "keeps values offset limit:\n{sql}");
    }

    #[test]
    fn index_planning_preserves_join_topology() {
        let storage = storage_with_indexes();

        let sql = "SELECT Test.id FROM Test JOIN Other ORDER BY name";
        let actual = plan_index(&storage, sql);
        let expected = table("Test")
            .index_by(non_clustered("idx_name".to_owned()))
            .select()
            .join("Other")
            .project("Test.id")
            .build();
        assert_eq!(actual, expected, "preserves ordered inner join:\n{sql}");

        let sql = "SELECT Test.id FROM Test LEFT JOIN Other ORDER BY name";
        let actual = plan_index(&storage, sql);
        let expected = table("Test")
            .index_by(non_clustered("idx_name".to_owned()))
            .select()
            .left_join("Other")
            .project("Test.id")
            .build();
        assert_eq!(
            actual, expected,
            "preserves ordered left outer join:\n{sql}"
        );

        let sql = "SELECT Test.id FROM Test JOIN Other GROUP BY Test.id ORDER BY name";
        let actual = plan_index(&storage, sql);
        let expected = table("Test")
            .index_by(non_clustered("idx_name".to_owned()))
            .select()
            .join("Other")
            .group_by("Test.id")
            .project("Test.id")
            .build();
        assert_eq!(actual, expected, "preserves aggregated inner join:\n{sql}");

        let sql = "SELECT Test.id FROM Test LEFT JOIN Other GROUP BY Test.id ORDER BY name";
        let actual = plan_index(&storage, sql);
        let expected = table("Test")
            .index_by(non_clustered("idx_name".to_owned()))
            .select()
            .left_join("Other")
            .group_by("Test.id")
            .project("Test.id")
            .build();
        assert_eq!(
            actual, expected,
            "preserves aggregated left outer join:\n{sql}"
        );

        let sql = "SELECT Test.id FROM Test JOIN Other WHERE id = 1";
        let actual = plan_index(&storage, sql);
        let expected = table("Test")
            .index_by(non_clustered("idx_id".to_owned()).eq(num(1)))
            .select()
            .join("Other")
            .project("Test.id")
            .build();
        assert_eq!(actual, expected, "preserves inner join:\n{sql}");

        let sql = "SELECT Test.id FROM Test LEFT JOIN Other WHERE id = 1";
        let actual = plan_index(&storage, sql);
        let expected = table("Test")
            .index_by(non_clustered("idx_id".to_owned()).eq(num(1)))
            .select()
            .left_join("Other")
            .project("Test.id")
            .build();
        assert_eq!(actual, expected, "preserves left outer join:\n{sql}");

        let sql = "SELECT Test.id FROM Test JOIN Other WHERE id = 1 GROUP BY Test.id";
        let actual = plan_index(&storage, sql);
        let expected = table("Test")
            .index_by(non_clustered("idx_id".to_owned()).eq(num(1)))
            .select()
            .join("Other")
            .group_by("Test.id")
            .project("Test.id")
            .build();
        assert_eq!(actual, expected, "preserves aggregated inner join:\n{sql}");

        let sql =
            "SELECT Test.id FROM Test LEFT JOIN Other WHERE id = 1 GROUP BY Test.id HAVING TRUE";
        let actual = plan_index(&storage, sql);
        let expected = table("Test")
            .index_by(non_clustered("idx_id".to_owned()).eq(num(1)))
            .select()
            .left_join("Other")
            .group_by("Test.id")
            .having("TRUE")
            .project("Test.id")
            .build();
        assert_eq!(actual, expected, "preserves having left outer join:\n{sql}");
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

        let sql = "
SELECT *
FROM Test
WHERE EXISTS (
    SELECT *
    FROM Test
    WHERE flag = TRUE
)
ORDER BY name;
";
        let actual = plan_index(&storage, sql);
        let expected = table("Test")
            .index_by(non_clustered("idx_name".to_owned()))
            .select()
            .filter(exists(
                table("Test")
                    .index_by(non_clustered("idx_flag".to_owned()).eq(true))
                    .select(),
            ))
            .build();
        assert_eq!(
            actual, expected,
            "plans filter subquery when order by owns the outer index:\n{sql}"
        );

        let sql = "
SELECT *
FROM Test
WHERE id = 1
  AND EXISTS (
    SELECT *
    FROM Test
    WHERE flag = TRUE
  );
";
        let actual = plan_index(&storage, sql);
        let expected = table("Test")
            .index_by(non_clustered("idx_id".to_owned()).eq(num(1)))
            .select()
            .filter(exists(
                table("Test")
                    .index_by(non_clustered("idx_flag".to_owned()).eq(true))
                    .select(),
            ))
            .build();
        assert_eq!(
            actual, expected,
            "plans right conjunct subquery after selecting the outer index:\n{sql}"
        );

        let sql = "
SELECT *
FROM Test
WHERE (id = 1 AND name IS NOT NULL)
  AND EXISTS (
    SELECT *
    FROM Test
    WHERE flag = TRUE
  );
";
        let actual = plan_index(&storage, sql);
        let expected = table("Test")
            .index_by(non_clustered("idx_id".to_owned()).eq(num(1)))
            .select()
            .filter(
                col("name").is_not_null().and(exists(
                    table("Test")
                        .index_by(non_clustered("idx_flag".to_owned()).eq(true))
                        .select(),
                )),
            )
            .build();
        assert_eq!(
            actual, expected,
            "preserves left residual and plans right conjunct subquery:\n{sql}"
        );
    }
}
