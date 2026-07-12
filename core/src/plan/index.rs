use {
    super::{context::Context, planner::Planner},
    crate::{
        ast::{BinaryOperator, IndexOperator},
        data::{Schema, SchemaIndex, SchemaIndexOrd, Value},
        plan::{
            ExprPlan, IndexItemPlan, LimitInputPlan, LimitPlan, OffsetPlan, OrderByExprPlan,
            QueryBodyPlan, QueryPlan, SelectPlan, SetExprPlan, StatementPlan, TableFactorPlan,
            expr::{deterministic::is_deterministic, nullability::may_return_null},
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
        let plan_body = |QueryBodyPlan { body, order_by }| {
            let (body, order_by) = match body {
                SetExprPlan::Select(select) => {
                    let (select, order_by) = self.select(outer_context.as_ref(), *select, order_by);

                    (SetExprPlan::Select(Box::new(select)), order_by)
                }
                SetExprPlan::Values(values) => (SetExprPlan::Values(values), order_by),
            };

            QueryBodyPlan { body, order_by }
        };

        match query {
            QueryPlan::Body(body) => QueryPlan::Body(plan_body(body)),
            QueryPlan::Offset(OffsetPlan { input, count }) => QueryPlan::Offset(OffsetPlan {
                input: plan_body(input),
                count,
            }),
            QueryPlan::Limit(LimitPlan { input, count }) => {
                let input = match input {
                    LimitInputPlan::Body(body) => LimitInputPlan::Body(plan_body(body)),
                    LimitInputPlan::Offset(OffsetPlan { input, count }) => {
                        LimitInputPlan::Offset(OffsetPlan {
                            input: plan_body(input),
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

impl<'a, S: BuildHasher> IndexPlanner<'a, S> {
    fn select(
        &self,
        outer_context: Option<&Rc<Context<'a>>>,
        select: SelectPlan,
        mut order_by: Vec<OrderByExprPlan>,
    ) -> (SelectPlan, Vec<OrderByExprPlan>) {
        let SelectPlan {
            distinct,
            projection,
            mut from,
            selection,
            group_by,
            having,
            aggregate_slots,
        } = select;

        let indexes = self.indexes(&from.relation);

        if let (Some(indexes), Some(order_expr)) = (indexes.as_ref(), order_by.last())
            && let TableFactorPlan::Table { index, .. } = &mut from.relation
            && index.is_none()
            && let Some(index_name) = indexes.find_ordered(order_expr)
        {
            *index = Some(IndexItemPlan::NonClustered {
                name: index_name,
                asc: order_expr.asc,
                cmp_expr: None,
            });
            order_by.pop();

            let select = SelectPlan {
                distinct,
                projection,
                from,
                selection,
                group_by,
                having,
                aggregate_slots,
            };

            return (select, order_by);
        }

        let selection = selection.and_then(|expr| {
            if let (Some(indexes), TableFactorPlan::Table { index: None, .. }) =
                (indexes.as_ref(), &from.relation)
            {
                match self.plan_index_expr(outer_context.map(Rc::clone), indexes, expr) {
                    Planned::IndexedExpr {
                        index_name,
                        index_op,
                        index_value_expr,
                        selection,
                    } => {
                        if let TableFactorPlan::Table { index, .. } = &mut from.relation {
                            *index = Some(IndexItemPlan::NonClustered {
                                name: index_name,
                                asc: None,
                                cmp_expr: Some((index_op, index_value_expr)),
                            });
                        }

                        selection
                    }
                    Planned::Expr(expr) => Some(expr),
                }
            } else {
                Some(self.subquery_expr(outer_context.map(Rc::clone), expr))
            }
        });

        let select = SelectPlan {
            distinct,
            projection,
            from,
            selection,
            group_by,
            having,
            aggregate_slots,
        };

        (select, order_by)
    }

    fn plan_index_expr(
        &self,
        outer_context: Option<Rc<Context<'a>>>,
        indexes: &Indexes<'a>,
        selection: ExprPlan,
    ) -> Planned {
        match selection {
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
                        selection,
                    } => {
                        let selection = match selection {
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
                            selection: Some(selection),
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
                        selection,
                    } => {
                        let selection = match selection {
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
                            selection: Some(selection),
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

    fn indexes(&self, relation: &TableFactorPlan) -> Option<Indexes<'_>> {
        match relation {
            TableFactorPlan::Table { name, .. } => self
                .schema_map
                .get(name)
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
                selection: None,
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
                selection: None,
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
                selection: None,
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
        selection: Option<ExprPlan>,
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
            plan::{StatementPlan, fetch_schema_map},
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
    }

    #[test]
    fn index_planning_keeps_existing_access_path() {
        let storage = storage_with_indexes();

        // Simulate the statement produced by the primary key planner, which runs
        // before the index planner: the table already carries an access path
        // (here a `PrimaryKey`) while the leftover selection still references an
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
