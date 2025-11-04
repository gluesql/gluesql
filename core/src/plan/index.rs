use {
    super::{context::Context, planner::Planner},
    crate::{
        ast::{
            AstLiteral, BinaryOperator, Expr, IndexItem, IndexOperator, OrderByExpr, Query, Select,
            SetExpr, Statement, TableFactor,
        },
        data::{Schema, SchemaIndex, SchemaIndexOrd},
        plan::expr::{deterministic::is_deterministic, nullability::may_return_null},
    },
    std::{collections::HashMap, hash::BuildHasher, sync::Arc},
};

pub fn plan<S: BuildHasher>(
    schema_map: &HashMap<String, Schema, S>,
    statement: Statement,
) -> Statement {
    let planner = IndexPlanner { schema_map };

    match statement {
        Statement::Query(query) => {
            let query = planner.query(None, query);

            Statement::Query(query)
        }
        _ => statement,
    }
}

struct IndexPlanner<'a, S> {
    schema_map: &'a HashMap<String, Schema, S>,
}

impl<'a, S: BuildHasher> Planner<'a> for IndexPlanner<'a, S> {
    fn query(&self, outer_context: Option<Arc<Context<'a>>>, query: Query) -> Query {
        let Query {
            body,
            order_by,
            limit,
            offset,
        } = query;

        let (body, order_by) = match body {
            SetExpr::Select(select) => {
                let (select, order_by) = self.select(outer_context.as_ref(), *select, order_by);

                (SetExpr::Select(Box::new(select)), order_by)
            }
            SetExpr::Values(values) => (SetExpr::Values(values), order_by),
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

impl<'a, S: BuildHasher> IndexPlanner<'a, S> {
    fn select(
        &self,
        outer_context: Option<&Arc<Context<'a>>>,
        select: Select,
        mut order_by: Vec<OrderByExpr>,
    ) -> (Select, Vec<OrderByExpr>) {
        let Select {
            distinct,
            projection,
            mut from,
            selection,
            group_by,
            having,
        } = select;

        let indexes = self.indexes(&from.relation);

        if let (Some(indexes), Some(order_expr)) = (indexes.as_ref(), order_by.last())
            && let TableFactor::Table { index, .. } = &mut from.relation
            && index.is_none()
            && let Some(index_name) = indexes.find_ordered(order_expr)
        {
            *index = Some(IndexItem::NonClustered {
                name: index_name,
                asc: order_expr.asc,
                cmp_expr: None,
            });
            order_by.pop();

            let select = Select {
                distinct,
                projection,
                from,
                selection,
                group_by,
                having,
            };

            return (select, order_by);
        }

        let selection = selection.and_then(|expr| {
            if let (Some(indexes), TableFactor::Table { .. }) = (indexes.as_ref(), &from.relation) {
                match self.plan_index_expr(outer_context.map(Arc::clone), indexes, expr) {
                    Planned::IndexedExpr {
                        index_name,
                        index_op,
                        index_value_expr,
                        selection,
                    } => {
                        if let TableFactor::Table { index, .. } = &mut from.relation {
                            *index = Some(IndexItem::NonClustered {
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
                Some(self.subquery_expr(outer_context.map(Arc::clone), expr))
            }
        });

        let select = Select {
            distinct,
            projection,
            from,
            selection,
            group_by,
            having,
        };

        (select, order_by)
    }

    fn plan_index_expr(
        &self,
        outer_context: Option<Arc<Context<'a>>>,
        indexes: &Indexes<'a>,
        selection: Expr,
    ) -> Planned {
        match selection {
            Expr::Nested(expr) => self.plan_index_expr(outer_context, indexes, *expr),
            Expr::IsNull(expr) => self.search_is_null(outer_context, indexes, true, *expr),
            Expr::IsNotNull(expr) => self.search_is_null(outer_context, indexes, false, *expr),
            Expr::Subquery(query) => {
                let query = self.query(outer_context, *query);

                Planned::Expr(Expr::Subquery(Box::new(query)))
            }
            Expr::Exists { subquery, negated } => {
                let subquery = self.query(outer_context.as_ref().map(Arc::clone), *subquery);

                Planned::Expr(Expr::Exists {
                    subquery: Box::new(subquery),
                    negated,
                })
            }
            Expr::InSubquery {
                expr,
                subquery,
                negated,
            } => {
                let expr = self.subquery_expr(outer_context.as_ref().map(Arc::clone), *expr);
                let subquery = self.query(outer_context, *subquery);

                Planned::Expr(Expr::InSubquery {
                    expr: Box::new(expr),
                    subquery: Box::new(subquery),
                    negated,
                })
            }
            Expr::BinaryOp {
                left,
                op: BinaryOperator::And,
                right,
            } => {
                let left = match self.plan_index_expr(
                    outer_context.as_ref().map(Arc::clone),
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
                            Some(expr) => Expr::BinaryOp {
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
                    Planned::Expr(expr) => Planned::Expr(Expr::BinaryOp {
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
                            Some(expr) => Expr::BinaryOp {
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
            Expr::BinaryOp {
                left,
                op: BinaryOperator::Gt,
                right,
            } => self.search_index_op(outer_context, indexes, IndexOperator::Gt, *left, *right),
            Expr::BinaryOp {
                left,
                op: BinaryOperator::Lt,
                right,
            } => self.search_index_op(outer_context, indexes, IndexOperator::Lt, *left, *right),
            Expr::BinaryOp {
                left,
                op: BinaryOperator::GtEq,
                right,
            } => self.search_index_op(outer_context, indexes, IndexOperator::GtEq, *left, *right),
            Expr::BinaryOp {
                left,
                op: BinaryOperator::LtEq,
                right,
            } => self.search_index_op(outer_context, indexes, IndexOperator::LtEq, *left, *right),
            Expr::BinaryOp {
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

    fn indexes(&self, relation: &TableFactor) -> Option<Indexes<'_>> {
        match relation {
            TableFactor::Table { name, .. } => self
                .schema_map
                .get(name)
                .map(|schema| Indexes(&schema.indexes)),
            _ => None,
        }
    }

    fn search_is_null(
        &self,
        outer_context: Option<Arc<Context<'a>>>,
        indexes: &Indexes<'a>,
        null: bool,
        expr: Expr,
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
                index_value_expr: Expr::Literal(AstLiteral::Null),
                selection: None,
            };
        }

        let expr = self.subquery_expr(outer_context, expr);
        let expr = if null {
            Expr::IsNull(Box::new(expr))
        } else {
            Expr::IsNotNull(Box::new(expr))
        };

        Planned::Expr(expr)
    }

    fn search_index_op(
        &self,
        outer_context: Option<Arc<Context<'a>>>,
        indexes: &Indexes<'a>,
        index_op: IndexOperator,
        left: Expr,
        right: Expr,
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

        if let Expr::Nested(left) = left {
            return self.search_index_op(outer_context, indexes, index_op, *left, right);
        }

        if let Expr::Nested(right) = right {
            return self.search_index_op(outer_context, indexes, index_op, left, *right);
        }

        let left = self.subquery_expr(outer_context.clone(), left);
        let right = self.subquery_expr(outer_context, right);

        Planned::Expr(Expr::BinaryOp {
            left: Box::new(left),
            op: index_op.into(),
            right: Box::new(right),
        })
    }
}

struct Indexes<'a>(&'a [SchemaIndex]);

impl Indexes<'_> {
    fn find(&self, target: &Expr) -> Option<String> {
        self.0
            .iter()
            .find(|SchemaIndex { expr, .. }| expr == target)
            .map(|SchemaIndex { name, .. }| name.to_owned())
    }

    fn find_ordered(&self, target: &OrderByExpr) -> Option<String> {
        self.0
            .iter()
            .find(|SchemaIndex { expr, order, .. }| {
                if expr != &target.expr {
                    return false;
                }

                matches!(
                    (target.asc, order),
                    (_, SchemaIndexOrd::Both)
                        | (Some(true) | None, SchemaIndexOrd::Asc)
                        | (Some(false), SchemaIndexOrd::Desc)
                )
            })
            .map(|SchemaIndex { name, .. }| name.to_owned())
    }
}

enum Planned {
    IndexedExpr {
        index_name: String,
        index_op: IndexOperator,
        index_value_expr: Expr,
        selection: Option<Expr>,
    },
    Expr(Expr),
}

#[cfg(test)]
mod tests {
    use {
        super::plan,
        crate::{
            ast::Statement,
            ast_builder::{Build, col, exists, nested, non_clustered, null, num, table, text},
            mock::{MockStorage, run},
            parse_sql::parse,
            plan::fetch_schema_map,
            result::{Error, Result},
            translate::translate,
        },
        futures::executor::block_on,
    };

    fn plan_index(storage: &MockStorage, sql: &str) -> Result<Statement> {
        let parsed = parse(sql)?;
        let parsed = parsed
            .into_iter()
            .next()
            .ok_or_else(|| Error::StorageMsg(format!("no statement parsed from: {sql}")))?;
        let statement = translate(&parsed)?;
        let schema_map = block_on(fetch_schema_map(storage, &statement))?;

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
