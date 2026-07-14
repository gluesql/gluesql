use {
    super::context::Context,
    crate::{
        ast::ColumnDef,
        data::Schema,
        plan::{ExprPlan, FunctionPlan, QueryPlan, TableAliasPlan, TableFactorPlan},
    },
    std::rc::Rc,
};

pub trait Planner<'a> {
    fn get_schema(&self, name: &str) -> Option<&'a Schema>;

    fn query(&self, outer_context: Option<Rc<Context<'a>>>, query: QueryPlan) -> QueryPlan;

    fn subquery_expr(&self, outer_context: Option<Rc<Context<'a>>>, expr: ExprPlan) -> ExprPlan {
        match expr {
            ExprPlan::IsNull(expr) => {
                ExprPlan::IsNull(Box::new(self.subquery_expr(outer_context, *expr)))
            }
            ExprPlan::IsNotNull(expr) => {
                ExprPlan::IsNotNull(Box::new(self.subquery_expr(outer_context, *expr)))
            }
            ExprPlan::InList {
                expr,
                list,
                negated,
            } => {
                let list = list
                    .into_iter()
                    .map(|expr| self.subquery_expr(outer_context.as_ref().map(Rc::clone), expr))
                    .collect();
                let expr = Box::new(self.subquery_expr(outer_context, *expr));

                ExprPlan::InList {
                    expr,
                    list,
                    negated,
                }
            }
            ExprPlan::Subquery(query) => {
                ExprPlan::Subquery(Box::new(self.query(outer_context, *query)))
            }
            ExprPlan::Exists { subquery, negated } => ExprPlan::Exists {
                subquery: Box::new(self.query(outer_context, *subquery)),
                negated,
            },
            ExprPlan::InSubquery {
                expr,
                subquery,
                negated,
            } => {
                let expr =
                    Box::new(self.subquery_expr(outer_context.as_ref().map(Rc::clone), *expr));
                let subquery = Box::new(self.query(outer_context, *subquery));

                ExprPlan::InSubquery {
                    expr,
                    subquery,
                    negated,
                }
            }
            ExprPlan::Between {
                expr,
                negated,
                low,
                high,
            } => {
                let expr =
                    Box::new(self.subquery_expr(outer_context.as_ref().map(Rc::clone), *expr));
                let low = Box::new(self.subquery_expr(outer_context.as_ref().map(Rc::clone), *low));
                let high = Box::new(self.subquery_expr(outer_context, *high));

                ExprPlan::Between {
                    expr,
                    negated,
                    low,
                    high,
                }
            }
            ExprPlan::Like {
                expr,
                negated,
                pattern,
            } => {
                let expr =
                    Box::new(self.subquery_expr(outer_context.as_ref().map(Rc::clone), *expr));
                let pattern =
                    Box::new(self.subquery_expr(outer_context.as_ref().map(Rc::clone), *pattern));

                ExprPlan::Like {
                    expr,
                    negated,
                    pattern,
                }
            }
            ExprPlan::ILike {
                expr,
                negated,
                pattern,
            } => {
                let expr =
                    Box::new(self.subquery_expr(outer_context.as_ref().map(Rc::clone), *expr));
                let pattern =
                    Box::new(self.subquery_expr(outer_context.as_ref().map(Rc::clone), *pattern));

                ExprPlan::ILike {
                    expr,
                    negated,
                    pattern,
                }
            }
            ExprPlan::BinaryOp { left, op, right } => ExprPlan::BinaryOp {
                left: Box::new(self.subquery_expr(outer_context.as_ref().map(Rc::clone), *left)),
                op,
                right: Box::new(self.subquery_expr(outer_context, *right)),
            },
            ExprPlan::UnaryOp { op, expr } => ExprPlan::UnaryOp {
                op,
                expr: Box::new(self.subquery_expr(outer_context, *expr)),
            },
            ExprPlan::Nested(expr) => {
                ExprPlan::Nested(Box::new(self.subquery_expr(outer_context, *expr)))
            }
            ExprPlan::Case {
                operand,
                when_then,
                else_result,
            } => {
                let operand = operand.map(|expr| {
                    Box::new(self.subquery_expr(outer_context.as_ref().map(Rc::clone), *expr))
                });
                let when_then = when_then
                    .into_iter()
                    .map(|(when, then)| {
                        let when = self.subquery_expr(outer_context.as_ref().map(Rc::clone), when);
                        let then = self.subquery_expr(outer_context.as_ref().map(Rc::clone), then);

                        (when, then)
                    })
                    .collect();
                let else_result =
                    else_result.map(|expr| Box::new(self.subquery_expr(outer_context, *expr)));

                ExprPlan::Case {
                    operand,
                    when_then,
                    else_result,
                }
            }
            ExprPlan::ArrayIndex { obj, indexes } => {
                let indexes = indexes
                    .into_iter()
                    .map(|expr| self.subquery_expr(outer_context.as_ref().map(Rc::clone), expr))
                    .collect();
                let obj = Box::new(self.subquery_expr(outer_context, *obj));
                ExprPlan::ArrayIndex { obj, indexes }
            }
            ExprPlan::Array { elem } => {
                let elem = elem
                    .into_iter()
                    .map(|expr| self.subquery_expr(outer_context.as_ref().map(Rc::clone), expr))
                    .collect();
                ExprPlan::Array { elem }
            }
            ExprPlan::Interval {
                expr,
                leading_field,
                last_field,
            } => ExprPlan::Interval {
                expr: Box::new(self.subquery_expr(outer_context, *expr)),
                leading_field,
                last_field,
            },
            ExprPlan::Function(func) => match *func {
                FunctionPlan::Cast { expr, data_type } => {
                    ExprPlan::Function(Box::new(FunctionPlan::Cast {
                        expr: self.subquery_expr(outer_context, expr),
                        data_type,
                    }))
                }
                FunctionPlan::Extract { field, expr } => {
                    ExprPlan::Function(Box::new(FunctionPlan::Extract {
                        field,
                        expr: self.subquery_expr(outer_context, expr),
                    }))
                }
                _ => ExprPlan::Function(func),
            },
            ExprPlan::Identifier(_)
            | ExprPlan::CompoundIdentifier { .. }
            | ExprPlan::Literal(_)
            | ExprPlan::Value(_)
            | ExprPlan::TypedString { .. }
            | ExprPlan::Aggregate(_) => expr,
        }
    }

    fn update_context(
        &self,
        next: Option<Rc<Context<'a>>>,
        table_factor: &TableFactorPlan,
    ) -> Option<Rc<Context<'a>>> {
        let (name, alias) = match table_factor {
            TableFactorPlan::Table { name, alias, .. } => {
                let alias = alias
                    .as_ref()
                    .map(|TableAliasPlan { name, .. }| name.clone());

                (name, alias)
            }
            TableFactorPlan::Derived { .. }
            | TableFactorPlan::Series { .. }
            | TableFactorPlan::Dictionary { .. } => return next,
        };

        let Some(Schema { column_defs, .. }) = self.get_schema(name) else {
            return next;
        };

        let Some(column_defs) = column_defs else {
            return next;
        };

        let columns = column_defs
            .iter()
            .map(|ColumnDef { name, .. }| name.as_str())
            .collect::<Vec<_>>();

        let context = Context::new(alias.unwrap_or_else(|| name.to_owned()), columns, next);
        Some(Rc::new(context))
    }
}
