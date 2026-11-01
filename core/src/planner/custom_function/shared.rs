mod validate_function_definition;

pub(super) use validate_function_definition::validate as validate_function_definition;

use crate::{
    plan::{
        AggregateFunctionPlan, AggregationInputPlan, AggregationPlan, CountArgExprPlan,
        DistinctInputPlan, DistinctPlan, ExprPlan, FilterInputPlan, FilterPlan, HashJoinInputPlan,
        HashJoinPlan, HavingPlan, InnerJoinInputPlan, InnerJoinPlan, JoinConditionInputPlan,
        JoinConditionPlan, LeftOuterJoinInputPlan, LeftOuterJoinPlan, LimitInputPlan, LimitPlan,
        NestedLoopJoinInputPlan, NestedLoopJoinPlan, OffsetInputPlan, OffsetPlan, OrderByExprPlan,
        ProjectInputPlan, ProjectPlan, ProjectionPlan, QueryPlan, SelectItemPlan,
        SelectOrderByPlan, SourcePlan, TableAccessPlan, ValuesOrderByPlan, ValuesPlan,
    },
    result::Result,
};

// Shared by definition validation and call binding. Expression traversal remains
// explicit: the expression visitor does not enter queries on its own.
pub(super) fn try_visit_mut_query<F, G>(
    query: &mut QueryPlan,
    expr: &mut F,
    source: &mut G,
) -> Result<()>
where
    F: FnMut(&mut ExprPlan) -> Result<()>,
    G: FnMut(&SourcePlan) -> Result<()>,
{
    QueryVisitor { expr, source }.query(query)
}

struct QueryVisitor<'a, F, G> {
    expr: &'a mut F,
    source: &'a mut G,
}

impl<F, G> QueryVisitor<'_, F, G>
where
    F: FnMut(&mut ExprPlan) -> Result<()>,
    G: FnMut(&SourcePlan) -> Result<()>,
{
    fn query(&mut self, query: &mut QueryPlan) -> Result<()> {
        match query {
            QueryPlan::Project(plan) => self.project(plan),
            QueryPlan::Values(plan) => self.values(plan),
            QueryPlan::SelectOrderBy(plan) => self.select_order_by(plan),
            QueryPlan::ValuesOrderBy(plan) => self.values_order_by(plan),
            QueryPlan::Distinct(plan) => self.distinct(plan),
            QueryPlan::Offset(plan) => self.offset(plan),
            QueryPlan::Limit(plan) => self.limit(plan),
        }
    }

    fn limit(&mut self, plan: &mut LimitPlan) -> Result<()> {
        match &mut plan.input {
            LimitInputPlan::Project(plan) => self.project(plan),
            LimitInputPlan::Values(plan) => self.values(plan),
            LimitInputPlan::SelectOrderBy(plan) => self.select_order_by(plan),
            LimitInputPlan::ValuesOrderBy(plan) => self.values_order_by(plan),
            LimitInputPlan::Distinct(plan) => self.distinct(plan),
            LimitInputPlan::Offset(plan) => self.offset(plan),
        }?;
        (self.expr)(&mut plan.count)
    }

    fn offset(&mut self, plan: &mut OffsetPlan) -> Result<()> {
        match &mut plan.input {
            OffsetInputPlan::Project(plan) => self.project(plan),
            OffsetInputPlan::Values(plan) => self.values(plan),
            OffsetInputPlan::SelectOrderBy(plan) => self.select_order_by(plan),
            OffsetInputPlan::ValuesOrderBy(plan) => self.values_order_by(plan),
            OffsetInputPlan::Distinct(plan) => self.distinct(plan),
        }?;
        (self.expr)(&mut plan.count)
    }

    fn distinct(&mut self, plan: &mut DistinctPlan) -> Result<()> {
        match &mut plan.input {
            DistinctInputPlan::Project(plan) => self.project(plan),
            DistinctInputPlan::SelectOrderBy(plan) => self.select_order_by(plan),
        }
    }

    fn select_order_by(&mut self, plan: &mut SelectOrderByPlan) -> Result<()> {
        self.project(&mut plan.input)?;
        self.order_by(&mut plan.exprs)
    }

    fn values_order_by(&mut self, plan: &mut ValuesOrderByPlan) -> Result<()> {
        self.values(&mut plan.input)?;
        self.order_by(&mut plan.exprs)
    }

    fn order_by(&mut self, exprs: &mut [OrderByExprPlan]) -> Result<()> {
        for expr in exprs {
            (self.expr)(&mut expr.expr)?;
        }
        Ok(())
    }

    fn values(&mut self, ValuesPlan(rows): &mut ValuesPlan) -> Result<()> {
        for expr in rows.iter_mut().flatten() {
            (self.expr)(expr)?;
        }
        Ok(())
    }

    fn project(&mut self, plan: &mut ProjectPlan) -> Result<()> {
        match &mut plan.input {
            ProjectInputPlan::Source(source) => self.source(source),
            ProjectInputPlan::InnerJoin(join) => self.inner_join(join),
            ProjectInputPlan::LeftOuterJoin(join) => self.left_outer_join(join),
            ProjectInputPlan::Filter(filter) => self.filter(filter),
            ProjectInputPlan::Aggregation(plan) => self.aggregation(plan),
            ProjectInputPlan::Having(plan) => self.having(plan),
        }?;
        if let ProjectionPlan::SelectItems(items) = &mut plan.projection {
            for item in items {
                if let SelectItemPlan::Expr { expr, .. } = item {
                    (self.expr)(expr)?;
                }
            }
        }
        Ok(())
    }

    fn aggregation(&mut self, plan: &mut AggregationPlan) -> Result<()> {
        match &mut plan.input {
            AggregationInputPlan::Source(source) => self.source(source),
            AggregationInputPlan::InnerJoin(join) => self.inner_join(join),
            AggregationInputPlan::LeftOuterJoin(join) => self.left_outer_join(join),
            AggregationInputPlan::Filter(filter) => self.filter(filter),
        }?;
        for expr in &mut plan.group_by {
            (self.expr)(expr)?;
        }
        for aggregate in &mut plan.aggregate_slots {
            match &mut aggregate.func {
                AggregateFunctionPlan::Count(CountArgExprPlan::Wildcard) => {}
                AggregateFunctionPlan::Count(CountArgExprPlan::Expr(expr))
                | AggregateFunctionPlan::Sum(expr)
                | AggregateFunctionPlan::Min(expr)
                | AggregateFunctionPlan::Max(expr)
                | AggregateFunctionPlan::Avg(expr)
                | AggregateFunctionPlan::Variance(expr)
                | AggregateFunctionPlan::Stdev(expr) => (self.expr)(expr)?,
            }
        }
        Ok(())
    }

    fn filter(&mut self, plan: &mut FilterPlan) -> Result<()> {
        match &mut plan.input {
            FilterInputPlan::Source(source) => self.source(source),
            FilterInputPlan::InnerJoin(join) => self.inner_join(join),
            FilterInputPlan::LeftOuterJoin(join) => self.left_outer_join(join),
        }?;
        (self.expr)(&mut plan.expr)
    }

    fn having(&mut self, plan: &mut HavingPlan) -> Result<()> {
        self.aggregation(&mut plan.input)?;
        (self.expr)(&mut plan.expr)
    }

    fn inner_join(&mut self, plan: &mut InnerJoinPlan) -> Result<()> {
        match &mut plan.input {
            InnerJoinInputPlan::NestedLoop(plan) => self.nested_loop_join(plan),
            InnerJoinInputPlan::Hash(plan) => self.hash_join(plan),
            InnerJoinInputPlan::Condition(plan) => self.join_condition(plan),
        }
    }

    fn left_outer_join(&mut self, plan: &mut LeftOuterJoinPlan) -> Result<()> {
        match &mut plan.input {
            LeftOuterJoinInputPlan::NestedLoop(plan) => self.nested_loop_join(plan),
            LeftOuterJoinInputPlan::Hash(plan) => self.hash_join(plan),
            LeftOuterJoinInputPlan::Condition(plan) => self.join_condition(plan),
        }
    }

    fn join_condition(&mut self, plan: &mut JoinConditionPlan) -> Result<()> {
        match &mut plan.input {
            JoinConditionInputPlan::NestedLoop(plan) => self.nested_loop_join(plan),
            JoinConditionInputPlan::Hash(plan) => self.hash_join(plan),
        }?;
        (self.expr)(&mut plan.expr)
    }

    fn nested_loop_join(&mut self, plan: &mut NestedLoopJoinPlan) -> Result<()> {
        match &mut plan.input {
            NestedLoopJoinInputPlan::Source(source) => self.source(source),
            NestedLoopJoinInputPlan::InnerJoin(join) => self.inner_join(join),
            NestedLoopJoinInputPlan::LeftOuterJoin(join) => self.left_outer_join(join),
        }?;
        self.source(&mut plan.right)
    }

    fn hash_join(&mut self, plan: &mut HashJoinPlan) -> Result<()> {
        match &mut plan.input {
            HashJoinInputPlan::Source(source) => self.source(source),
            HashJoinInputPlan::InnerJoin(join) => self.inner_join(join),
            HashJoinInputPlan::LeftOuterJoin(join) => self.left_outer_join(join),
        }?;
        self.source(&mut plan.right)?;
        (self.expr)(&mut plan.input_key)?;
        (self.expr)(&mut plan.right_key)?;
        if let Some(expr) = &mut plan.right_filter {
            (self.expr)(expr)?;
        }
        Ok(())
    }

    fn source(&mut self, source: &mut SourcePlan) -> Result<()> {
        (self.source)(source)?;
        match source {
            SourcePlan::Table(table) => match &mut table.access {
                TableAccessPlan::PrimaryKey { expr } => (self.expr)(expr)?,
                TableAccessPlan::Index {
                    predicate: Some(predicate),
                    ..
                } => {
                    (self.expr)(&mut predicate.expr)?;
                }
                TableAccessPlan::FullScan
                | TableAccessPlan::Index {
                    predicate: None, ..
                } => {}
            },
            SourcePlan::Derived(derived) => self.query(&mut derived.query)?,
            SourcePlan::Series(series) => (self.expr)(&mut series.size)?,
            SourcePlan::Dictionary(_) => {}
        }
        Ok(())
    }
}
