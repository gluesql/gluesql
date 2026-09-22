use {
    crate::{
        ast::BinaryOperator,
        plan::{
            ExprPlan, FilterInputPlan, FilterPlan, ProjectInputPlan, QueryPlan, SourcePlan,
            StatementPlan, TableAccessPlan,
        },
        result::Result,
        store::{Statistic, Statistics},
    },
    bigdecimal::ToPrimitive,
};

/// Conservative cardinality used when a storage cannot provide one.
pub const DEFAULT_FULL_SCAN_CARDINALITY: u64 = 1_000;
const DEFAULT_EQUALITY_SELECTIVITY: f64 = 0.1;
const DEFAULT_RANGE_SELECTIVITY: f64 = 1.0 / 3.0;

/// Estimates collected from an already-produced statement plan.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct PlanStatistics {
    /// Estimates for full table scans in the plan.
    pub full_scans: Vec<FullScanStatistics>,
    /// Estimates for filter operators in the plan.
    pub filters: Vec<FilterStatistics>,
}

/// Cardinality and modeled cost for one full table scan.
#[derive(Clone, Debug, PartialEq)]
pub struct FullScanStatistics {
    /// Name of the scanned table.
    pub table_name: String,
    /// Estimated or exact input cardinality.
    pub cardinality: Statistic<u64>,
    /// Modeled scan cost.
    pub cost: Statistic<u64>,
}

/// Cardinality and modeled cost for one filter predicate.
#[derive(Clone, Debug, PartialEq)]
pub struct FilterStatistics {
    /// Estimated predicate selectivity.
    pub selectivity: Statistic<f64>,
    /// Cardinality before applying the filter.
    pub input_cardinality: Statistic<u64>,
    /// Cardinality after applying the filter.
    pub cardinality: Statistic<u64>,
    /// Modeled filter cost.
    pub cost: Statistic<u64>,
}

/// A statement plan together with estimates that do not alter that plan.
#[derive(Clone, Debug, PartialEq)]
pub struct PlannedStatement {
    pub plan: StatementPlan,
    pub statistics: PlanStatistics,
}

/// Collects observable estimates without changing the statement plan.
pub fn plan_statistics<S: Statistics + ?Sized>(
    statistics_provider: &S,
    statement: &StatementPlan,
) -> Result<PlanStatistics> {
    let mut statistics = PlanStatistics::default();

    match statement {
        StatementPlan::Query(query) | StatementPlan::Insert { source: query, .. } => {
            collect_query(statistics_provider, query, &mut statistics)?;
        }
        StatementPlan::CreateTable {
            source: Some(query),
            ..
        } => collect_query(statistics_provider, query, &mut statistics)?,
        StatementPlan::Update {
            table_name,
            selection,
            ..
        }
        | StatementPlan::Delete {
            table_name,
            selection,
        } => {
            let input = collect_table(statistics_provider, table_name, &mut statistics)?;
            if let Some(expr) = selection {
                collect_filter(
                    statistics_provider,
                    Some(table_name),
                    expr,
                    input,
                    &mut statistics,
                )?;
            }
        }
        _ => {}
    }

    Ok(statistics)
}

/// Collects estimates from a query plan.
fn collect_query<S: Statistics + ?Sized>(
    storage: &S,
    query: &QueryPlan,
    statistics: &mut PlanStatistics,
) -> Result<()> {
    if let Some(project) = query.project() {
        collect_project_input(storage, &project.input, statistics)?;
    }

    Ok(())
}

/// Collects estimates from a project input.
fn collect_project_input<S: Statistics + ?Sized>(
    storage: &S,
    input: &ProjectInputPlan,
    statistics: &mut PlanStatistics,
) -> Result<()> {
    match input {
        ProjectInputPlan::Source(source) => {
            collect_source(storage, source, statistics)?;
        }
        ProjectInputPlan::Filter(FilterPlan { input, expr }) => {
            collect_filter_input(storage, input, statistics)?;
            if let FilterInputPlan::Source(SourcePlan::Table(table)) = input
                && table.access == TableAccessPlan::FullScan
                && let Some(input) = find_table_cardinality(&table.name, statistics)
            {
                collect_filter(storage, Some(&table.name), expr, input, statistics)?;
            } else {
                collect_fallback_filter(
                    expr,
                    Statistic::Estimated(DEFAULT_FULL_SCAN_CARDINALITY),
                    statistics,
                );
            }
        }
        ProjectInputPlan::Aggregation(aggregation) => {
            collect_aggregation_input(storage, &aggregation.input, statistics)?;
        }
        ProjectInputPlan::Having(having) => {
            collect_aggregation_input(storage, &having.input.input, statistics)?;
        }
        ProjectInputPlan::InnerJoin(join) => collect_inner_join(storage, join, statistics)?,
        ProjectInputPlan::LeftOuterJoin(join) => {
            collect_left_outer_join(storage, join, statistics)?;
        }
    }

    Ok(())
}

/// Collects estimates from an aggregation input.
fn collect_aggregation_input<S: Statistics + ?Sized>(
    storage: &S,
    input: &crate::plan::AggregationInputPlan,
    statistics: &mut PlanStatistics,
) -> Result<()> {
    match input {
        crate::plan::AggregationInputPlan::Source(source) => {
            collect_source(storage, source, statistics)
        }
        crate::plan::AggregationInputPlan::Filter(FilterPlan { input, expr }) => {
            collect_filter_input(storage, input, statistics)?;
            if let FilterInputPlan::Source(SourcePlan::Table(table)) = input
                && table.access == TableAccessPlan::FullScan
                && let Some(input) = find_table_cardinality(&table.name, statistics)
            {
                collect_filter(storage, Some(&table.name), expr, input, statistics)?;
            } else {
                collect_fallback_filter(
                    expr,
                    Statistic::Estimated(DEFAULT_FULL_SCAN_CARDINALITY),
                    statistics,
                );
            }
            Ok(())
        }
        crate::plan::AggregationInputPlan::InnerJoin(join) => {
            collect_inner_join(storage, join, statistics)
        }
        crate::plan::AggregationInputPlan::LeftOuterJoin(join) => {
            collect_left_outer_join(storage, join, statistics)
        }
    }
}

/// Collects estimates from a filter input.
fn collect_filter_input<S: Statistics + ?Sized>(
    storage: &S,
    input: &FilterInputPlan,
    statistics: &mut PlanStatistics,
) -> Result<()> {
    match input {
        FilterInputPlan::Source(source) => collect_source(storage, source, statistics),
        FilterInputPlan::InnerJoin(join) => collect_inner_join(storage, join, statistics),
        FilterInputPlan::LeftOuterJoin(join) => collect_left_outer_join(storage, join, statistics),
    }
}

/// Traverses an inner join for observable scan estimates.
fn collect_inner_join<S: Statistics + ?Sized>(
    storage: &S,
    join: &crate::plan::InnerJoinPlan,
    statistics: &mut PlanStatistics,
) -> Result<()> {
    match &join.input {
        crate::plan::InnerJoinInputPlan::NestedLoop(join) => {
            collect_nested_loop(storage, &join.input, statistics)?;
            collect_source(storage, &join.right, statistics)?;
        }
        crate::plan::InnerJoinInputPlan::Hash(join) => {
            collect_hash_input(storage, &join.input, statistics)?;
            collect_source(storage, &join.right, statistics)?;
        }
        crate::plan::InnerJoinInputPlan::Condition(condition) => {
            collect_join_condition_input(storage, &condition.input, statistics)?;
        }
    }
    Ok(())
}

/// Traverses a left outer join for observable scan estimates.
fn collect_left_outer_join<S: Statistics + ?Sized>(
    storage: &S,
    join: &crate::plan::LeftOuterJoinPlan,
    statistics: &mut PlanStatistics,
) -> Result<()> {
    match &join.input {
        crate::plan::LeftOuterJoinInputPlan::NestedLoop(join) => {
            collect_nested_loop(storage, &join.input, statistics)?;
            collect_source(storage, &join.right, statistics)?;
        }
        crate::plan::LeftOuterJoinInputPlan::Hash(join) => {
            collect_hash_input(storage, &join.input, statistics)?;
            collect_source(storage, &join.right, statistics)?;
        }
        crate::plan::LeftOuterJoinInputPlan::Condition(condition) => {
            collect_join_condition_input(storage, &condition.input, statistics)?;
        }
    }
    Ok(())
}

/// Traverses a nested-loop join input.
fn collect_nested_loop<S: Statistics + ?Sized>(
    storage: &S,
    input: &crate::plan::NestedLoopJoinInputPlan,
    statistics: &mut PlanStatistics,
) -> Result<()> {
    match input {
        crate::plan::NestedLoopJoinInputPlan::Source(source) => {
            collect_source(storage, source, statistics)
        }
        crate::plan::NestedLoopJoinInputPlan::InnerJoin(join) => {
            collect_inner_join(storage, join, statistics)
        }
        crate::plan::NestedLoopJoinInputPlan::LeftOuterJoin(join) => {
            collect_left_outer_join(storage, join, statistics)
        }
    }
}

/// Traverses a hash join input.
fn collect_hash_input<S: Statistics + ?Sized>(
    storage: &S,
    input: &crate::plan::HashJoinInputPlan,
    statistics: &mut PlanStatistics,
) -> Result<()> {
    match input {
        crate::plan::HashJoinInputPlan::Source(source) => {
            collect_source(storage, source, statistics)
        }
        crate::plan::HashJoinInputPlan::InnerJoin(join) => {
            collect_inner_join(storage, join, statistics)
        }
        crate::plan::HashJoinInputPlan::LeftOuterJoin(join) => {
            collect_left_outer_join(storage, join, statistics)
        }
    }
}

/// Traverses a join-condition input.
fn collect_join_condition_input<S: Statistics + ?Sized>(
    storage: &S,
    input: &crate::plan::JoinConditionInputPlan,
    statistics: &mut PlanStatistics,
) -> Result<()> {
    match input {
        crate::plan::JoinConditionInputPlan::NestedLoop(join) => {
            collect_nested_loop(storage, &join.input, statistics)?;
            collect_source(storage, &join.right, statistics)
        }
        crate::plan::JoinConditionInputPlan::Hash(join) => {
            collect_hash_input(storage, &join.input, statistics)?;
            collect_source(storage, &join.right, statistics)
        }
    }
}

/// Collects estimates from a source plan.
fn collect_source<S: Statistics + ?Sized>(
    storage: &S,
    source: &SourcePlan,
    statistics: &mut PlanStatistics,
) -> Result<()> {
    match source {
        SourcePlan::Table(table) if table.access == TableAccessPlan::FullScan => {
            collect_table(storage, &table.name, statistics)?;
        }
        SourcePlan::Derived(derived) => collect_query(storage, &derived.query, statistics)?,
        SourcePlan::Table(_) | SourcePlan::Series(_) | SourcePlan::Dictionary(_) => {}
    }
    Ok(())
}

/// Records a table scan estimate.
fn collect_table<S: Statistics + ?Sized>(
    storage: &S,
    table_name: &str,
    statistics: &mut PlanStatistics,
) -> Result<Statistic<u64>> {
    let cardinality = match storage.fetch_table_statistics(table_name)?.row_count {
        Statistic::Unknown => Statistic::Estimated(DEFAULT_FULL_SCAN_CARDINALITY),
        statistic => statistic,
    };
    let cost = Statistic::Estimated(cardinality_value(&cardinality));
    statistics.full_scans.push(FullScanStatistics {
        table_name: table_name.to_owned(),
        cardinality: cardinality.clone(),
        cost,
    });
    Ok(cardinality)
}

/// Records a provider-backed filter estimate.
fn collect_filter<S: Statistics + ?Sized>(
    storage: &S,
    table_name: Option<&str>,
    expr: &ExprPlan,
    input_cardinality: Statistic<u64>,
    statistics: &mut PlanStatistics,
) -> Result<()> {
    let selectivity = estimate_selectivity(storage, table_name, expr)?;
    record_filter(selectivity, input_cardinality, statistics);
    Ok(())
}

/// Records a provider-independent filter estimate.
fn collect_fallback_filter(
    expr: &ExprPlan,
    input_cardinality: Statistic<u64>,
    statistics: &mut PlanStatistics,
) {
    record_filter(fallback_selectivity(expr), input_cardinality, statistics);
}

/// Appends a filter estimate to the plan statistics.
fn record_filter(
    selectivity: f64,
    input_cardinality: Statistic<u64>,
    statistics: &mut PlanStatistics,
) {
    let cardinality =
        Statistic::Estimated(scale(cardinality_value(&input_cardinality), selectivity));
    let cost = Statistic::Estimated(cardinality_value(&input_cardinality));
    statistics.filters.push(FilterStatistics {
        selectivity: Statistic::Estimated(selectivity),
        input_cardinality,
        cardinality,
        cost,
    });
}

/// Estimates selectivity without column statistics.
fn fallback_selectivity(expr: &ExprPlan) -> f64 {
    if let ExprPlan::Value(crate::data::Value::Bool(value)) = expr {
        return if *value { 1.0 } else { 0.0 };
    }
    let ExprPlan::BinaryOp { left, op, right } = expr else {
        return DEFAULT_EQUALITY_SELECTIVITY;
    };

    match op {
        BinaryOperator::And => fallback_selectivity(left) * fallback_selectivity(right),
        BinaryOperator::Or => {
            let left = fallback_selectivity(left);
            let right = fallback_selectivity(right);
            left + right - (left * right)
        }
        BinaryOperator::Gt | BinaryOperator::GtEq | BinaryOperator::Lt | BinaryOperator::LtEq => {
            DEFAULT_RANGE_SELECTIVITY
        }
        _ => DEFAULT_EQUALITY_SELECTIVITY,
    }
}

/// Estimates selectivity using an optional statistics provider.
fn estimate_selectivity<S: Statistics + ?Sized>(
    storage: &S,
    table_name: Option<&str>,
    expr: &ExprPlan,
) -> Result<f64> {
    if let ExprPlan::Value(crate::data::Value::Bool(value)) = expr {
        return Ok(if *value { 1.0 } else { 0.0 });
    }
    let ExprPlan::BinaryOp { left, op, right } = expr else {
        return Ok(DEFAULT_EQUALITY_SELECTIVITY);
    };

    match op {
        BinaryOperator::And => Ok(estimate_selectivity(storage, table_name, left)?
            * estimate_selectivity(storage, table_name, right)?),
        BinaryOperator::Or => {
            let left = estimate_selectivity(storage, table_name, left)?;
            let right = estimate_selectivity(storage, table_name, right)?;
            Ok(left + right - (left * right))
        }
        BinaryOperator::Eq => equality_selectivity(storage, table_name, left, right),
        BinaryOperator::Gt | BinaryOperator::GtEq | BinaryOperator::Lt | BinaryOperator::LtEq => {
            range_selectivity(storage, table_name, left, op, right)
        }
        _ => Ok(DEFAULT_EQUALITY_SELECTIVITY),
    }
}

/// Estimates a range predicate from column bounds.
fn range_selectivity<S: Statistics + ?Sized>(
    storage: &S,
    table_name: Option<&str>,
    left: &ExprPlan,
    op: &BinaryOperator,
    right: &ExprPlan,
) -> Result<f64> {
    let (column, value, op) = match (
        column_name(left),
        numeric_expr(right),
        numeric_expr(left),
        column_name(right),
    ) {
        (Some(column), Some(value), _, _) => (column, value, op.clone()),
        (_, _, Some(value), Some(column)) => (column, value, reverse_range_operator(op)),
        _ => return Ok(DEFAULT_RANGE_SELECTIVITY),
    };
    let Some(table_name) = table_name else {
        return Ok(DEFAULT_RANGE_SELECTIVITY);
    };
    let statistics = storage.fetch_column_statistics(table_name, column)?;
    let (
        Statistic::Exact(min) | Statistic::Estimated(min),
        Statistic::Exact(max) | Statistic::Estimated(max),
    ) = (statistics.min_value, statistics.max_value)
    else {
        return Ok(DEFAULT_RANGE_SELECTIVITY);
    };
    let (Some(min), Some(max)) = (numeric_value(&min), numeric_value(&max)) else {
        return Ok(DEFAULT_RANGE_SELECTIVITY);
    };
    if max <= min {
        return Ok(DEFAULT_RANGE_SELECTIVITY);
    }
    let selectivity = match op {
        BinaryOperator::Gt | BinaryOperator::GtEq => (max - value) / (max - min),
        BinaryOperator::Lt | BinaryOperator::LtEq => (value - min) / (max - min),
        _ => return Ok(DEFAULT_RANGE_SELECTIVITY),
    };
    Ok(selectivity.clamp(0.0, 1.0))
}

/// Reverses a range operator when operands are swapped.
fn reverse_range_operator(op: &BinaryOperator) -> BinaryOperator {
    match op {
        BinaryOperator::Gt => BinaryOperator::Lt,
        BinaryOperator::GtEq => BinaryOperator::LtEq,
        BinaryOperator::Lt => BinaryOperator::Gt,
        BinaryOperator::LtEq => BinaryOperator::GtEq,
        _ => op.clone(),
    }
}

/// Estimates equality selectivity from distinct-count statistics.
fn equality_selectivity<S: Statistics + ?Sized>(
    storage: &S,
    table_name: Option<&str>,
    left: &ExprPlan,
    right: &ExprPlan,
) -> Result<f64> {
    let Some(column) = column_name(left).or_else(|| column_name(right)) else {
        return Ok(DEFAULT_EQUALITY_SELECTIVITY);
    };
    let other = if column_name(left).is_some() {
        right
    } else {
        left
    };
    if !is_value(other) {
        return Ok(DEFAULT_EQUALITY_SELECTIVITY);
    }

    let Some(table_name) = table_name else {
        return Ok(DEFAULT_EQUALITY_SELECTIVITY);
    };
    let distinct_count = storage
        .fetch_column_statistics(table_name, column)?
        .distinct_count;
    match distinct_count {
        Statistic::Exact(count) | Statistic::Estimated(count) if count > 0 => {
            Ok(1.0 / count as f64)
        }
        Statistic::Exact(_) | Statistic::Estimated(_) | Statistic::Unknown => {
            Ok(DEFAULT_EQUALITY_SELECTIVITY)
        }
    }
}

/// Finds the latest scan estimate for a table.
fn find_table_cardinality(table_name: &str, statistics: &PlanStatistics) -> Option<Statistic<u64>> {
    statistics
        .full_scans
        .iter()
        .rev()
        .find(|scan| scan.table_name == table_name)
        .map(|scan| scan.cardinality.clone())
}

/// Extracts a column name from an expression.
fn column_name(expr: &ExprPlan) -> Option<&str> {
    match expr {
        ExprPlan::Identifier(name) => Some(name),
        ExprPlan::CompoundIdentifier { ident, .. } => Some(ident),
        _ => None,
    }
}

/// Returns whether an expression is a literal value.
fn is_value(expr: &ExprPlan) -> bool {
    matches!(
        expr,
        ExprPlan::Literal(_) | ExprPlan::Value(_) | ExprPlan::TypedString { .. }
    )
}

/// Converts a numeric expression to an estimate input.
fn numeric_expr(expr: &ExprPlan) -> Option<f64> {
    match expr {
        ExprPlan::Value(value) => numeric_value(value),
        ExprPlan::Literal(crate::ast::Literal::Number(value)) => value.to_f64(),
        _ => None,
    }
}

/// Converts a numeric value to `f64`.
fn numeric_value(value: &crate::data::Value) -> Option<f64> {
    use crate::data::Value;

    match value {
        Value::I8(value) => Some(f64::from(*value)),
        Value::I16(value) => Some(f64::from(*value)),
        Value::I32(value) => Some(f64::from(*value)),
        Value::I64(value) => Some(*value as f64),
        Value::I128(value) => Some(*value as f64),
        Value::U8(value) => Some(f64::from(*value)),
        Value::U16(value) => Some(f64::from(*value)),
        Value::U32(value) => Some(f64::from(*value)),
        Value::U64(value) => Some(*value as f64),
        Value::U128(value) => Some(*value as f64),
        Value::F32(value) => Some(f64::from(*value)),
        Value::F64(value) => Some(*value),
        _ => None,
    }
}

/// Resolves unknown cardinality to the default fallback.
fn cardinality_value(statistic: &Statistic<u64>) -> u64 {
    match statistic {
        Statistic::Exact(value) | Statistic::Estimated(value) => *value,
        Statistic::Unknown => DEFAULT_FULL_SCAN_CARDINALITY,
    }
}

/// Applies selectivity to an input cardinality.
fn scale(cardinality: u64, selectivity: f64) -> u64 {
    (cardinality as f64 * selectivity).floor() as u64
}

#[cfg(test)]
mod tests {
    use {
        super::{
            DEFAULT_EQUALITY_SELECTIVITY, PlanStatistics, cardinality_value,
            collect_aggregation_input, collect_filter_input, collect_hash_input,
            collect_inner_join, collect_join_condition_input, collect_left_outer_join,
            collect_nested_loop, collect_project_input, collect_source, column_name,
            estimate_selectivity, fallback_selectivity, numeric_expr, numeric_value,
            plan_statistics, range_selectivity, reverse_range_operator,
        },
        crate::{
            ast::{BinaryOperator, Literal},
            data::Value,
            plan::{
                AggregationInputPlan, AggregationPlan, DictionarySourcePlan, ExprPlan,
                FilterInputPlan, FilterPlan, HashJoinInputPlan, HashJoinPlan, HavingPlan,
                InnerJoinInputPlan, InnerJoinPlan, JoinConditionInputPlan, JoinConditionPlan,
                LeftOuterJoinInputPlan, LeftOuterJoinPlan, NestedLoopJoinInputPlan,
                NestedLoopJoinPlan, ProjectInputPlan, ProjectPlan, ProjectionPlan, QueryPlan,
                SeriesSourcePlan, SourcePlan, StatementPlan, TableAccessPlan, TableAliasPlan,
                TableSourcePlan, ValuesPlan,
            },
            result::Result,
            store::{ColumnStatistics, Statistic, Statistics, TableStatistics},
        },
    };

    struct FixedStatistics;

    struct ColumnRangeStatistics(ColumnStatistics);

    struct FailingTableStatistics;

    impl Statistics for FailingTableStatistics {
        fn fetch_table_statistics(&self, _table_name: &str) -> Result<TableStatistics> {
            Err(crate::result::Error::StorageMsg(
                "statistics unavailable".to_owned(),
            ))
        }
    }

    struct FailingColumnStatistics;

    impl Statistics for FailingColumnStatistics {
        fn fetch_table_statistics(&self, _table_name: &str) -> Result<TableStatistics> {
            Ok(TableStatistics {
                row_count: Statistic::Exact(1),
                ..TableStatistics::default()
            })
        }

        fn fetch_column_statistics(
            &self,
            _table_name: &str,
            _column_name: &str,
        ) -> Result<ColumnStatistics> {
            Err(crate::result::Error::StorageMsg(
                "statistics unavailable".to_owned(),
            ))
        }
    }

    impl Statistics for ColumnRangeStatistics {
        fn fetch_column_statistics(
            &self,
            _table_name: &str,
            _column_name: &str,
        ) -> Result<ColumnStatistics> {
            Ok(self.0.clone())
        }
    }

    impl Statistics for FixedStatistics {
        fn fetch_table_statistics(&self, _table_name: &str) -> Result<TableStatistics> {
            Ok(TableStatistics {
                row_count: Statistic::Exact(100),
                ..TableStatistics::default()
            })
        }

        fn fetch_column_statistics(
            &self,
            _table_name: &str,
            column_name: &str,
        ) -> Result<ColumnStatistics> {
            Ok(ColumnStatistics {
                distinct_count: match column_name {
                    "known" => Statistic::Exact(20),
                    _ => Statistic::Unknown,
                },
                min_value: Statistic::Exact(Value::I64(0)),
                max_value: Statistic::Exact(Value::I64(100)),
                ..ColumnStatistics::default()
            })
        }
    }

    /// Builds an identifier expression for estimator tests.
    fn column(name: &str) -> ExprPlan {
        ExprPlan::Identifier(name.to_owned())
    }

    /// Builds an integer value expression for estimator tests.
    fn value(value: i64) -> ExprPlan {
        ExprPlan::Value(Value::I64(value))
    }

    /// Builds a binary expression for estimator tests.
    fn binary(left: ExprPlan, op: BinaryOperator, right: ExprPlan) -> ExprPlan {
        ExprPlan::BinaryOp {
            left: Box::new(left),
            op,
            right: Box::new(right),
        }
    }

    /// Builds a full-scan table source for planner tests.
    fn table(name: &str) -> SourcePlan {
        SourcePlan::Table(TableSourcePlan {
            name: name.to_owned(),
            alias: None,
            access: TableAccessPlan::FullScan,
        })
    }

    /// Builds a nested-loop join wrapper for planner tests.
    fn nested(input: NestedLoopJoinInputPlan) -> NestedLoopJoinPlan {
        NestedLoopJoinPlan {
            input,
            right: table("right"),
        }
    }

    /// Builds a hash join wrapper for planner tests.
    fn hash(input: HashJoinInputPlan) -> HashJoinPlan {
        HashJoinPlan {
            input,
            right: table("right"),
            input_key: ExprPlan::Value(Value::Bool(true)),
            right_key: ExprPlan::Value(Value::Bool(true)),
            right_filter: None,
        }
    }

    /// Builds a representative inner join.
    fn inner() -> InnerJoinPlan {
        InnerJoinPlan {
            input: InnerJoinInputPlan::NestedLoop(nested(NestedLoopJoinInputPlan::Source(table(
                "left",
            )))),
        }
    }

    /// Builds a representative left outer join.
    fn left() -> LeftOuterJoinPlan {
        LeftOuterJoinPlan {
            input: LeftOuterJoinInputPlan::NestedLoop(nested(NestedLoopJoinInputPlan::Source(
                table("left"),
            ))),
        }
    }

    /// Builds a project query wrapper for planner tests.
    fn project(input: ProjectInputPlan) -> QueryPlan {
        QueryPlan::Project(ProjectPlan {
            input,
            projection: ProjectionPlan::SelectItems(Vec::new()),
        })
    }

    #[test]
    /// Covers the supported plan input shapes with observable scan output.
    fn collects_statistics_for_plan_shapes() {
        let provider = FixedStatistics;
        let mut stats = PlanStatistics::default();
        let nested_source = nested(NestedLoopJoinInputPlan::Source(table("nested")));
        let hash_source = hash(HashJoinInputPlan::Source(table("hash")));
        for input in [
            NestedLoopJoinInputPlan::Source(table("n")),
            NestedLoopJoinInputPlan::InnerJoin(Box::new(inner())),
            NestedLoopJoinInputPlan::LeftOuterJoin(Box::new(left())),
        ] {
            collect_nested_loop(&provider, &input, &mut stats).unwrap();
        }
        for input in [
            HashJoinInputPlan::Source(table("h")),
            HashJoinInputPlan::InnerJoin(Box::new(inner())),
            HashJoinInputPlan::LeftOuterJoin(Box::new(left())),
        ] {
            collect_hash_input(&provider, &input, &mut stats).unwrap();
        }
        for input in [
            JoinConditionInputPlan::NestedLoop(nested_source.clone()),
            JoinConditionInputPlan::Hash(hash_source.clone()),
        ] {
            collect_join_condition_input(&provider, &input, &mut stats).unwrap();
        }
        for join in [
            InnerJoinPlan {
                input: InnerJoinInputPlan::NestedLoop(nested_source.clone()),
            },
            InnerJoinPlan {
                input: InnerJoinInputPlan::Hash(hash_source.clone()),
            },
            InnerJoinPlan {
                input: InnerJoinInputPlan::Condition(JoinConditionPlan {
                    input: JoinConditionInputPlan::NestedLoop(nested_source.clone()),
                    expr: ExprPlan::Value(Value::Bool(true)),
                }),
            },
        ] {
            collect_inner_join(&provider, &join, &mut stats).unwrap();
        }
        for join in [
            LeftOuterJoinPlan {
                input: LeftOuterJoinInputPlan::NestedLoop(nested_source.clone()),
            },
            LeftOuterJoinPlan {
                input: LeftOuterJoinInputPlan::Hash(hash_source.clone()),
            },
            LeftOuterJoinPlan {
                input: LeftOuterJoinInputPlan::Condition(JoinConditionPlan {
                    input: JoinConditionInputPlan::Hash(hash_source.clone()),
                    expr: ExprPlan::Value(Value::Bool(true)),
                }),
            },
        ] {
            collect_left_outer_join(&provider, &join, &mut stats).unwrap();
        }

        let filter = FilterPlan {
            input: FilterInputPlan::Source(table("filter")),
            expr: ExprPlan::Value(Value::Bool(true)),
        };
        for input in [
            AggregationInputPlan::Source(table("a")),
            AggregationInputPlan::Filter(filter.clone()),
            AggregationInputPlan::InnerJoin(Box::new(inner())),
            AggregationInputPlan::LeftOuterJoin(Box::new(left())),
        ] {
            collect_aggregation_input(&provider, &input, &mut stats).unwrap();
        }
        for input in [
            FilterInputPlan::Source(table("f")),
            FilterInputPlan::InnerJoin(Box::new(inner())),
            FilterInputPlan::LeftOuterJoin(Box::new(left())),
        ] {
            collect_filter_input(&provider, &input, &mut stats).unwrap();
        }
        let aggregation = AggregationPlan {
            input: AggregationInputPlan::Source(table("g")),
            group_by: Vec::new(),
            aggregate_slots: Vec::new(),
        };
        for input in [
            ProjectInputPlan::Source(table("p")),
            ProjectInputPlan::Aggregation(aggregation.clone()),
            ProjectInputPlan::Having(HavingPlan {
                input: aggregation.clone(),
                expr: ExprPlan::Value(Value::Bool(true)),
            }),
            ProjectInputPlan::InnerJoin(Box::new(inner())),
            ProjectInputPlan::LeftOuterJoin(Box::new(left())),
        ] {
            collect_project_input(&provider, &input, &mut stats).unwrap();
        }
        let alias = TableAliasPlan {
            name: "source".to_owned(),
            columns: Vec::new(),
        };
        for source in [
            SourcePlan::Table(TableSourcePlan {
                name: "indexed".to_owned(),
                alias: None,
                access: TableAccessPlan::PrimaryKey {
                    expr: ExprPlan::Value(Value::Bool(true)),
                },
            }),
            SourcePlan::Series(SeriesSourcePlan {
                alias: alias.clone(),
                size: ExprPlan::Value(Value::I64(1)),
            }),
            SourcePlan::Dictionary(DictionarySourcePlan {
                dictionary: crate::ast::Dictionary::GlueTables,
                alias,
            }),
        ] {
            collect_source(&provider, &source, &mut stats).unwrap();
        }
        let statements = [
            StatementPlan::Query(project(ProjectInputPlan::Source(table("q")))),
            StatementPlan::Insert {
                table_name: "t".to_owned(),
                columns: Vec::new(),
                source: project(ProjectInputPlan::Source(table("q"))),
            },
            StatementPlan::CreateTable {
                if_not_exists: false,
                name: "t".to_owned(),
                columns: None,
                source: Some(Box::new(project(ProjectInputPlan::Source(table("q"))))),
                engine: None,
                foreign_keys: Vec::new(),
                comment: None,
            },
            StatementPlan::Update {
                table_name: "t".to_owned(),
                assignments: Vec::new(),
                selection: None,
            },
            StatementPlan::Delete {
                table_name: "t".to_owned(),
                selection: None,
            },
            StatementPlan::ShowColumns {
                table_name: "t".to_owned(),
            },
        ];
        for statement in statements {
            plan_statistics(&provider, &statement).unwrap();
        }
        assert!(!stats.full_scans.is_empty());
        assert!(
            plan_statistics(
                &provider,
                &StatementPlan::Query(QueryPlan::Values(ValuesPlan(Vec::new())))
            )
            .unwrap()
            .full_scans
            .is_empty()
        );
    }

    #[test]
    /// Verifies provider-backed and fallback selectivity formulas.
    fn estimates_equality_range_and_boolean_predicates() {
        let statistics = FixedStatistics;
        let equality = binary(column("known"), BinaryOperator::Eq, value(1));
        let unknown = binary(column("unknown"), BinaryOperator::Eq, value(1));
        let range = binary(
            column("known"),
            BinaryOperator::Gt,
            ExprPlan::Literal(Literal::Number(1.into())),
        );

        assert_eq!(
            estimate_selectivity(&statistics, Some("items"), &equality).unwrap(),
            1.0 / 20.0
        );
        assert_eq!(
            estimate_selectivity(&statistics, Some("items"), &unknown).unwrap(),
            DEFAULT_EQUALITY_SELECTIVITY
        );
        assert_eq!(
            estimate_selectivity(&statistics, Some("items"), &range).unwrap(),
            0.99
        );
        assert_eq!(
            estimate_selectivity(
                &statistics,
                Some("items"),
                &binary(column("known"), BinaryOperator::Lt, value(-1))
            )
            .unwrap(),
            0.0
        );
        assert_eq!(
            estimate_selectivity(
                &statistics,
                Some("items"),
                &binary(value(25), BinaryOperator::Lt, column("known"))
            )
            .unwrap(),
            0.75
        );
        assert_eq!(
            estimate_selectivity(&statistics, None, &range).unwrap(),
            1.0 / 3.0
        );
        assert_eq!(
            estimate_selectivity(
                &statistics,
                None,
                &binary(column("known"), BinaryOperator::Eq, value(1)),
            )
            .unwrap(),
            DEFAULT_EQUALITY_SELECTIVITY
        );
        assert_eq!(
            estimate_selectivity(
                &statistics,
                Some("items"),
                &ExprPlan::Value(Value::Bool(true))
            )
            .unwrap(),
            1.0
        );
        assert_eq!(
            estimate_selectivity(
                &statistics,
                Some("items"),
                &ExprPlan::Value(Value::Bool(false))
            )
            .unwrap(),
            0.0
        );

        let and = binary(equality.clone(), BinaryOperator::And, unknown.clone());
        let or = binary(equality, BinaryOperator::Or, unknown);
        assert_eq!(
            estimate_selectivity(&statistics, Some("items"), &and).unwrap(),
            (1.0 / 20.0) * DEFAULT_EQUALITY_SELECTIVITY
        );
        assert_eq!(
            estimate_selectivity(&statistics, Some("items"), &or).unwrap(),
            (1.0 / 20.0) + DEFAULT_EQUALITY_SELECTIVITY
                - ((1.0 / 20.0) * DEFAULT_EQUALITY_SELECTIVITY)
        );

        let fallback_equality = binary(column("unknown"), BinaryOperator::Eq, value(1));
        let fallback_range = binary(column("unknown"), BinaryOperator::Gt, value(1));
        assert_eq!(fallback_selectivity(&fallback_range), 1.0 / 3.0);
        assert_eq!(
            fallback_selectivity(&binary(
                fallback_equality.clone(),
                BinaryOperator::And,
                fallback_range.clone(),
            )),
            DEFAULT_EQUALITY_SELECTIVITY * (1.0 / 3.0)
        );
        assert_eq!(
            fallback_selectivity(&binary(
                fallback_equality,
                BinaryOperator::Or,
                fallback_range,
            )),
            DEFAULT_EQUALITY_SELECTIVITY + (1.0 / 3.0)
                - (DEFAULT_EQUALITY_SELECTIVITY * (1.0 / 3.0))
        );

        let mut estimates = PlanStatistics::default();
        plan_statistics(
            &statistics,
            &StatementPlan::Update {
                table_name: "items".to_owned(),
                assignments: Vec::new(),
                selection: Some(ExprPlan::Value(Value::Bool(true))),
            },
        )
        .unwrap();
        let filter = FilterPlan {
            input: FilterInputPlan::InnerJoin(Box::new(inner())),
            expr: ExprPlan::Value(Value::Bool(true)),
        };
        collect_project_input(
            &statistics,
            &ProjectInputPlan::Filter(filter.clone()),
            &mut estimates,
        )
        .unwrap();
        collect_aggregation_input(
            &statistics,
            &AggregationInputPlan::Filter(filter),
            &mut estimates,
        )
        .unwrap();
        assert_eq!(estimates.filters.len(), 2);

        for expr in [
            ExprPlan::Identifier("id".to_owned()),
            binary(value(1), BinaryOperator::NotEq, value(2)),
        ] {
            assert_eq!(
                estimate_selectivity(&statistics, Some("items"), &expr).unwrap(),
                DEFAULT_EQUALITY_SELECTIVITY
            );
        }
        assert_eq!(numeric_expr(&ExprPlan::Value(Value::I64(1))), Some(1.0));
        assert_eq!(numeric_expr(&ExprPlan::Value(Value::Bool(true))), None);
        assert_eq!(
            range_selectivity(
                &statistics,
                Some("items"),
                &value(1),
                &BinaryOperator::Eq,
                &column("known"),
            )
            .unwrap(),
            1.0 / 3.0
        );
        assert_eq!(
            reverse_range_operator(&BinaryOperator::Eq),
            BinaryOperator::Eq
        );
        for value in [
            Value::I8(1),
            Value::I16(1),
            Value::I32(1),
            Value::I64(1),
            Value::I128(1),
            Value::U8(1),
            Value::U16(1),
            Value::U32(1),
            Value::U64(1),
            Value::U128(1),
            Value::F32(1.0),
            Value::F64(1.0),
        ] {
            assert!(numeric_value(&value).is_some());
        }
        for op in [
            BinaryOperator::Gt,
            BinaryOperator::GtEq,
            BinaryOperator::Lt,
            BinaryOperator::LtEq,
        ] {
            let _ = reverse_range_operator(&op);
        }
        assert_eq!(cardinality_value(&Statistic::Unknown), 1_000);
        assert_eq!(
            fallback_selectivity(&ExprPlan::Identifier("id".to_owned())),
            DEFAULT_EQUALITY_SELECTIVITY
        );
        assert_eq!(
            estimate_selectivity(
                &statistics,
                Some("items"),
                &binary(value(1), BinaryOperator::Eq, column("known"))
            )
            .unwrap(),
            1.0 / 20.0
        );
        assert_eq!(
            column_name(&ExprPlan::CompoundIdentifier {
                alias: "items".to_owned(),
                ident: "known".to_owned()
            }),
            Some("known")
        );
        assert_eq!(
            estimate_selectivity(
                &statistics,
                Some("items"),
                &binary(value(1), BinaryOperator::Eq, value(2)),
            )
            .unwrap(),
            DEFAULT_EQUALITY_SELECTIVITY
        );
        assert_eq!(
            estimate_selectivity(
                &statistics,
                Some("items"),
                &binary(column("known"), BinaryOperator::Eq, column("other")),
            )
            .unwrap(),
            DEFAULT_EQUALITY_SELECTIVITY
        );
        assert_eq!(
            estimate_selectivity(
                &statistics,
                Some("items"),
                &binary(
                    column("known"),
                    BinaryOperator::Eq,
                    ExprPlan::TypedString {
                        data_type: crate::ast::DataType::Int,
                        value: "1".to_owned(),
                    },
                ),
            )
            .unwrap(),
            1.0 / 20.0
        );
        assert_eq!(
            range_selectivity(
                &statistics,
                Some("items"),
                &value(1),
                &BinaryOperator::Gt,
                &value(2),
            )
            .unwrap(),
            1.0 / 3.0
        );
        for column_statistics in [
            ColumnStatistics::default(),
            ColumnStatistics {
                min_value: Statistic::Exact(Value::Str("a".to_owned())),
                max_value: Statistic::Exact(Value::Str("z".to_owned())),
                ..ColumnStatistics::default()
            },
            ColumnStatistics {
                min_value: Statistic::Exact(Value::I64(10)),
                max_value: Statistic::Exact(Value::I64(1)),
                ..ColumnStatistics::default()
            },
        ] {
            assert_eq!(
                range_selectivity(
                    &ColumnRangeStatistics(column_statistics),
                    Some("items"),
                    &column("known"),
                    &BinaryOperator::Gt,
                    &value(1),
                )
                .unwrap(),
                1.0 / 3.0
            );
        }
        let derived = SourcePlan::Derived(crate::plan::DerivedSourcePlan {
            query: Box::new(project(ProjectInputPlan::Source(table("inner")))),
            alias: TableAliasPlan {
                name: "inner".to_owned(),
                columns: Vec::new(),
            },
        });
        collect_source(&statistics, &derived, &mut estimates).unwrap();
        assert!(
            plan_statistics(
                &FailingTableStatistics,
                &StatementPlan::Update {
                    table_name: "items".to_owned(),
                    assignments: Vec::new(),
                    selection: Some(ExprPlan::Value(Value::Bool(true))),
                }
            )
            .is_err()
        );
        assert!(
            plan_statistics(
                &FailingColumnStatistics,
                &StatementPlan::Update {
                    table_name: "items".to_owned(),
                    assignments: Vec::new(),
                    selection: Some(binary(column("id"), BinaryOperator::Eq, value(1))),
                }
            )
            .is_err()
        );
    }
}
