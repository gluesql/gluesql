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
    pub full_scans: Vec<FullScanStatistics>,
    pub filters: Vec<FilterStatistics>,
}

/// Cardinality and modeled cost for one full table scan.
#[derive(Clone, Debug, PartialEq)]
pub struct FullScanStatistics {
    pub table_name: String,
    pub cardinality: Statistic<u64>,
    pub cost: Statistic<u64>,
}

/// Cardinality and modeled cost for one filter predicate.
#[derive(Clone, Debug, PartialEq)]
pub struct FilterStatistics {
    pub selectivity: Statistic<f64>,
    pub input_cardinality: Statistic<u64>,
    pub cardinality: Statistic<u64>,
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
    storage: &S,
    statement: &StatementPlan,
) -> Result<PlanStatistics> {
    let mut statistics = PlanStatistics::default();

    match statement {
        StatementPlan::Query(query) | StatementPlan::Insert { source: query, .. } => {
            collect_query(storage, query, &mut statistics)?;
        }
        StatementPlan::CreateTable {
            source: Some(query),
            ..
        } => collect_query(storage, query, &mut statistics)?,
        StatementPlan::Update {
            table_name,
            selection,
            ..
        }
        | StatementPlan::Delete {
            table_name,
            selection,
        } => {
            let input = collect_table(storage, table_name, &mut statistics)?;
            if let Some(expr) = selection {
                collect_filter(storage, Some(table_name), expr, input, &mut statistics)?;
            }
        }
        _ => {}
    }

    Ok(statistics)
}

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
            } else if matches!(input, FilterInputPlan::Source(SourcePlan::Derived(_))) {
                collect_filter(
                    storage,
                    None,
                    expr,
                    Statistic::Estimated(DEFAULT_FULL_SCAN_CARDINALITY),
                    statistics,
                )?;
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
            } else if matches!(input, FilterInputPlan::Source(SourcePlan::Derived(_))) {
                collect_filter(
                    storage,
                    None,
                    expr,
                    Statistic::Estimated(DEFAULT_FULL_SCAN_CARDINALITY),
                    statistics,
                )?;
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

fn collect_filter<S: Statistics + ?Sized>(
    storage: &S,
    table_name: Option<&str>,
    expr: &ExprPlan,
    input_cardinality: Statistic<u64>,
    statistics: &mut PlanStatistics,
) -> Result<()> {
    let selectivity = estimate_selectivity(storage, table_name, expr)?;
    let cardinality =
        Statistic::Estimated(scale(cardinality_value(&input_cardinality), selectivity));
    let cost = Statistic::Estimated(cardinality_value(&input_cardinality));
    statistics.filters.push(FilterStatistics {
        selectivity: Statistic::Estimated(selectivity),
        input_cardinality,
        cardinality,
        cost,
    });
    Ok(())
}

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

fn reverse_range_operator(op: &BinaryOperator) -> BinaryOperator {
    match op {
        BinaryOperator::Gt => BinaryOperator::Lt,
        BinaryOperator::GtEq => BinaryOperator::LtEq,
        BinaryOperator::Lt => BinaryOperator::Gt,
        BinaryOperator::LtEq => BinaryOperator::GtEq,
        _ => op.clone(),
    }
}

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

fn find_table_cardinality(table_name: &str, statistics: &PlanStatistics) -> Option<Statistic<u64>> {
    statistics
        .full_scans
        .iter()
        .rev()
        .find(|scan| scan.table_name == table_name)
        .map(|scan| scan.cardinality.clone())
}

fn column_name(expr: &ExprPlan) -> Option<&str> {
    match expr {
        ExprPlan::Identifier(name) => Some(name),
        ExprPlan::CompoundIdentifier { ident, .. } => Some(ident),
        _ => None,
    }
}

fn is_value(expr: &ExprPlan) -> bool {
    matches!(
        expr,
        ExprPlan::Literal(_) | ExprPlan::Value(_) | ExprPlan::TypedString { .. }
    )
}

fn numeric_expr(expr: &ExprPlan) -> Option<f64> {
    match expr {
        ExprPlan::Value(value) => numeric_value(value),
        ExprPlan::Literal(crate::ast::Literal::Number(value)) => value.to_f64(),
        _ => None,
    }
}

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

fn cardinality_value(statistic: &Statistic<u64>) -> u64 {
    match statistic {
        Statistic::Exact(value) | Statistic::Estimated(value) => *value,
        Statistic::Unknown => DEFAULT_FULL_SCAN_CARDINALITY,
    }
}

fn scale(cardinality: u64, selectivity: f64) -> u64 {
    (cardinality as f64 * selectivity).floor() as u64
}

#[cfg(test)]
mod tests {
    use {
        super::{DEFAULT_EQUALITY_SELECTIVITY, estimate_selectivity},
        crate::{
            ast::{BinaryOperator, Literal},
            data::Value,
            plan::ExprPlan,
            result::Result,
            store::{ColumnStatistics, Statistic, Statistics},
        },
    };

    struct FixedStatistics;

    impl Statistics for FixedStatistics {
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

    fn column(name: &str) -> ExprPlan {
        ExprPlan::Identifier(name.to_owned())
    }

    fn value(value: i64) -> ExprPlan {
        ExprPlan::Value(Value::I64(value))
    }

    fn binary(left: ExprPlan, op: BinaryOperator, right: ExprPlan) -> ExprPlan {
        ExprPlan::BinaryOp {
            left: Box::new(left),
            op,
            right: Box::new(right),
        }
    }

    #[test]
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
    }
}
