#[path = "native_query/cursor.rs"]
mod cursor;
#[path = "native_query/execution.rs"]
mod execution;
#[path = "native_query/filter.rs"]
mod filter;
#[path = "native_query/scan.rs"]
mod scan;
#[path = "native_query/subquery.rs"]
mod subquery;

use gluesql_core::{
    data::Value,
    plan::{
        FilterInputPlan, FilterPlan, NativeSourcePlan, ProjectInputPlan, QueryPlan, SourcePlan,
        StatementPlan, StatementPlanKind,
    },
    prelude::Payload,
};

fn expect_query(plan: &StatementPlan) -> &QueryPlan {
    let StatementPlanKind::Query(query) = &plan.kind else {
        panic!("expected query: {plan:?}");
    };

    query
}

fn expect_native_source(query: &QueryPlan) -> &NativeSourcePlan {
    let ProjectInputPlan::Source(SourcePlan::Native(native)) = &query.project().unwrap().input
    else {
        panic!("expected native source: {query:?}");
    };

    native
}

fn expect_native_source_under_filter(query: &QueryPlan) -> &NativeSourcePlan {
    let ProjectInputPlan::Filter(FilterPlan {
        input: FilterInputPlan::Source(SourcePlan::Native(native)),
        ..
    }) = &query.project().unwrap().input
    else {
        panic!("expected a remaining filter over a native source: {query:?}");
    };

    native
}

fn select(labels: &[&str], rows: Vec<Vec<Value>>) -> Payload {
    Payload::Select {
        labels: labels.iter().map(|name| (*name).to_owned()).collect(),
        rows,
    }
}
