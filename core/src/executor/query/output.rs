use crate::plan::{
    DistinctInputPlan, DistinctPlan, LimitInputPlan, LimitPlan, OffsetInputPlan, OffsetPlan,
    ProjectPlan, QueryPlan, ValuesPlan,
};

pub(crate) enum OutputBody<'a> {
    Project(&'a ProjectPlan),
    Values(&'a ValuesPlan),
}

pub(crate) fn body(query: &QueryPlan) -> OutputBody<'_> {
    match query {
        QueryPlan::Project(project) => OutputBody::Project(project),
        QueryPlan::Values(values) => OutputBody::Values(values),
        QueryPlan::SelectOrderBy(order_by) => OutputBody::Project(&order_by.input),
        QueryPlan::ValuesOrderBy(order_by) => OutputBody::Values(&order_by.input),
        QueryPlan::Distinct(distinct) => distinct_body(distinct),
        QueryPlan::Offset(offset) => offset_body(offset),
        QueryPlan::Limit(LimitPlan { input, .. }) => match input {
            LimitInputPlan::Project(project) => OutputBody::Project(project),
            LimitInputPlan::Values(values) => OutputBody::Values(values),
            LimitInputPlan::SelectOrderBy(order_by) => OutputBody::Project(&order_by.input),
            LimitInputPlan::ValuesOrderBy(order_by) => OutputBody::Values(&order_by.input),
            LimitInputPlan::Distinct(distinct) => distinct_body(distinct),
            LimitInputPlan::Offset(offset) => offset_body(offset),
        },
    }
}

fn offset_body(offset: &OffsetPlan) -> OutputBody<'_> {
    match &offset.input {
        OffsetInputPlan::Project(project) => OutputBody::Project(project),
        OffsetInputPlan::Values(values) => OutputBody::Values(values),
        OffsetInputPlan::SelectOrderBy(order_by) => OutputBody::Project(&order_by.input),
        OffsetInputPlan::ValuesOrderBy(order_by) => OutputBody::Values(&order_by.input),
        OffsetInputPlan::Distinct(distinct) => distinct_body(distinct),
    }
}

fn distinct_body(distinct: &DistinctPlan) -> OutputBody<'_> {
    match &distinct.input {
        DistinctInputPlan::Project(project) => OutputBody::Project(project),
        DistinctInputPlan::SelectOrderBy(order_by) => OutputBody::Project(&order_by.input),
    }
}
