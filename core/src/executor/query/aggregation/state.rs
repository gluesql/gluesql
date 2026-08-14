use {
    super::accumulator::{AggrValue, empty_value},
    crate::{
        data::Value,
        executor::{
            context::{AggregateContext, AggregateValues, RowContext},
            evaluate::evaluate,
        },
        plan::{AggregateExprPlan, AggregateFunctionPlan, CountArgExprPlan},
        result::Result,
        store::GStore,
    },
    std::{collections::HashMap, rc::Rc},
};

struct GroupState<'a> {
    representative: Option<Rc<RowContext<'a>>>,
    values: Vec<Option<AggrValue>>,
}

impl<'a> GroupState<'a> {
    fn new(slot_count: usize, representative: Option<Rc<RowContext<'a>>>) -> Self {
        Self {
            representative,
            values: vec![None; slot_count],
        }
    }
}

pub struct State<'a, T: GStore> {
    storage: &'a T,
    slot_count: usize,
    groups: Vec<GroupState<'a>>,
    group_indexes: HashMap<Vec<Value>, usize>,
}

impl<'a, T: GStore> State<'a, T> {
    pub fn new(storage: &'a T, slot_count: usize, global: bool) -> Self {
        let mut groups = Vec::new();
        let mut group_indexes = HashMap::new();

        if global {
            groups.push(GroupState::new(slot_count, None));
            group_indexes.insert(Vec::new(), 0);
        }

        Self {
            storage,
            slot_count,
            groups,
            group_indexes,
        }
    }

    pub fn apply(&mut self, group: Vec<Value>, context: Rc<RowContext<'a>>) -> usize {
        if let Some(index) = self.group_indexes.get(&group).copied() {
            if self.groups[index].representative.is_none() {
                self.groups[index].representative = Some(context);
            }

            return index;
        }

        let index = self.groups.len();
        self.groups
            .push(GroupState::new(self.slot_count, Some(Rc::clone(&context))));
        self.group_indexes.insert(group, index);

        index
    }

    pub fn accumulate(
        &mut self,
        group_index: usize,
        filter_context: &Rc<RowContext<'a>>,
        slot: usize,
        aggregate: &AggregateExprPlan,
    ) -> Result<()> {
        let value = match &aggregate.func {
            AggregateFunctionPlan::Count(CountArgExprPlan::Wildcard) => {
                if aggregate.distinct {
                    let entries = filter_context.get_all_entries();
                    let values: Vec<Value> = entries.into_iter().map(|(_, value)| value).collect();
                    Value::List(values)
                } else {
                    Value::Null
                }
            }
            AggregateFunctionPlan::Count(CountArgExprPlan::Expr(expr))
            | AggregateFunctionPlan::Sum(expr)
            | AggregateFunctionPlan::Min(expr)
            | AggregateFunctionPlan::Max(expr)
            | AggregateFunctionPlan::Avg(expr)
            | AggregateFunctionPlan::Variance(expr)
            | AggregateFunctionPlan::Stdev(expr) => {
                evaluate(self.storage, Some(filter_context), None, None, expr)?.try_into()?
            }
        };

        if AggrValue::ignores(aggregate, &value) {
            return Ok(());
        }

        let group = self
            .groups
            .get_mut(group_index)
            .expect("group index must exist");
        match group.values[slot].as_mut() {
            Some(aggr_value) => {
                aggr_value.accumulate(&value)?;
            }
            None => {
                group.values[slot] = Some(AggrValue::new(aggregate, &value)?);
            }
        }

        Ok(())
    }

    pub fn export(
        self,
        aggregate_slots: &[AggregateExprPlan],
    ) -> Result<Vec<AggregateContext<'a>>> {
        let groups = self.groups;

        groups
            .into_iter()
            .map(|group| {
                let values = if aggregate_slots.is_empty() {
                    None
                } else {
                    let values = group
                        .values
                        .into_iter()
                        .zip(aggregate_slots.iter())
                        .map(|(value, aggregate)| match value {
                            Some(value) => value.export(),
                            None => Ok(empty_value(aggregate)),
                        })
                        .collect::<Result<Vec<_>>>()?;

                    Some(Rc::new(AggregateValues::new(values)))
                };

                Ok(AggregateContext {
                    aggregated: values,
                    next: group.representative,
                })
            })
            .collect()
    }
}
