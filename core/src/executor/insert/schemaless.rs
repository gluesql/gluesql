use {
    super::{InsertError, values},
    crate::{
        data::{Value, value::BTreeMapJsonExt},
        executor::{
            evaluate::{EvaluateError, evaluate_stateless},
            query,
        },
        plan::{QueryPlan, ValuesPlan},
        result::Result,
        store::GStore,
    },
    std::collections::BTreeMap,
};

pub(super) fn fetch_rows<T: GStore>(storage: &T, source: &QueryPlan) -> Result<Vec<Vec<Value>>> {
    let rows_iter: Box<dyn Iterator<Item = Result<Vec<Value>>> + '_> =
        if let Some(rows) = values::execute(source, values_rows)? {
            let rows = rows.map(|row| convert_values(row?.into_values()));

            Box::new(rows)
        } else {
            let rows = query::execute(storage, source, None)?
                .map(|row| convert_query_values(row?.into_values()));

            Box::new(rows)
        };
    let rows = rows_iter.collect::<Result<Vec<Vec<Value>>>>()?;

    Ok(rows)
}

fn values_rows(ValuesPlan(values_list): &ValuesPlan) -> values::EvaluatedRows<'_> {
    let rows = values_list.iter().map(|exprs| {
        exprs
            .iter()
            .map(|expr| evaluate_stateless(None, expr).and_then(Value::try_from))
            .collect::<Result<Vec<_>>>()
    });

    Box::new(rows)
}

fn convert_values(values: Vec<Value>) -> Result<Vec<Value>> {
    if values.len() > 1 {
        return Err(InsertError::OnlySingleValueAcceptedForSchemalessRow(values.len()).into());
    }

    let map = match values.into_iter().next() {
        None => return Err(InsertError::OnlySingleValueAcceptedForSchemalessRow(0).into()),
        Some(Value::Map(map)) => map,
        Some(Value::Str(value)) => BTreeMap::parse_json_object(&value)?,
        Some(value) => return Err(EvaluateError::MapOrStringValueRequired((&value).into()).into()),
    };

    Ok(vec![Value::Map(map)])
}

fn convert_query_values(values: Vec<Value>) -> Result<Vec<Value>> {
    if values.len() > 1 {
        return Err(InsertError::OnlySingleValueAcceptedForSchemalessRow(values.len()).into());
    }

    let map = match values.into_iter().next() {
        None => return Err(InsertError::OnlySingleValueAcceptedForSchemalessRow(0).into()),
        Some(Value::Map(map)) => map,
        Some(Value::Str(value)) => BTreeMap::parse_json_object(&value)?,
        Some(value) => return Err(InsertError::MapTypeValueRequired((&value).into()).into()),
    };

    Ok(vec![Value::Map(map)])
}
