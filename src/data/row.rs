use {
    crate::{
        data::{schema::ColumnDefExt, value::TryFromLiteral, Value},
        evaluate,
        result::Result,
        store::Store,
    },
    serde::{Deserialize, Serialize},
    sqlparser::ast::{ColumnDef, Expr, Ident},
    std::fmt::Debug,
    thiserror::Error,
};

#[derive(Error, Serialize, Debug, PartialEq)]
pub enum RowError {
    #[error("lack of required column: {0}")]
    LackOfRequiredColumn(String),

    #[error("literals does not fit to columns")]
    LackOfRequiredValue(String),

    #[error("literals have more values than target columns")]
    TooManyValues,

    #[error("conflict! row cannot be empty")]
    ConflictOnEmptyRow,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Row(pub Vec<Value>);

impl Row {
    pub fn get_value(&self, index: usize) -> Option<&Value> {
        self.0.get(index)
    }

    pub fn take_first_value(self) -> Result<Value> {
        self.0
            .into_iter()
            .next()
            .ok_or_else(|| RowError::ConflictOnEmptyRow.into())
    }

    pub async fn new<T: 'static + Debug>(
        storage: &dyn Store<T>,
        column_defs: &[ColumnDef],
        columns: &[Ident],
        values: &[Expr],
    ) -> Result<Self> {
        // FAIL: No mut
        if values.len() > column_defs.len() {
            return Err(RowError::TooManyValues.into());
        }

        let column_defs = column_defs.into_iter().enumerate();
        let mut output_values: Vec<Value> = Vec::new();
        for (index, column_def) in column_defs {
            let ColumnDef {
                name, data_type, ..
            } = column_def;
            let name = name.to_string();

            let index = match columns.len() {
                0 => Some(index),
                _ => columns.iter().position(|target| target.value == name),
            };

            let default = column_def.get_default();
            let nullable = column_def.is_nullable();
            let expr = match (index, default) {
                (Some(index), _) => values
                    .get(index)
                    .ok_or_else(|| RowError::LackOfRequiredValue(name.clone())),
                (None, Some(expr)) => Ok(expr),
                (None, _) => {
                    if nullable {
                        output_values.push(Value::Null);
                        continue;
                    } else {
                        Err(RowError::LackOfRequiredColumn(name.clone()))
                    }
                }
            }?;

            let evaluated = evaluate(storage, None, None, expr, false).await?;
            let value: Value = Value::try_from_evaluated(data_type, evaluated)?;

            value.validate_null(nullable)?;
            output_values.push(value);
        }
        Ok(Row(output_values))
    }

    pub fn validate(&self, column_defs: &[ColumnDef]) -> Result<()> {
        let items = column_defs
            .iter()
            .enumerate()
            .filter_map(|(index, column_def)| {
                let value = self.get_value(index);

                value.map(|v| (v, column_def))
            });

        for (value, column_def) in items {
            let ColumnDef { data_type, .. } = column_def;
            let nullable = column_def.is_nullable();

            value.validate_type(data_type)?;
            value.validate_null(nullable)?;
        }

        Ok(())
    }
}
