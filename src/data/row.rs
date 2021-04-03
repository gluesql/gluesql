use {
    crate::{
        data::{schema::ColumnDefExt, value::TryFromLiteral, Value},
        evaluate,
        result::Result,
        store::Store,
    },
    serde::{Deserialize, Serialize},
    sqlparser::ast::{ColumnDef, DataType, Expr, Ident},
    std::fmt::Debug,
    thiserror::Error,
};

#[cfg(feature = "auto-increment")]
use sqlparser::ast::Value as Literal;

#[derive(Error, Serialize, Debug, PartialEq)]
pub enum RowError {
    #[error("lack of required column: {0}")]
    LackOfRequiredColumn(String),

    #[error("columns and values must match")]
    WrongNumberOfValues,

    #[error("conflict! row cannot be empty")]
    ConflictOnEmptyRow,

    #[error("unreachable")]
    Unreachable,
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
        expressions: &[Expr],
    ) -> Result<Self> {
        Ok(bulk_build_rows_expr(
            storage,
            column_defs,
            columns,
            vec![expressions.to_vec()],
            false,
            false,
        )
        .await?
        .into_iter()
        .next()
        .unwrap())
    }
}

async fn evaluate_expression<T: 'static + Debug>(
    storage: &dyn Store<T>,
    expression: &Expr,
    data_type: &DataType,
) -> Result<Value> {
    Value::try_from_evaluated(
        data_type,
        evaluate(storage, None, None, expression, false).await?,
    )
}

// For macro TODO: find a better way
async fn process_value<T: 'static + Debug>(
    _a: &dyn Store<T>,
    value: Value,
    _b: &DataType,
) -> Result<Value> {
    Ok(value)
}

async fn process_expr<T: 'static + Debug>(
    storage: &dyn Store<T>,
    expression: Expr,
    data_type: &DataType,
) -> Result<Value> {
    evaluate_expression(storage, &expression, data_type).await
}

macro_rules! bulk_build_rows {
    ($type: ty, $name: ident, $processing_function: ident) => {
        pub async fn $name<T: 'static + Debug>(
            storage: &dyn Store<T>,
            column_defs: &[ColumnDef],
            columns: &[Ident],
            rows: Vec<Vec<$type>>,
            do_default: bool,
            do_validate: bool,
        ) -> Result<Vec<Row>> {
            // FAIL: No mut
            let table_columns_count = column_defs.len();
            let selection_columns_count = if columns.len() == 0 {
                table_columns_count
            } else {
                columns.len()
            };

            if rows.iter().any(|row| row.len() != selection_columns_count) {
                return Err(RowError::WrongNumberOfValues.into());
            } // KG: I don't like this

            let mut output_values: Vec<Vec<Value>> = Vec::new();
            output_values.resize(rows.len(), Vec::new());
            for row in &mut output_values {
                row.resize(table_columns_count, Value::Null);
            }
            // This feels slow, how can we initialise better?
            let column_defs_indexed = column_defs.into_iter().enumerate();
            for (column_index, column_def) in column_defs_indexed {
                let ColumnDef {
                    name, data_type, ..
                } = column_def;

                let name = name.to_string();
                let values_index = if columns.len() == 0 {
                    Some(column_index)
                } else {
                    columns.iter().position(|target| target.value == name)
                };

                let expr = i.map(|i| values.get(i));

                #[cfg(feature = "auto-increment")]
                if matches!(expr, None | Some(Some(Expr::Value(Literal::Null))))
                    && column_def.is_auto_incremented()
                {
                    return Ok(Value::Null);
                }

                let default = column_def.get_default();
                let expr = match (expr, default) {
                    (Some(expr), _) => {
                        expr.ok_or_else(|| RowError::LackOfRequiredValue(name.clone()))
                    }
                    (None, Some(expr)) => Ok(expr),
                    (None, _) => Err(RowError::LackOfRequiredColumn(name.clone())),
                }?;

                let nullable = column_def.is_nullable();

                let default = column_def.get_default();
                let failure_value = if do_default && default.is_some() {
                    if let Some(default) = default {
                        FailureValue::Default(default) // We can't execute this here because some defaults might be functions and some functions might not always have the same result.
                    } else {
                        return Err(RowError::Unreachable.into());
                    }
                } else if nullable {
                    FailureValue::Null
                } else {
                    FailureValue::Throw
                };

                for (row_index, row) in rows.iter().enumerate() {
                    output_values[row_index][column_index] = if let Some(index) = values_index {
                        let found = row[index].clone();
                        let value = $processing_function(storage, found, data_type).await?;

                        println!(
                            "!!! DataType {:?} Column {:?} Value {:?}",
                            data_type, name, value
                        );
                        if do_validate {
                            value.validate_null(nullable)?;
                            value.validate_type(data_type)?;
                        }
                        value
                    } else {
                        match failure_value {
                            FailureValue::Throw => {
                                return Err(RowError::LackOfRequiredColumn(name.clone()).into());
                            }
                            FailureValue::Null => Value::Null,
                            FailureValue::Default(expression) => {
                                evaluate_expression(storage, expression, data_type).await?
                            }
                        }
                    }
                }
            }
            Ok(output_values.into_iter().map(Row).collect())
        }
    };
}

bulk_build_rows!(Expr, bulk_build_rows_expr, process_expr);
bulk_build_rows!(Value, bulk_build_rows_value, process_value);

pub async fn bulk_build_rows_row<T: 'static + Debug>(
    storage: &dyn Store<T>,
    column_defs: &[ColumnDef],
    columns: &[Ident],
    rows: Vec<Row>,
    do_default: bool,
    do_validate: bool,
) -> Result<Vec<Row>> {
    bulk_build_rows_value(
        storage,
        column_defs,
        columns,
        rows.into_iter().map(|row| row.0).collect(),
        do_default,
        do_validate,
    )
    .await
}

enum FailureValue<'a> {
    Default(&'a Expr),
    Null,
    Throw,
}
