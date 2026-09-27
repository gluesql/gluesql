use {
    super::{AlterError, Referencing, validate_column_def},
    crate::{
        ast::{Expr, Function},
        data::{Schema, SchemaIndex, Value},
        executor::evaluate_stateless,
        plan::AlterTableOperationPlan,
        result::Result,
        store::{GStore, GStoreMut},
    },
};

pub fn alter_table<T: GStore + GStoreMut>(
    storage: &mut T,
    table_name: &str,
    operation: &AlterTableOperationPlan,
) -> Result<()> {
    if let AlterTableOperationPlan::RenameColumn {
        old_column_name: column_name,
        ..
    }
    | AlterTableOperationPlan::DropColumn { column_name, .. } = operation
    {
        if let Some(schema) = storage.fetch_schema(table_name)? {
            let referencing_foreign_key = schema
                .foreign_keys
                .into_iter()
                .find(|foreign_key| column_name == &foreign_key.referencing_column_name);

            if let Some(foreign_key) = referencing_foreign_key {
                return Err(AlterError::CannotAlterReferencingColumn {
                    referencing: Referencing {
                        table_name: table_name.to_owned(),
                        foreign_key,
                    },
                }
                .into());
            }
        }

        let referencings = storage.fetch_referencings(table_name)?;
        let referencing = referencings
            .into_iter()
            .find(|Referencing { foreign_key, .. }| {
                column_name == &foreign_key.referenced_column_name
            });

        if let Some(referencing) = referencing {
            return Err(AlterError::CannotAlterReferencedColumn { referencing }.into());
        }
    }

    match operation {
        AlterTableOperationPlan::RenameTable {
            table_name: new_table_name,
        } => storage.rename_schema(table_name, new_table_name),
        AlterTableOperationPlan::RenameColumn {
            old_column_name,
            new_column_name,
        } => storage.rename_column(table_name, old_column_name, new_column_name),
        AlterTableOperationPlan::AddColumn { column_def } => {
            validate_column_def(column_def)?;

            let default_value = match (&column_def.default, column_def.nullable) {
                (Some(default), _) => evaluate_stateless(None, default.planned())?
                    .try_into_value(&column_def.data_type, column_def.nullable)?,
                (None, true) => Value::Null,
                (None, false) => {
                    return Err(AlterError::DefaultValueRequired(column_def.to_column_def()).into());
                }
            };

            storage.add_column(table_name, &column_def.to_column_def(), default_value)
        }
        AlterTableOperationPlan::DropColumn {
            column_name,
            if_exists,
        } => {
            let Some(Schema { indexes, .. }) = storage.fetch_schema(table_name)? else {
                return Err(AlterError::TableNotFound(table_name.to_owned()).into());
            };

            let indexes = indexes
                .iter()
                .filter(|SchemaIndex { expr, .. }| find_column(expr, column_name))
                .map(|SchemaIndex { name, .. }| name);

            for index_name in indexes {
                storage.drop_index(table_name, index_name)?;
            }

            storage.drop_column(table_name, column_name, *if_exists)
        }
    }
}

fn find_column(expr: &Expr, column_name: &str) -> bool {
    let find = |expr| find_column(expr, column_name);

    match expr {
        Expr::Identifier(ident) => ident == column_name,
        Expr::Nested(expr) | Expr::UnaryOp { expr, .. } => find(expr),
        Expr::BinaryOp { left, right, .. } => find(left) || find(right),
        Expr::Function(func) => match func.as_ref() {
            Function::Cast { expr, .. } => find(expr),
            _ => false,
        },
        _ => false,
    }
}
