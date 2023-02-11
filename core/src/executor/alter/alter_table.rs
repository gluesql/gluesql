use {
    super::{validate, AlterError},
    crate::{
        ast::{AlterTableOperation, Expr, Function},
        data::{Schema, SchemaIndex},
        result::Result,
        store::{GStore, GStoreMut},
    },
};

pub async fn alter_table<T: GStore + GStoreMut>(
    storage: &mut T,
    table_name: &str,
    operation: &AlterTableOperation,
) -> Result<()> {
    match operation {
        AlterTableOperation::RenameTable {
            table_name: new_table_name,
        } => storage.rename_schema(table_name, new_table_name).await,
        AlterTableOperation::RenameColumn {
            old_column_name,
            new_column_name,
        } => {
            storage
                .rename_column(table_name, old_column_name, new_column_name)
                .await
        }
        AlterTableOperation::AddColumn { column_def } => {
            validate(column_def)?;

            storage.add_column(table_name, column_def).await
        }
        AlterTableOperation::DropColumn {
            column_name,
            if_exists,
        } => {
            let indexes = match storage.fetch_schema(table_name).await? {
                Some(Schema { indexes, .. }) => indexes,
                None => {
                    return Err(AlterError::TableNotFound(table_name.to_owned()).into());
                }
            };

            let indexes = indexes
                .iter()
                .filter(|SchemaIndex { expr, .. }| find_column(expr, column_name))
                .map(|SchemaIndex { name, .. }| name);

            for index_name in indexes {
                storage.drop_index(table_name, index_name).await?;
            }

            storage
                .drop_column(table_name, column_name, *if_exists)
                .await
        }
    }
}

fn find_column(expr: &Expr, column_name: &str) -> bool {
    let find = |expr| find_column(expr, column_name);

    match expr {
        Expr::Identifier(ident) => ident == column_name,
        Expr::Nested(expr) => find(expr),
        Expr::BinaryOp { left, right, .. } => find(left) || find(right),
        Expr::UnaryOp { expr, .. } => find(expr),
        Expr::Function(func) => match func.as_ref() {
            Function::Cast { expr, .. } => find(expr),
            _ => false,
        },
        _ => false,
    }
}
