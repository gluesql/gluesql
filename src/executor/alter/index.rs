#![cfg(feature = "index")]

use {
    crate::{
        ast::{Expr, ObjectName},
        data::get_name,
        result::MutResult,
        store::{GStore, GStoreMut},
    },
    std::fmt::Debug,
};

pub async fn create_index<T: 'static + Debug, U: GStore<T> + GStoreMut<T>>(
    storage: U,
    table_name: &ObjectName,
    index_name: &ObjectName,
    expr: &Expr,
) -> MutResult<U, ()> {
    let names = (|| {
        let table_name = get_name(table_name)?;
        let index_name = get_name(index_name)?;

        Ok((table_name, index_name))
    })();

    let (table_name, index_name) = match names {
        Ok(s) => s,
        Err(e) => {
            return Err((storage, e));
        }
    };

    storage.create_index(table_name, index_name, expr).await
}

pub async fn drop_index<T: 'static + Debug, U: GStore<T> + GStoreMut<T>>(
    storage: U,
    table_name: &ObjectName,
    index_name: &ObjectName,
) -> MutResult<U, ()> {
    let names = (|| {
        let table_name = get_name(table_name)?;
        let index_name = get_name(index_name)?;

        Ok((table_name, index_name))
    })();

    let (table_name, index_name) = match names {
        Ok(s) => s,
        Err(e) => {
            return Err((storage, e));
        }
    };

    storage.drop_index(table_name, index_name).await
}
