pub use error::FilterError;

mod check;
mod error;
mod parsed;

use std::fmt::Debug;

use sqlparser::ast::{Expr, Ident};

use crate::data::Row;
use crate::executor::{BlendContext, FilterContext};
use crate::result::Result;
use crate::storage::Store;

use check::{check_blended_expr, check_expr};

pub struct Filter<'a, T: 'static + Debug> {
    storage: &'a dyn Store<T>,
    where_clause: Option<&'a Expr>,
    context: Option<&'a FilterContext<'a>>,
}

impl<'a, T: 'static + Debug> Filter<'a, T> {
    pub fn new(
        storage: &'a dyn Store<T>,
        where_clause: Option<&'a Expr>,
        context: Option<&'a FilterContext<'a>>,
    ) -> Self {
        Self {
            storage,
            where_clause,
            context,
        }
    }

    pub fn check(&self, table_alias: &str, columns: &[Ident], row: &Row) -> Result<bool> {
        let context = FilterContext::new(table_alias, columns, row, self.context);

        match self.where_clause {
            Some(expr) => check_expr(self.storage, &context, expr),
            None => Ok(true),
        }
    }

    pub fn check_blended(&self, blend_context: &BlendContext<'_, T>) -> Result<bool> {
        match self.where_clause {
            Some(expr) => check_blended_expr(self.storage, self.context, blend_context, expr),
            None => Ok(true),
        }
    }
}

/*
pub struct BlendedFilter<'a, T: 'static + Debug> {
    filter: &'a Filter<'a, T>,
    context: Option<&'a BlendContext<'a, T>>,
}

impl<'a, T: 'static + Debug> BlendedFilter<'a, T> {
    pub fn new(filter: &'a Filter<'a, T>, context: Option<&'a BlendContext<'a, T>>) -> Self {
        Self { filter, context }
    }

    pub fn check(&self, table: &Table, columns: &[Column], row: &Row) -> Result<bool> {
        let BlendedFilter {
            filter:
                Filter {
                    storage,
                    where_clause,
                    context: next,
                },
            context: blend_context,
        } = self;

        let filter_context = FilterContext::new(table, columns, row, *next);

        where_clause.map_or(Ok(true), |expr| match blend_context {
            Some(blend_context) => {
                check_blended_expr(*storage, Some(&filter_context), blend_context, expr)
            }
            None => check_expr(*storage, &filter_context, expr),
        })
    }
}
*/
