use serde::Serialize;
use std::fmt::Debug;
use thiserror::Error;

use super::{BlendContext, FilterContext};
use crate::data::Value;
use crate::result::Result;

// TODO: add error test case
#[derive(Error, Serialize, Debug, PartialEq)]
pub enum UnionContextError {
    #[error("value not found")]
    ValueNotFound,
}

#[derive(Clone)]
pub struct UnionContext<'a> {
    pub filter_context: Option<&'a FilterContext<'a>>,
    blend_context: Option<&'a BlendContext<'a>>,
}

impl<'a> UnionContext<'a> {
    pub fn new(
        filter_context: Option<&'a FilterContext<'a>>,
        blend_context: Option<&'a BlendContext<'a>>,
    ) -> Self {
        Self {
            filter_context,
            blend_context,
        }
    }

    pub fn get_value(&self, target: &str) -> Result<&'a Value> {
        match (self.filter_context, self.blend_context) {
            (Some(fc), Some(bc)) => fc.get_value(target).or_else(|_| bc.get_value(target)),
            (Some(fc), None) => fc.get_value(target),
            (None, Some(bc)) => bc.get_value(target),
            (None, None) => Err(UnionContextError::ValueNotFound.into()),
        }
    }

    pub fn get_alias_value(&self, table_alias: &str, target: &str) -> Result<&'a Value> {
        match (self.filter_context, self.blend_context) {
            (Some(fc), Some(bc)) => fc
                .get_alias_value(table_alias, target)
                .or_else(|_| bc.get_alias_value(table_alias, target)),
            (Some(fc), None) => fc.get_alias_value(table_alias, target),
            (None, Some(bc)) => bc.get_alias_value(table_alias, target),
            (None, None) => Err(UnionContextError::ValueNotFound.into()),
        }
    }
}
