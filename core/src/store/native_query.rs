use crate::{
    ast::DataType,
    data::Value,
    result::{Error, Result},
};

pub type NativeRowIter<'a> = Box<dyn Iterator<Item = Result<Vec<Value>>> + 'a>;

/// Optional execution of read queries prepared by a storage's planner.
pub trait NativeQuery {
    /// Returns rows in the order and logical types described by `output_types`.
    ///
    /// The iterator borrows the storage, not the query, parameters, or type slice.
    /// Empty output types request an empty value list for each matching row.
    fn query_native<'a>(
        &'a self,
        _query: &str,
        _params: &[Value],
        _output_types: &[DataType],
    ) -> Result<NativeRowIter<'a>> {
        Err(Error::StorageMsg(
            "[Storage] NativeQuery::query_native is not supported".to_owned(),
        ))
    }
}
