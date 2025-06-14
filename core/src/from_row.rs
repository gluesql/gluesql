use crate::{data::Row, result::Result};

pub trait FromRow: Sized {
    fn from_row(row: &Row) -> Result<Self>;
}
