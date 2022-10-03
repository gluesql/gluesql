use crate::{ast::Statement, result::Result};

pub trait Build {
    fn build(self) -> Result<Statement>;
}
