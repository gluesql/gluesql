use crate::{plan::StatementPlan, result::Result};

pub trait Build {
    fn build(self) -> Result<StatementPlan>;
}
