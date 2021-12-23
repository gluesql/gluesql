mod aggregate;
mod alter;
mod context;
mod evaluate;
mod execute;
mod fetch;
mod filter;
mod join;
mod limit;
mod select;
mod sort;
mod update;
mod validate;

pub use aggregate::{AggregateError, GroupKey};
pub use alter::AlterError;
pub use evaluate::{evaluate_stateless, EvaluateError};
pub use execute::{ExecuteError, Payload};
pub use fetch::FetchError;
pub use select::SelectError;
pub use update::UpdateError;
pub use validate::{UniqueKey, ValidateError};

#[cfg(not(feature = "transaction"))]
pub use execute::execute;
#[cfg(feature = "transaction")]
pub use execute::execute_atomic as execute;

#[cfg(feature = "metadata")]
pub use execute::PayloadVariable;
