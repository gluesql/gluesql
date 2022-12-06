use {serde::Serialize, std::fmt::Debug, thiserror::Error as ThisError};

#[derive(ThisError, Serialize, Debug, PartialEq, Eq)]
pub enum PlanError {
    /// Error that that omits when user projects common column name from multiple tables in `JOIN`
    /// situation.
    #[error("column reference {0} is ambiguous, please specify the table name")]
    ColumnReferenceAmbiguous(String),
}
