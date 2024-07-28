//! Structs relative to SQL Triggers

use crate::ast::{Expr, OperateFunctionArg, ReferentialAction};
use strum_macros::Display;

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    PartialOrd,
    Eq,
    Ord,
    Hash,
    serde::Serialize,
    serde::Deserialize,
    Display,
)]
/// An enumeration representing the different types of events that can trigger a database action.
///
/// Triggers in a database system can be activated by various events such as data insertion,
/// updates, deletions, or table truncations. The `TriggerEvent` enum provides a way to specify
/// and handle these different triggering events in a structured and type-safe manner. Each variant
/// corresponds to a specific kind of database operation that can initiate a trigger.
///
/// # Variants
///
/// - `Insert`: Represents a trigger event that occurs when a new row is inserted into a table.
///   This event is useful for operations that need to process or validate new data as it is added
///   to the database.
///
/// - `Update(Vec<String>)`: Represents a trigger event that occurs when existing rows in a table are updated.
///   The `Vec<String>` parameter allows specification of the columns that are relevant to the trigger.
///   This is useful for triggers that should only fire when certain columns are updated, allowing for more
///   fine-grained control over the trigger's behavior.
///
/// - `Delete`: Represents a trigger event that occurs when rows are deleted from a table.
///   This event is commonly used for operations that need to clean up related data or enforce constraints
///   before data is removed from the database.
///
/// - `Truncate`: Represents a trigger event that occurs when a table is truncated. Truncation
///   removes all rows from a table without logging the individual row deletions. Triggers on this event
///   can be used to handle scenarios where bulk data removal needs additional processing.
///
pub enum TriggerPeriod {
    #[strum(to_string = "BEFORE")]
    /// Represents a trigger that fires before the associated event occurs.
    Before,

    #[strum(to_string = "AFTER")]
    /// Represents a trigger that fires after the associated event occurs.
    After,

    #[strum(to_string = "INSTEAD OF")]
    /// Represents a trigger that executes instead of the associated event.
    InsteadOf,
}

#[derive(
    Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash, serde::Serialize, serde::Deserialize,
)]
/// An enumeration representing the timing of a trigger's execution in relation to a database event.
///
/// In database systems like PostgreSQL, triggers can be set to execute at different times relative
/// to the event they are associated with. The `TriggerPeriod` enum provides a way to specify when
/// a trigger should be executed, allowing for precise control over the timing of trigger actions.
///
/// # Variants
///
/// - `Before`: Represents a trigger that fires before the specified event occurs. This timing is often
///   used to perform validation, transformation, or other pre-processing tasks that must occur before
///   the event is finalized. `Before` triggers can be particularly useful for enforcing constraints
///   or modifying data before it is committed to the database.
///
/// - `After`: Represents a trigger that fires after the specified event has occurred. This timing is typically
///   used for operations that need to occur after the data change is finalized, such as logging,
///   updating related tables, or enforcing cross-row constraints that depend on the final state of
///   the data. `After` triggers have access to the final values of the affected rows, making them
///   suitable for tasks that require knowledge of the completed change.
///
/// - `InsteadOf`: Represents a trigger that substitutes itself for the specified event, executing
///   instead of the standard action. This is often used with views to provide custom behavior for
///   operations like `INSERT`, `UPDATE`, or `DELETE` that would not normally be supported on a view.
///   `InsteadOf` triggers allow complex business logic to be implemented directly in the database
///   by overriding the default behavior of SQL operations.
pub enum TriggerEvent {
    /// Represents an event triggered by inserting new rows into a table.
    Insert,

    /// Represents an event triggered by updating existing rows. The optional `Vec<String>` can specify
    /// the columns involved in the update.
    Update(Vec<String>),

    /// Represents an event triggered by deleting rows from a table.
    Delete,

    /// Represents an event triggered by truncating a table, removing all rows at once.
    Truncate,
}

impl TriggerEvent {
    pub fn to_dll(&self) -> String {
        match self {
            TriggerEvent::Insert => "INSERT".to_string(),
            TriggerEvent::Update(columns) => format!("UPDATE OF {}", columns.join(", ")),
            TriggerEvent::Delete => "DELETE".to_string(),
            TriggerEvent::Truncate => "TRUNCATE".to_string(),
        }
    }
}

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    PartialOrd,
    Eq,
    Ord,
    Hash,
    serde::Serialize,
    serde::Deserialize,
    Display,
)]
/// An enumeration representing the types of transition tables that can be referenced in a database trigger.
///
/// In database systems like PostgreSQL, triggers can access transition tables to examine the state of
/// the data before and after a triggering event. The `TriggerReferencingType` enum is used to specify
/// which transition table is being referred to in a trigger function, providing a clear and type-safe
/// mechanism to differentiate between the `OLD` and `NEW` states of the data.
///
/// # Variants
///
/// - `OldTable`: Represents a reference to the transition table containing the old state of the data,
///   before the triggering event occurs. This is typically used in `AFTER` or `BEFORE` triggers to
///   examine the previous values of the rows that have been modified.
///
/// - `NewTable`: Represents a reference to the transition table containing the new state of the data,
///   after the triggering event occurs. This is commonly used to access the updated values of rows
///   during `INSERT` or `UPDATE` operations.
///
pub enum TriggerReferencingType {
    #[strum(to_string = "OLD TABLE")]
    /// Represents a reference to the old state of the data, accessible via the OLD transition table.
    OldTable,

    #[strum(to_string = "NEW TABLE")]
    /// Represents a reference to the new state of the data, accessible via the NEW transition table.
    NewTable,
}

#[derive(
    Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash, serde::Serialize, serde::Deserialize,
)]
/// A struct representing the details of how a trigger references transition tables in a database.
///
/// In database triggers, it is possible to reference transition tables that capture the old and new
/// states of data affected by a triggering event. The `TriggerReferencing` struct encapsulates the
/// information necessary to specify such references, including the type of transition table, any
/// aliasing behavior, and the name of the transition relation being referenced.
///
/// # Fields
///
/// - `refer_type`: Specifies the type of transition table being referenced. It uses the
///   `TriggerReferencingType` enum to indicate whether the reference is to the old or new state
///   of the data, using variants like `OldTable` or `NewTable`.
///
/// - `is_as`: A boolean indicating whether the reference uses an alias for the transition relation.
///   When true, it signifies that an alias is being used to refer to the transition table, allowing
///   for more readable or context-specific naming in complex queries and trigger functions.
///
/// - `transition_relation_name`: A string that specifies the name of the transition relation as it
///   will be referred to in the trigger function. This name is used to access the transition table
///   within the trigger logic, providing a way to perform operations based on the table's data.
///
pub struct TriggerReferencing {
    /// The type of transition table being referenced (`OldTable` or `NewTable`).
    pub(crate) refer_type: TriggerReferencingType,

    /// Indicates if the transition table is being referenced using an alias (`AS` keyword).
    pub(crate) is_as: bool,

    /// The name used for the transition relation in the trigger function.
    pub(crate) transition_relation_name: String,
}

impl TriggerReferencing {
    pub fn to_dll<'a, I: IntoIterator<Item = &'a TriggerReferencing>>(referencing: I) -> String {
        let mut number_of_references = 0;
        let references = referencing
            .into_iter()
            .map(|r| {
                number_of_references += 1;
                format!(
                    "{} {}{}",
                    r.refer_type,
                    if r.is_as { "AS " } else { "" },
                    r.transition_relation_name
                )
            })
            .collect::<Vec<String>>()
            .join(" ");
        if number_of_references == 0 {
            "".to_string()
        } else {
            format!(" REFERENCING {references}")
        }
    }
}

#[derive(
    Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash, serde::Serialize, serde::Deserialize, Display,
)]
/// An enumeration representing the level at which a trigger operates within a database context.
///
/// Triggers in a database can be defined to operate at different levels depending on the
/// requirements of the application. The `TriggerObject` enum provides a way to specify
/// these levels in a type-safe manner.
///
/// # Variants
///
/// - `Row`: Represents a row-level trigger. A trigger defined at this level executes
///   once for each row affected by the triggering SQL statement. This is particularly
///   useful when operations need to be performed on individual rows of a table, such
///   as logging changes, validating data, or maintaining audit trails. Row-level triggers
///   can access the `OLD` and `NEW` values of the rows, allowing detailed examination
///   of changes.
///
/// - `Statement`: Represents a statement-level trigger. A trigger at this level executes
///   once for the entire SQL statement, regardless of the number of rows affected. This
///   is useful for operations that should occur once per transaction, such as updating
///   summary tables, maintaining cross-row integrity, or auditing high-level operations
///   without the need for specific row details.
///
pub enum TriggerObject {
    #[strum(to_string = "ROW")]
    /// Represents a trigger that operates at the row level, firing once for each row affected.
    Row,
    #[strum(to_string = "STATEMENT")]
    /// Represents a trigger that operates at the statement level, firing once for each SQL statement.
    Statement,
}

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize, Display,
)]
/// An enumeration representing the deferrable state of a database trigger or constraint.
///
/// In relational database systems, constraints and triggers can be set to defer their execution
/// to a later point in the transaction. This behavior is controlled by specifying whether they
/// should execute immediately or be deferred until the end of the transaction. The `Deferred`
/// enum provides a type-safe way to express this behavior, allowing developers to define when
/// triggers and constraints should be evaluated.
///
/// # Variants
///
/// - `Immediate`: Represents a constraint or trigger that is `INITIALLY IMMEDIATE`. This means
///   that the constraint or trigger is evaluated immediately when the associated event occurs.
///   Immediate evaluation is useful for ensuring that data integrity is checked as soon as a
///   change is made, catching violations at the earliest possible moment.
///
/// - `Deferred`: Represents a constraint or trigger that is `INITIALLY DEFERRED`. This means
///   that the constraint or trigger's evaluation is deferred until the end of the transaction.
///   Deferring evaluation can be beneficial in scenarios where intermediate states of data
///   during a transaction might temporarily violate constraints but the final state does not.
///   It allows for greater flexibility in complex transactions by checking constraints only
///   once all operations are complete.
pub enum Deferred {
    #[strum(to_string = "DEFERRABLE INITIALLY IMMEDIATE")]
    /// Represents an `INITIALLY IMMEDIATE` constraint or trigger.
    Immediate,

    #[strum(to_string = "DEFERRABLE INITIALLY DEFERRED")]
    /// Represents an `INITIALLY DEFERRED` constraint or trigger.
    Deferred,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
/// A struct representing the creation of a database trigger.
///
/// The `CreateTrigger` struct is used to define the properties and behavior of a database trigger.
/// Triggers are procedural code that automatically execute in response to specific events on a table
/// or view within a database.
pub struct CreateTrigger {
    /// The name of the trigger.
    pub name: String,

    /// The event(s) that trigger the execution of the trigger.
    /// This can be `Insert`, `Update`, `Delete`, or `Truncate`.
    pub events: Vec<TriggerEvent>,

    /// The timing of the trigger execution.
    /// This can be `Before`, `After`, or `InsteadOf`.
    pub period: TriggerPeriod,

    /// The level at which the trigger operates.
    /// This can be `Row` or `Statement`.
    pub object: TriggerObject,

    /// The name of the table on which the trigger is defined.
    pub table_name: String,

    /// The trigger function to be executed.
    pub function_name: String,

    /// The arguments to be passed to the trigger function.
    pub arguments: Vec<OperateFunctionArg>,

    /// The condition under which the trigger should be executed.
    pub condition: Option<Expr>,

    /// The list of transition tables referenced by the trigger.
    pub referencing: Vec<TriggerReferencing>,

    /// The deferrable state of the trigger.
    pub deferrable: Option<Deferred>,

    /// Whether this trigger creation is meant to replace an existing trigger.
    pub or_replace: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct DropTrigger {
    /// The name of the trigger to be dropped.
    pub name: String,
    /// The name of the table from which the trigger is to be dropped.
    pub table_name: String,
    /// Whether it is safe to ignore the error if the trigger does not exist.
    pub if_exists: bool,
    /// Referential action to be taken when the trigger is dropped.
    pub referential_action: Option<ReferentialAction>,
}
