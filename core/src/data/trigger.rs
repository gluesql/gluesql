//! Submodule providing the Trigger-related struct for the data module.

use crate::ast::{
    CreateTrigger, Deferred, Expr, OperateFunctionArg, TriggerEvent, TriggerObject, TriggerPeriod,
    TriggerReferencing,
};
use crate::ast::ToSql;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Trigger {
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
}

impl From<CreateTrigger> for Trigger {
    fn from(trigger: CreateTrigger) -> Self {
        Self {
            name: trigger.name,
            events: trigger.events,
            period: trigger.period,
            object: trigger.object,
            table_name: trigger.table_name,
            function_name: trigger.function_name,
            arguments: trigger.arguments,
            condition: trigger.condition,
            referencing: trigger.referencing,
            deferrable: trigger.deferrable,
        }
    }
}

impl Trigger {
    pub fn to_ddl(&self) -> String {
        let events = self
            .events
            .iter()
            .map(|event| event.to_dll())
            .collect::<Vec<_>>()
            .join(" OR ");
        let deferrable = self
            .deferrable
            .map_or("NOT DEFERRABLE".to_string(), |d| d.to_string());
        let referencing = TriggerReferencing::to_dll(&self.referencing);
        let arguments = if !self.arguments.is_empty() {
            format!(
                "({})",
                self.arguments
                    .iter()
                    .map(|arg| arg.to_sql())
                    .collect::<Vec<_>>()
                    .join(", ")
            )
        } else {
            "".to_string()
        };
        let when = if let Some(condition) = &self.condition {
            format!(" WHEN {}", condition.to_sql())
        } else {
            "".to_string()
        };

        format!(
            "CREATE OR REPLACE TRIGGER {} {} {events} ON {} {deferrable}{referencing} FOR EACH {}{when} EXECUTE FUNCTION {}{arguments}",
            self.name, self.period, self.table_name, self.object, self.function_name
        )
    }
}
