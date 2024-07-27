//! Structs relative to SQL Triggers

use crate::ast::{
    CreateTrigger, Deferred, DropTrigger, TriggerEvent, TriggerObject, TriggerPeriod, TriggerReferencing, TriggerReferencingType
};
use crate::ast::{OperateFunctionArg, ReferentialAction};

use super::{translate_expr, translate_operate_function_arg};

impl From<sqlparser::ast::TriggerPeriod> for TriggerPeriod {
    fn from(period: sqlparser::ast::TriggerPeriod) -> Self {
        match period {
            sqlparser::ast::TriggerPeriod::Before => TriggerPeriod::Before,
            sqlparser::ast::TriggerPeriod::After => TriggerPeriod::After,
            sqlparser::ast::TriggerPeriod::InsteadOf => TriggerPeriod::InsteadOf,
        }
    }
}

impl From<sqlparser::ast::ReferentialAction> for ReferentialAction {
    fn from(action: sqlparser::ast::ReferentialAction) -> Self {
        match action {
            sqlparser::ast::ReferentialAction::Restrict
            | sqlparser::ast::ReferentialAction::NoAction => ReferentialAction::NoAction,
            sqlparser::ast::ReferentialAction::Cascade => ReferentialAction::Cascade,
            sqlparser::ast::ReferentialAction::SetNull => ReferentialAction::SetNull,
            sqlparser::ast::ReferentialAction::SetDefault => ReferentialAction::SetDefault,
        }
    }
}

impl From<sqlparser::ast::TriggerEvent> for TriggerEvent {
    fn from(event: sqlparser::ast::TriggerEvent) -> Self {
        match event {
            sqlparser::ast::TriggerEvent::Insert => TriggerEvent::Insert,
            sqlparser::ast::TriggerEvent::Update(columns) => {
                TriggerEvent::Update(columns.into_iter().map(|ident| ident.value).collect())
            }
            sqlparser::ast::TriggerEvent::Delete => TriggerEvent::Delete,
            sqlparser::ast::TriggerEvent::Truncate => TriggerEvent::Truncate,
        }
    }
}

impl From<sqlparser::ast::TriggerReferencingType> for TriggerReferencingType {
    fn from(referencing: sqlparser::ast::TriggerReferencingType) -> Self {
        match referencing {
            sqlparser::ast::TriggerReferencingType::OldTable => TriggerReferencingType::OldTable,
            sqlparser::ast::TriggerReferencingType::NewTable => TriggerReferencingType::NewTable,
        }
    }
}

impl From<sqlparser::ast::TriggerReferencing> for TriggerReferencing {
    fn from(referencing: sqlparser::ast::TriggerReferencing) -> Self {
        TriggerReferencing {
            refer_type: referencing.refer_type.into(),
            is_as: referencing.is_as,
            transition_relation_name: referencing.transition_relation_name.to_string(),
        }
    }
}

impl From<sqlparser::ast::TriggerObject> for TriggerObject {
    fn from(object: sqlparser::ast::TriggerObject) -> Self {
        match object {
            sqlparser::ast::TriggerObject::Row => TriggerObject::Row,
            sqlparser::ast::TriggerObject::Statement => TriggerObject::Statement,
        }
    }
}

impl From<sqlparser::ast::DeferrableInitial> for Deferred {
    fn from(deferred: sqlparser::ast::DeferrableInitial) -> Self {
        match deferred {
            sqlparser::ast::DeferrableInitial::Immediate => Deferred::Immediate,
            sqlparser::ast::DeferrableInitial::Deferred => Deferred::Deferred,
        }
    }
}

impl From<sqlparser::ast::DeferrableCharacteristics> for Deferred {
    fn from(characteristics: sqlparser::ast::DeferrableCharacteristics) -> Self {
        if characteristics.deferrable.unwrap_or(false) {
            characteristics
                .initially
                .map_or(Deferred::Deferred, Deferred::from)
        } else {
            Deferred::Immediate
        }
    }
}

impl CreateTrigger {
    pub(crate) fn from_sql_parser(
        statement: sqlparser::ast::Statement,
    ) -> crate::result::Result<Self> {
        let (
            or_replace,
            name,
            table_name,
            trigger_object,
            events,
            period,
            condition,
            exec_body,
            referencing,
            characteristics,
        ) = match statement {
            sqlparser::ast::Statement::CreateTrigger {
                or_replace,
                name,
                table_name,
                trigger_object,
                events,
                period,
                condition,
                exec_body,
                referencing,
                characteristics,
                include_each: _,
            } => (
                or_replace,
                name,
                table_name,
                trigger_object,
                events,
                period,
                condition,
                exec_body,
                referencing,
                characteristics,
            ),
            _ => unreachable!(),
        };

        Ok(CreateTrigger {
            name: name.to_string(),
            events: events.into_iter().map(TriggerEvent::from).collect(),
            period: period.into(),
            object: trigger_object.into(),
            table_name: table_name.to_string(),
            function_name: exec_body.func_desc.name.to_string(),
            arguments: exec_body.func_desc.args.map_or_else(
                || Ok(Vec::new()),
                |args| {
                    args.iter()
                        .map(translate_operate_function_arg)
                        .collect::<crate::result::Result<Vec<OperateFunctionArg>>>()
                },
            )?,
            condition: condition.as_ref().map(translate_expr).transpose()?,
            referencing: referencing
                .into_iter()
                .map(TriggerReferencing::from)
                .collect(),
            deferrable: characteristics.map(Deferred::from),
            or_replace,
        })
    }
}

impl DropTrigger {
    pub(super) fn from_sql_parser(
        statement: sqlparser::ast::Statement,
    ) -> Self {
        let (
            if_exists,
            trigger_name,
            table_name,
            option,
        ) = match statement {
            sqlparser::ast::Statement::DropTrigger {
                if_exists,
                trigger_name,
                table_name,
                option,
            } => (
                if_exists,
                trigger_name,
                table_name,
                option,
            ),
            _ => unreachable!(),
        };

        DropTrigger {
            if_exists,
            name: trigger_name.to_string(),
            table_name: table_name.to_string(),
            referential_action: option.map(ReferentialAction::from),
        }
    }
}