mod ddl;
mod expr;
mod projection;
mod query;

pub use {
    ddl::AlterTableOperationPlan,
    expr::{
        AggregateExprPlan, AggregateFunctionPlan, CountArgExprPlan, ExprPlan, FunctionExprPlan,
        plan_scalar_expr,
    },
    projection::{ProjectionPlan, SelectItemPlan},
    query::{
        AggregationInputPlan, AggregationPlan, DerivedSourcePlan, DictionarySourcePlan,
        DistinctInputPlan, DistinctPlan, FilterInputPlan, FilterPlan, HashJoinInputPlan,
        HashJoinPlan, HavingPlan, IndexPredicatePlan, InnerJoinInputPlan, InnerJoinPlan,
        JoinConditionInputPlan, JoinConditionPlan, LeftOuterJoinInputPlan, LeftOuterJoinPlan,
        LimitInputPlan, LimitPlan, NestedLoopJoinInputPlan, NestedLoopJoinPlan, OffsetInputPlan,
        OffsetPlan, OrderByExprPlan, ProjectInputPlan, ProjectPlan, QueryPlan, SelectOrderByPlan,
        SeriesSourcePlan, SourcePlan, TableAccessPlan, TableAliasPlan, TableSourcePlan,
        ValuesOrderByPlan, ValuesPlan,
    },
};

use {
    crate::ast::{self, ForeignKey, Variable},
    serde::{Deserialize, Serialize},
};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum StatementPlan {
    ShowColumns {
        table_name: String,
    },
    Query(QueryPlan),
    Insert {
        table_name: String,
        columns: Vec<String>,
        source: QueryPlan,
        on_conflict: Option<OnConflictPlan>,
        returning: Option<Vec<SelectItemPlan>>,
    },
    Update {
        table_name: String,
        assignments: Vec<AssignmentPlan>,
        from: Option<ast::SourceTable>,
        selection: Option<ExprPlan>,
        returning: Option<Vec<SelectItemPlan>>,
    },
    Delete {
        table_name: String,
        using: Option<ast::SourceTable>,
        selection: Option<ExprPlan>,
        returning: Option<Vec<SelectItemPlan>>,
    },
    CreateTable {
        if_not_exists: bool,
        name: String,
        columns: Option<Vec<ast::ColumnDef>>,
        source: Option<Box<QueryPlan>>,
        engine: Option<String>,
        foreign_keys: Vec<ForeignKey>,
        comment: Option<String>,
    },
    CreateFunction {
        or_replace: bool,
        name: String,
        args: Vec<ast::OperateFunctionArg>,
        return_: ast::Expr,
    },
    AlterTable {
        name: String,
        operation: AlterTableOperationPlan,
    },
    DropTable {
        if_exists: bool,
        names: Vec<String>,
        cascade: bool,
    },
    DropFunction {
        if_exists: bool,
        names: Vec<String>,
    },
    CreateIndex {
        name: String,
        table_name: String,
        column: ast::OrderByExpr,
    },
    DropIndex {
        name: String,
        table_name: String,
    },
    StartTransaction,
    Commit,
    Rollback,
    ShowVariable(Variable),
    ShowIndexes(String),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct AssignmentPlan {
    pub id: String,
    pub value: ExprPlan,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct OnConflictPlan {
    pub conflict_target: Vec<String>,
    pub action: OnConflictActionPlan,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum OnConflictActionPlan {
    DoNothing,
    DoUpdate {
        assignments: Vec<AssignmentPlan>,
        selection: Option<ExprPlan>,
    },
}

impl From<ast::OnConflict> for OnConflictPlan {
    fn from(on_conflict: ast::OnConflict) -> Self {
        let ast::OnConflict {
            conflict_target,
            action,
        } = on_conflict;

        Self {
            conflict_target,
            action: match action {
                ast::OnConflictAction::DoNothing => OnConflictActionPlan::DoNothing,
                ast::OnConflictAction::DoUpdate {
                    assignments,
                    selection,
                } => OnConflictActionPlan::DoUpdate {
                    assignments: assignments.into_iter().map(Into::into).collect(),
                    selection: selection.map(Into::into),
                },
            },
        }
    }
}

impl From<ast::Statement> for StatementPlan {
    fn from(statement: ast::Statement) -> Self {
        match statement {
            ast::Statement::ShowColumns { table_name } => Self::ShowColumns { table_name },
            ast::Statement::Query(query) => Self::Query(query.into()),
            ast::Statement::Insert {
                table_name,
                columns,
                source,
                on_conflict,
                returning,
            } => Self::Insert {
                table_name,
                columns,
                source: source.into(),
                on_conflict: on_conflict.map(Into::into),
                returning: returning.map(|items| items.into_iter().map(Into::into).collect()),
            },
            ast::Statement::Update {
                table_name,
                assignments,
                from,
                selection,
                returning,
            } => Self::Update {
                table_name,
                assignments: assignments.into_iter().map(Into::into).collect(),
                from,
                selection: selection.map(Into::into),
                returning: returning.map(|items| items.into_iter().map(Into::into).collect()),
            },
            ast::Statement::Delete {
                table_name,
                using,
                selection,
                returning,
            } => Self::Delete {
                table_name,
                using,
                selection: selection.map(Into::into),
                returning: returning.map(|items| items.into_iter().map(Into::into).collect()),
            },
            ast::Statement::CreateTable {
                if_not_exists,
                name,
                columns,
                source,
                engine,
                foreign_keys,
                comment,
            } => Self::CreateTable {
                if_not_exists,
                name,
                columns,
                source: source.map(|query| Box::new((*query).into())),
                engine,
                foreign_keys,
                comment,
            },
            ast::Statement::CreateFunction {
                or_replace,
                name,
                args,
                return_,
            } => Self::CreateFunction {
                or_replace,
                name,
                args,
                return_,
            },
            ast::Statement::AlterTable { name, operation } => Self::AlterTable {
                name,
                operation: operation.into(),
            },
            ast::Statement::DropTable {
                if_exists,
                names,
                cascade,
            } => Self::DropTable {
                if_exists,
                names,
                cascade,
            },
            ast::Statement::DropFunction { if_exists, names } => {
                Self::DropFunction { if_exists, names }
            }
            ast::Statement::CreateIndex {
                name,
                table_name,
                column,
            } => Self::CreateIndex {
                name,
                table_name,
                column,
            },
            ast::Statement::DropIndex { name, table_name } => Self::DropIndex { name, table_name },
            ast::Statement::StartTransaction => Self::StartTransaction,
            ast::Statement::Commit => Self::Commit,
            ast::Statement::Rollback => Self::Rollback,
            ast::Statement::ShowVariable(variable) => Self::ShowVariable(variable),
            ast::Statement::ShowIndexes(table_name) => Self::ShowIndexes(table_name),
        }
    }
}

impl From<ast::Assignment> for AssignmentPlan {
    fn from(assignment: ast::Assignment) -> Self {
        let ast::Assignment { id, value } = assignment;

        Self {
            id,
            value: value.into(),
        }
    }
}
