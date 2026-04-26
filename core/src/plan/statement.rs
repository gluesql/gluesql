mod ddl;
mod expr;
mod join;
mod projection;
mod query;
mod table_factor;

pub use {
    ddl::AlterTableOperationPlan,
    expr::{AggregateFunctionPlan, AggregatePlan, CountArgExprPlan, ExprPlan, FunctionPlan},
    join::{JoinConstraintPlan, JoinExecutorPlan, JoinOperatorPlan, JoinPlan},
    projection::{ProjectionPlan, SelectItemPlan},
    query::{OrderByExprPlan, QueryPlan, SelectPlan, SetExprPlan, ValuesPlan},
    table_factor::{IndexItemPlan, TableAliasPlan, TableFactorPlan, TableWithJoinsPlan},
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
    },
    Update {
        table_name: String,
        assignments: Vec<AssignmentPlan>,
        selection: Option<ExprPlan>,
    },
    Delete {
        table_name: String,
        selection: Option<ExprPlan>,
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

impl From<ast::Statement> for StatementPlan {
    fn from(statement: ast::Statement) -> Self {
        match statement {
            ast::Statement::ShowColumns { table_name } => Self::ShowColumns { table_name },
            ast::Statement::Query(query) => Self::Query(query.into()),
            ast::Statement::Insert {
                table_name,
                columns,
                source,
            } => Self::Insert {
                table_name,
                columns,
                source: source.into(),
            },
            ast::Statement::Update {
                table_name,
                assignments,
                selection,
            } => Self::Update {
                table_name,
                assignments: assignments.into_iter().map(Into::into).collect(),
                selection: selection.map(Into::into),
            },
            ast::Statement::Delete {
                table_name,
                selection,
            } => Self::Delete {
                table_name,
                selection: selection.map(Into::into),
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
