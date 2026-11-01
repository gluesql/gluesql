use {
    super::shared::{try_visit_mut_query, validate_function_definition},
    crate::{
        data::Value,
        executor::EvaluateError,
        plan::{
            CustomFunctionPlan, ExprPlan, FunctionExprPlan, QueryPlan, StatementPlan,
            StatementPlanKind,
        },
        planner::{aggregate, expr::try_visit_mut_expr},
        result::Result,
        store::CustomFunction,
    },
    std::{collections::HashMap, mem},
};

pub fn plan<T: CustomFunction + ?Sized>(
    storage: &T,
    mut statement: StatementPlan,
) -> Result<StatementPlan> {
    let first_new = statement.functions.len();
    let mut planner = CustomFunctionPlanner {
        storage,
        names: statement
            .functions
            .iter()
            .enumerate()
            .map(|(index, function)| (function.name.to_uppercase(), index))
            .collect(),
        functions: &mut statement.functions,
    };
    match &mut statement.kind {
        StatementPlanKind::Query(query) | StatementPlanKind::Insert { source: query, .. } => {
            planner.query(query)?;
        }
        StatementPlanKind::CreateTable {
            source: Some(query),
            ..
        } => planner.query(query)?,
        StatementPlanKind::Update {
            assignments,
            selection,
            ..
        } => {
            for assignment in assignments {
                planner.expr(&mut assignment.value)?;
            }
            if let Some(expr) = selection {
                planner.expr(expr)?;
            }
        }
        StatementPlanKind::Delete {
            selection: Some(expr),
            ..
        } => planner.expr(expr)?,
        _ => {}
    }

    // Definitions enter the registry before their bodies are visited, so recursive
    // references reuse an index instead of recursively expanding the definition.
    let mut index = first_new;
    while index < planner.functions.len() {
        let mut body = mem::replace(
            &mut planner.functions[index].body,
            ExprPlan::Value(Value::Null),
        );
        planner.expr(&mut body)?;
        aggregate::plan_expr(&mut body);
        planner.functions[index].body = body;
        index += 1;
    }
    Ok(statement)
}

struct CustomFunctionPlanner<'a, T: ?Sized> {
    storage: &'a T,
    names: HashMap<String, usize>,
    functions: &'a mut Vec<CustomFunctionPlan>,
}

impl<T: CustomFunction + ?Sized> CustomFunctionPlanner<'_, T> {
    fn query(&mut self, query: &mut QueryPlan) -> Result<()> {
        try_visit_mut_query(query, &mut |expr| self.expr(expr), &mut |_| Ok(()))
    }

    fn expr(&mut self, expr: &mut ExprPlan) -> Result<()> {
        try_visit_mut_expr(expr, &mut |expr| match expr {
            ExprPlan::Subquery(query)
            | ExprPlan::Exists {
                subquery: query, ..
            }
            | ExprPlan::InSubquery {
                subquery: query, ..
            } => self.query(query),
            ExprPlan::Function(function) => {
                match function.as_mut() {
                    FunctionExprPlan::UnplannedCustom { name, exprs } => {
                        let function_id = self.prepare(name)?;
                        self.validate_call(function_id, exprs.len())?;
                        **function = FunctionExprPlan::Custom {
                            function_id,
                            exprs: mem::take(exprs),
                        };
                    }
                    FunctionExprPlan::Custom { function_id, exprs } => {
                        self.validate_call(*function_id, exprs.len())?;
                    }
                    _ => {}
                }
                Ok(())
            }
            _ => Ok(()),
        })
    }

    fn prepare(&mut self, name: &str) -> Result<usize> {
        let key = name.to_uppercase();
        if let Some(index) = self.names.get(&key) {
            return Ok(*index);
        }
        let definition = self
            .storage
            .fetch_function(&key)?
            .ok_or_else(|| EvaluateError::UnsupportedFunction(name.to_owned()))?;
        let function = validate_function_definition(
            &definition.func_name,
            &definition.args,
            &definition.body,
        )?;
        let index = self.functions.len();
        self.names.insert(key, index);
        self.functions.push(function);
        Ok(index)
    }

    fn validate_call(&self, index: usize, found: usize) -> Result<()> {
        let function = self
            .functions
            .get(index)
            .ok_or(EvaluateError::InvalidCustomFunctionIndex(index))?;
        let min = function
            .args
            .iter()
            .filter(|arg| arg.default.is_none())
            .count();
        let max = function.args.len();
        if !(min..=max).contains(&found) {
            return Err(EvaluateError::FunctionArgsLengthNotWithinRange {
                name: function.name.clone(),
                expected_minimum: min,
                expected_maximum: max,
                found,
            }
            .into());
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use {
        crate::{
            data::{CustomFunction as Definition, Key, Schema, Value},
            executor::{Payload, execute},
            glue::Glue,
            parse_sql::parse,
            plan::{StatementPlan, StatementPlanKind},
            planner::PlannerError,
            query_builder::Execute,
            result::{Error, Result},
            store::{
                AlterTable, CustomFunction, CustomFunctionMut, Index, IndexMut, Metadata, Planner,
                RowIter, Store, StoreMut, Transaction,
            },
            translate::translate,
        },
        std::{cell::Cell, collections::HashMap},
    };

    #[derive(Default)]
    struct FunctionStorage {
        definitions: HashMap<String, Definition>,
        lookups: Cell<usize>,
    }

    impl CustomFunction for FunctionStorage {
        fn fetch_function<'a>(&'a self, name: &str) -> Result<Option<&'a Definition>> {
            self.lookups.set(self.lookups.get() + 1);
            Ok(self.definitions.get(name))
        }
    }
    impl Store for FunctionStorage {
        fn fetch_schema(&self, _: &str) -> Result<Option<Schema>> {
            panic!("function body must not fetch schemas");
        }

        fn fetch_all_schemas(&self) -> Result<Vec<Schema>> {
            panic!("function body must not fetch metadata");
        }

        fn fetch_data(&self, _: &str, _: &Key) -> Result<Option<Vec<Value>>> {
            panic!("function body must not fetch rows");
        }

        fn scan_data(&self, _: &str) -> Result<RowIter<'_>> {
            panic!("function body must not scan rows");
        }
    }
    impl StoreMut for FunctionStorage {}
    impl Index for FunctionStorage {}
    impl IndexMut for FunctionStorage {}
    impl Metadata for FunctionStorage {}
    impl AlterTable for FunctionStorage {}
    impl CustomFunctionMut for FunctionStorage {}
    impl Transaction for FunctionStorage {}
    impl Planner for FunctionStorage {}

    fn statement(sql: &str) -> StatementPlan {
        translate(&parse(sql).unwrap()[0]).unwrap().into()
    }

    impl FunctionStorage {
        fn define(&mut self, sql: &str) {
            let StatementPlanKind::CreateFunction {
                name,
                args,
                return_,
                ..
            } = statement(sql).kind
            else {
                panic!("expected CREATE FUNCTION");
            };
            self.definitions.insert(
                name.to_uppercase(),
                Definition {
                    func_name: name,
                    args,
                    body: return_,
                },
            );
        }
    }

    fn scalar(payload: Payload) -> Value {
        let Payload::Select { mut rows, .. } = payload else {
            panic!("expected SELECT");
        };
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].len(), 1);
        rows.remove(0).remove(0)
    }

    #[test]
    fn prepares_each_definition_once_including_aggregate_slots() {
        let mut storage = FunctionStorage::default();
        storage.define("CREATE FUNCTION plus_one(x INT) RETURN x + 1");
        storage.define("CREATE FUNCTION twice(x INT) RETURN plus_one(plus_one(x))");
        let plan = storage
            .plan(statement("SELECT SUM(twice(N)) FROM SERIES(3)"))
            .unwrap();
        assert_eq!(plan.functions.len(), 2);
        assert_eq!(storage.lookups.get(), 2);
        for _ in 0..2 {
            assert_eq!(
                scalar(execute(&mut storage, &plan).unwrap()),
                Value::I64(12)
            );
        }
        let replanned = storage.plan(plan.clone()).unwrap();
        assert_eq!(replanned, plan);
        assert_eq!(storage.lookups.get(), 2);
    }

    #[test]
    fn prepared_subqueries_execute_without_storage_access() {
        let mut storage = FunctionStorage::default();
        storage.define("CREATE FUNCTION plus_one(x INT) RETURN x + 1");
        storage.define(
            "CREATE FUNCTION sum_plus(x INT) RETURN (SELECT SUM(plus_one(N)) FROM SERIES(x))",
        );
        let plan = storage.plan(statement("VALUES (sum_plus(3))")).unwrap();
        assert_eq!(storage.lookups.get(), 2);
        assert_eq!(scalar(execute(&mut storage, &plan).unwrap()), Value::I64(9));
        assert_eq!(storage.lookups.get(), 2);
    }

    #[test]
    fn validates_stored_definitions_before_binding_calls() {
        let mut storage = FunctionStorage::default();
        for body in [
            "(SELECT id FROM Item)",
            "(SELECT COUNT(*) FROM GLUE_TABLES)",
            "CASE WHEN true THEN 1 ELSE (SELECT id FROM Item) END",
            "(SELECT 1 FROM SERIES((SELECT id FROM Item)))",
            "(SELECT column1 FROM (VALUES ((SELECT id FROM Item))) v)",
        ] {
            let sql = format!("CREATE FUNCTION leak() RETURN {body}");
            storage.define(&sql);
            assert_eq!(
                storage.plan(statement("SELECT leak()")),
                Err(Error::Planner(PlannerError::StorageAccessInFunctionBody)),
                "{sql}"
            );
        }
        storage.define("CREATE FUNCTION indirect() RETURN leak()");
        assert_eq!(
            storage.plan(statement("SELECT indirect()")),
            Err(Error::Planner(PlannerError::StorageAccessInFunctionBody))
        );
    }

    #[test]
    fn recursive_calls_have_finite_plans() {
        let mut storage = FunctionStorage::default();
        storage.define(
            "CREATE FUNCTION total(x INT) RETURN CASE WHEN x <= 0 THEN 0 ELSE x + total(x - 1) END",
        );
        let plan = storage.plan(statement("SELECT total(5)")).unwrap();
        assert_eq!(plan.functions.len(), 1);
        assert_eq!(
            scalar(execute(&mut storage, &plan).unwrap()),
            Value::I64(15)
        );

        storage.define("CREATE FUNCTION even_value(x INT) RETURN CASE WHEN x = 0 THEN true ELSE odd_value(x - 1) END");
        storage.define("CREATE FUNCTION odd_value(x INT) RETURN CASE WHEN x = 0 THEN false ELSE even_value(x - 1) END");
        let plan = storage.plan(statement("SELECT even_value(4)")).unwrap();
        assert_eq!(plan.functions.len(), 2);
        assert_eq!(
            scalar(execute(&mut storage, &plan).unwrap()),
            Value::Bool(true)
        );
    }

    #[test]
    fn prepared_snapshots_survive_catalog_changes_and_replanning() {
        let mut glue = Glue::new(FunctionStorage::default());
        glue.storage
            .define("CREATE FUNCTION bump(x INT) RETURN x + 1");
        glue.storage
            .define("CREATE FUNCTION call_bump(x INT) RETURN bump(x)");
        let plan = glue.plan("SELECT call_bump(1)").unwrap().remove(0);
        let original = glue.execute_stmt(&plan).unwrap();

        glue.storage
            .define("CREATE FUNCTION bump(x INT) RETURN x + 100");
        assert_eq!(plan.clone().execute(&mut glue).unwrap(), original);
        let fresh = glue.plan("SELECT call_bump(1)").unwrap().remove(0);
        assert_eq!(scalar(glue.execute_stmt(&fresh).unwrap()), Value::I64(101));

        glue.storage.definitions.clear();
        assert_eq!(plan.execute(&mut glue).unwrap(), original);
        assert!(glue.plan("SELECT call_bump(1)").is_err());
        assert!(
            glue.plan("CREATE FUNCTION pending() RETURN 1; SELECT pending()")
                .is_err()
        );
        assert!(!glue.storage.definitions.contains_key("PENDING"));
    }
}
