use crate::{
    ast::{Expr, Placeholder, Query, SetExpr, Statement, Values},
    data::Value,
    result::{Error, Result},
};

impl TryFrom<&Vec<u8>> for Value {
    type Error = Error;
    fn try_from(bytes: &Vec<u8>) -> Result<Self, Self::Error> {
        // TODO: recover Value from bytes.
        Ok(Value::I64(12345))
    }
}

impl Into<Vec<u8>> for Value {
    fn into(self) -> Vec<u8> {
        // TODO: serialize Value to bytes.
        vec![0]
    }
}

fn resolve_parameters_expr(x: Expr) -> Result<Expr> {
    match &x {
        Expr::Placeholder(p) => match p {
            Placeholder::Text(t) => {
                let v = vec![0, 234];
                return Ok(Expr::Placeholder(Placeholder::Resolved(t.clone(), v)));
            }
            _ => {}
        },
        _ => {}
    }
    Ok(x)
}

fn resolve_parameters_query(q: &mut Query) -> Result<()> {
    match &mut q.body {
        SetExpr::Values(values) => {
            let Values(exprs) = values;
            for i in 0..exprs.len() {
                let g = &mut exprs[i];
                let len = g.len();
                for j in 0..len {
                    let v = g.remove(j);
                    let v1 = resolve_parameters_expr(v)?;
                    g.insert(j, v1);
                }
            }
        }
        _ => {}
    }
    Ok(())
}

pub fn resolve_parameters(s: &mut Statement) -> Result<()> {
    match s {
        Statement::Query(query) => {
            resolve_parameters_query(query)?;
        }
        Statement::Insert {
            table_name: _,
            columns: _,
            source,
        } => {
            resolve_parameters_query(source)?;
        }
        _ => {}
    }
    Ok(())
}
