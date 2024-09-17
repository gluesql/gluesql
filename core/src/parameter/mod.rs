use crate::{
    ast::{Expr, Placeholder, Query, SetExpr, Statement, Values},
    data::Value,
    result::{Error, Result},
};
use std::borrow::BorrowMut;

impl TryFrom<&Vec<u8>> for Value {
    type Error = Error;
    fn try_from(bytes: &Vec<u8>) -> Result<Self, Self::Error> {
        // TODO: recover Value from bytes.
        Ok(Value::I64(123))
    }
}

impl Into<Vec<u8>> for Value {
    fn into(self) -> Vec<u8> {
        // TODO: serialize Value to bytes.
        vec![0]
    }
}

fn resolve_parameters_expr(x: Expr) -> Result<Expr> {
    match x {
        Expr::Placeholder(p) => match p {
            Placeholder::Text(t) => {
                let v = vec![0, 234];
                return Ok(Expr::Placeholder(Placeholder::Resolved(t.clone(), v)));
                // return Ok(Some(

                // ));
            }
            _ => {}
        },
        Expr::IsNull(bv) => {
            // let mut v = bv.as_mut();
            let v = resolve_parameters_expr(*bv)?;
            return Ok(Expr::IsNull(Box::new(v)));
            // *x = Expr::IsNull(Box::new(*v));
            // match resolve_parameters_expr(&*bv)? {
            //     Some(v) => {
            //         // return Ok(Some());
            //         *x = Expr::IsNull(Box::new(v));
            //     }
            //     _ => {}
            // }
        }
        Expr::IsNotNull(bv) => {
            // match resolve_parameters_expr(&*bv)? {
            //     Some(v) => {
            //         // return Ok(Some());
            //         *x = Expr::IsNotNull(Box::new(v));
            //     }
            //     _ => {}
            // }
        }
        // Expr::InList {
        //     expr,
        //     list,
        //     negated: _,
        // } => {

        // }
        _ => {}
    }
    Ok(x)
}

fn resolve_parameters_query(q: Query) -> Result<()> {
    match q.body {
        SetExpr::Values(values) => {
            let Values(exprs) = values;
            for i in 0..exprs.len() {
                let g = &mut exprs[i];
                let len = g.len();
                for j in 0..len {
                    let mut v = g.remove(j);
                    resolve_parameters_expr(&mut v)?;
                    g.insert(j, v);
                    // match resolve_parameters_expr(&v)? {
                    //     Some(v1) => {
                    //         g.insert(j, v1);
                    //     }
                    //     _ => {}
                    // }
                }
            }
        }
        _ => {}
    }
    Ok(())
}

pub fn resolve_parameters(s: Statement) -> Result<Statement> {
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
        Statement::Update {
            table_name: _,
            assignments: _,
            selection,
        } => {
            if let Some(expr) = selection {
                resolve_parameters_expr(expr)?;
                // match resolve_parameters_expr(expr)? {
                //     Some(v) => {
                //         println!("resolved as {:?}", &v);
                //         *selection = Some(v);
                //     }
                //     _ => {}
                // }
            }
        }
        _ => {}
    }
    Ok(s)
}
