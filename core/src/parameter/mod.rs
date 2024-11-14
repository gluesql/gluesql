use crate::{
    ast::{Expr, Placeholder, Query, SelectItem, SetExpr, Statement, Values},
    result::Result,
};

mod error;

pub use error::ParameterError;

mod wire;

pub trait Parameters {
    fn get(&self, i: usize) -> Option<Vec<u8>>;
}

fn placeholder_to_usize(p: &str) -> usize {
    let i_str = String::from_utf8(p.as_bytes()[1..].to_vec()).unwrap();
    i_str.as_str().parse::<usize>().unwrap_or(0)
}

fn resolve(ps: &dyn Parameters, p: &mut Placeholder) -> Result<()> {
    let t = match p {
        Placeholder::Text(t) => t.as_str(),
        Placeholder::Resolved(t, _) => t.as_str(),
    };
    let i: usize = placeholder_to_usize(t);
    match ps.get(i) {
        Some(v) => {
            *p = Placeholder::Resolved(t.to_owned(), v);
            Ok(())
        }
        None => Err(ParameterError::Notfound(t.to_owned()).into()),
    }
}

fn resolve_parameters_expr(ps: &dyn Parameters, x: &mut Expr) -> Result<()> {
    match x {
        Expr::Placeholder(p) => resolve(ps, p)?,
        Expr::IsNull(bv) => {
            let v = bv.as_mut();
            resolve_parameters_expr(ps, v)?;
        }
        Expr::IsNotNull(bv) => {
            let v = bv.as_mut();
            resolve_parameters_expr(ps, v)?;
        }
        Expr::InList {
            expr,
            list,
            negated: _,
        } => {
            let x = expr.as_mut();
            resolve_parameters_expr(ps, x)?;
            for x in list.iter_mut() {
                resolve_parameters_expr(ps, x)?;
            }
        }
        Expr::InSubquery {
            expr,
            subquery,
            negated: _,
        } => {
            let x = expr.as_mut();
            resolve_parameters_expr(ps, x)?;
            let q = subquery.as_mut();
            resolve_parameters_query(ps, q)?;
        }
        Expr::Between {
            expr,
            negated: _,
            low,
            high,
        } => {
            let x = expr.as_mut();
            resolve_parameters_expr(ps, x)?;
            let vl = low.as_mut();
            let vh = high.as_mut();
            resolve_parameters_expr(ps, vl)?;
            resolve_parameters_expr(ps, vh)?;
        }
        Expr::Like {
            expr,
            negated: _,
            pattern,
        } => {
            let x = expr.as_mut();
            resolve_parameters_expr(ps, x)?;
            let p = pattern.as_mut();
            resolve_parameters_expr(ps, p)?;
        }
        Expr::ILike {
            expr,
            negated: _,
            pattern,
        } => {
            let x = expr.as_mut();
            resolve_parameters_expr(ps, x)?;
            let p = pattern.as_mut();
            resolve_parameters_expr(ps, p)?;
        }
        Expr::BinaryOp { left, op: _, right } => {
            let vl = left.as_mut();
            resolve_parameters_expr(ps, vl)?;
            let vr = right.as_mut();
            resolve_parameters_expr(ps, vr)?;
        }
        Expr::UnaryOp { op: _, expr } => {
            let x = expr.as_mut();
            resolve_parameters_expr(ps, x)?;
        }
        Expr::Nested(bv) => {
            let v = bv.as_mut();
            resolve_parameters_expr(ps, v)?;
        }
        Expr::Exists {
            subquery,
            negated: _,
        } => {
            let q = subquery.as_mut();
            resolve_parameters_query(ps, q)?;
        }
        Expr::Subquery(bq) => {
            let q = bq.as_mut();
            resolve_parameters_query(ps, q)?;
        }
        Expr::Case {
            operand,
            when_then,
            else_result,
        } => {
            if let Some(bv) = operand {
                let v = bv.as_mut();
                resolve_parameters_expr(ps, v)?;
            }
            for (wh, th) in when_then.iter_mut() {
                resolve_parameters_expr(ps, wh)?;
                resolve_parameters_expr(ps, th)?;
            }
            if let Some(bv) = else_result {
                let v = bv.as_mut();
                resolve_parameters_expr(ps, v)?;
            }
        }
        Expr::ArrayIndex { obj, indexes } => {
            let v = obj.as_mut();
            resolve_parameters_expr(ps, v)?;
            for x in indexes.iter_mut() {
                resolve_parameters_expr(ps, x)?;
            }
        }
        Expr::Interval {
            expr,
            leading_field: _,
            last_field: _,
        } => {
            let x = expr.as_mut();
            resolve_parameters_expr(ps, x)?;
        }
        Expr::Array { elem } => {
            for x in elem.iter_mut() {
                resolve_parameters_expr(ps, x)?;
            }
        }
        _ => {}
    }
    Ok(())
}

fn resolve_parameters_query(ps: &dyn Parameters, q: &mut Query) -> Result<()> {
    match &mut q.body {
        SetExpr::Select(bs) => {
            let s = bs.as_mut();
            if let Some(ref mut selection) = s.selection {
                resolve_parameters_expr(ps, selection)?;
            }
            for select_item in s.projection.iter_mut() {
                if let SelectItem::Expr { expr, label: _ } = select_item {
                    resolve_parameters_expr(ps, expr)?;
                }
            }
            for x in s.group_by.iter_mut() {
                resolve_parameters_expr(ps, x)?;
            }
            if let Some(having) = &mut s.having {
                resolve_parameters_expr(ps, having)?;
            }
        }
        SetExpr::Values(values) => {
            let Values(exprs) = values;
            for g in exprs.iter_mut() {
                for x in g.iter_mut() {
                    resolve_parameters_expr(ps, x)?;
                }
            }
        }
    }
    Ok(())
}

pub fn resolve_parameters(ps: &dyn Parameters, s: &mut Statement) -> Result<()> {
    match s {
        Statement::Query(query) => {
            resolve_parameters_query(ps, query)?;
        }
        Statement::Insert {
            table_name: _,
            columns: _,
            source,
        } => {
            resolve_parameters_query(ps, source)?;
        }
        Statement::Update {
            table_name: _,
            assignments,
            selection: Some(expr),
        } => {
            for x in assignments.iter_mut() {
                resolve_parameters_expr(ps, &mut x.value)?;
            }
            resolve_parameters_expr(ps, expr)?;
        }
        Statement::Delete {
            table_name: _,
            selection: Some(ref mut v),
        } => {
            resolve_parameters_expr(ps, v)?;
        }
        Statement::CreateTable {
            if_not_exists: _,
            name: _,
            columns: _,
            source: Some(bq),
            engine: _,
            foreign_keys: _,
            comment: _,
        } => {
            let q = bq.as_mut();
            resolve_parameters_query(ps, q)?;
        }
        _ => {}
    }
    Ok(())
}
