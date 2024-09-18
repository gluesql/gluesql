use crate::{
    ast::{Expr, Placeholder, Query, SelectItem, SetExpr, Statement, Values},
    result::Result,
};

mod wire;

fn resolve_parameters_expr(x: &mut Expr) -> Result<()> {
    match x {
        Expr::Placeholder(p) => match p {
            Placeholder::Text(t) => {
                println!("^^^^ resolving {} ^^^^", &t);
                let v = vec![0, 234];
                *x = Expr::Placeholder(Placeholder::Resolved(t.clone(), v));
            }
            _ => {}
        },
        Expr::IsNull(bv) => {
            let mut v = bv.as_mut();
            resolve_parameters_expr(&mut v)?;
        }
        Expr::IsNotNull(bv) => {
            let mut v = bv.as_mut();
            resolve_parameters_expr(&mut v)?;
        }
        Expr::InList {
            expr,
            list,
            negated: _,
        } => {
            let mut x = expr.as_mut();
            resolve_parameters_expr(&mut x)?;
            for i in 0..list.len() {
                let x = &mut list[i];
                resolve_parameters_expr(x)?;
            }
        }
        Expr::InSubquery {
            expr,
            subquery,
            negated: _,
        } => {
            let mut x = expr.as_mut();
            resolve_parameters_expr(&mut x)?;
            let mut q = subquery.as_mut();
            resolve_parameters_query(&mut q)?;
        }
        Expr::Between {
            expr,
            negated: _,
            low,
            high,
        } => {
            let mut x = expr.as_mut();
            resolve_parameters_expr(&mut x)?;
            let mut vl = low.as_mut();
            let mut vh = high.as_mut();
            resolve_parameters_expr(&mut vl)?;
            resolve_parameters_expr(&mut vh)?;
        }
        Expr::Like {
            expr,
            negated: _,
            pattern,
        } => {
            let mut x = expr.as_mut();
            resolve_parameters_expr(&mut x)?;
            let mut p = pattern.as_mut();
            resolve_parameters_expr(&mut p)?;
        }
        Expr::ILike {
            expr,
            negated: _,
            pattern,
        } => {
            let mut x = expr.as_mut();
            resolve_parameters_expr(&mut x)?;
            let mut p = pattern.as_mut();
            resolve_parameters_expr(&mut p)?;
        }
        Expr::BinaryOp { left, op: _, right } => {
            let mut vl = left.as_mut();
            resolve_parameters_expr(&mut vl)?;
            let mut vr = right.as_mut();
            resolve_parameters_expr(&mut vr)?;
        }
        Expr::UnaryOp { op: _, expr } => {
            let mut x = expr.as_mut();
            resolve_parameters_expr(&mut x)?;
        }
        Expr::Nested(bv) => {
            let mut v = bv.as_mut();
            resolve_parameters_expr(&mut v)?;
        }
        Expr::Exists {
            subquery,
            negated: _,
        } => {
            let mut q = subquery.as_mut();
            resolve_parameters_query(&mut q)?;
        }
        Expr::Subquery(bq) => {
            let mut q = bq.as_mut();
            resolve_parameters_query(&mut q)?;
        }
        Expr::Case {
            operand,
            when_then,
            else_result,
        } => {
            if let Some(bv) = operand {
                let mut v = bv.as_mut();
                resolve_parameters_expr(&mut v)?;
            }
            for i in 0..when_then.len() {
                let (wh, th) = &mut when_then[i];
                resolve_parameters_expr(wh)?;
                resolve_parameters_expr(th)?;
            }
            if let Some(bv) = else_result {
                let mut v = bv.as_mut();
                resolve_parameters_expr(&mut v)?;
            }
        }
        Expr::ArrayIndex { obj, indexes } => {
            let mut v = obj.as_mut();
            resolve_parameters_expr(&mut v)?;
            for i in 0..indexes.len() {
                resolve_parameters_expr(&mut indexes[i])?;
            }
        }
        Expr::Interval {
            expr,
            leading_field: _,
            last_field: _,
        } => {
            let mut x = expr.as_mut();
            resolve_parameters_expr(&mut x)?;
        }
        Expr::Array { elem } => {
            for i in 0..elem.len() {
                resolve_parameters_expr(&mut elem[i])?;
            }
        }
        _ => {}
    }
    Ok(())
}

fn resolve_parameters_query(q: &mut Query) -> Result<()> {
    match &mut q.body {
        SetExpr::Select(bs) => {
            let s = bs.as_mut();
            if let Some(ref mut selection) = s.selection {
                resolve_parameters_expr(selection)?;
            }
            for i in 0..s.projection.len() {
                let select_item = &mut s.projection[i];
                match select_item {
                    SelectItem::Expr { expr, label: _ } => {
                        resolve_parameters_expr(expr)?;
                    }
                    _ => {}
                }
            }
            for i in 0..s.group_by.len() {
                let v = &mut s.group_by[i];
                resolve_parameters_expr(v)?;
            }
            if let Some(having) = &mut s.having {
                resolve_parameters_expr(having)?;
            }
        }
        SetExpr::Values(values) => {
            let Values(exprs) = values;
            for i in 0..exprs.len() {
                let g = &mut exprs[i];
                for j in 0..g.len() {
                    resolve_parameters_expr(&mut g[j])?;
                }
            }
        }
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
        Statement::Update {
            table_name: _,
            assignments,
            selection,
        } => {
            for i in 0..assignments.len() {
                resolve_parameters_expr(&mut assignments[i].value)?;
            }
            if let Some(expr) = selection {
                resolve_parameters_expr(expr)?;
            }
        }
        Statement::Delete {
            table_name: _,
            selection,
        } => {
            if let Some(ref mut v) = selection {
                resolve_parameters_expr(v)?;
            }
        }
        Statement::CreateTable {
            if_not_exists: _,
            name: _,
            columns: _,
            source,
            engine: _,
            foreign_keys: _,
            comment: _,
        } => {
            if let Some(bq) = source {
                let mut q = bq.as_mut();
                resolve_parameters_query(&mut q)?;
            }
        }
        _ => {}
    }
    Ok(())
}
