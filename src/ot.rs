//! ot.js-style text operations over Unicode code points (PROTOCOL.md section 3).
//!
//! An independent implementation of the published algorithm (ot.js is MIT).

use serde_json::Value;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum C {
    Ret(usize),
    Ins(String),
    Del(usize),
}

pub type Op = Vec<C>;

#[derive(Default)]
pub struct Builder {
    pub ops: Op,
}

impl Builder {
    pub fn retain(&mut self, n: usize) {
        if n == 0 {
            return;
        }
        if let Some(C::Ret(m)) = self.ops.last_mut() {
            *m += n;
        } else {
            self.ops.push(C::Ret(n));
        }
    }
    pub fn insert(&mut self, s: &str) {
        if s.is_empty() {
            return;
        }
        let l = self.ops.len();
        if let Some(C::Ins(t)) = self.ops.last_mut() {
            t.push_str(s);
        } else if matches!(self.ops.last(), Some(C::Del(_))) {
            // canonical order: insert before an adjacent delete
            if l >= 2 {
                if let C::Ins(t) = &mut self.ops[l - 2] {
                    t.push_str(s);
                    return;
                }
            }
            self.ops.insert(l - 1, C::Ins(s.to_string()));
        } else {
            self.ops.push(C::Ins(s.to_string()));
        }
    }
    pub fn delete(&mut self, n: usize) {
        if n == 0 {
            return;
        }
        if let Some(C::Del(m)) = self.ops.last_mut() {
            *m += n;
        } else {
            self.ops.push(C::Del(n));
        }
    }
}

pub fn normalize(op: &[C]) -> Op {
    let mut b = Builder::default();
    for c in op {
        match c {
            C::Ret(n) => b.retain(*n),
            C::Ins(s) => b.insert(s),
            C::Del(n) => b.delete(*n),
        }
    }
    b.ops
}

pub fn base_len(op: &[C]) -> usize {
    op.iter()
        .map(|c| match c {
            C::Ret(n) | C::Del(n) => *n,
            C::Ins(_) => 0,
        })
        .sum()
}

pub fn is_noop(op: &[C]) -> bool {
    op.iter().all(|c| matches!(c, C::Ret(_)))
}

pub fn apply(doc: &[char], op: &[C]) -> Result<Vec<char>, String> {
    if base_len(op) != doc.len() {
        return Err(format!(
            "base length {} does not match document length {}",
            base_len(op),
            doc.len()
        ));
    }
    let mut out = Vec::with_capacity(doc.len());
    let mut i = 0;
    for c in op {
        match c {
            C::Ins(s) => out.extend(s.chars()),
            C::Ret(n) => {
                out.extend_from_slice(&doc[i..i + n]);
                i += n;
            }
            C::Del(n) => i += n,
        }
    }
    Ok(out)
}

/// A walker over the components that can consume part of the current one.
struct Cur<'a> {
    it: std::slice::Iter<'a, C>,
    cur: Option<C>,
}

impl<'a> Cur<'a> {
    fn new(op: &'a [C]) -> Self {
        let mut it = op.iter();
        let cur = it.next().cloned();
        Cur { it, cur }
    }
    fn advance(&mut self) {
        self.cur = self.it.next().cloned();
    }
    /// Consume `m` units of the current Ret/Del component.
    fn eat(&mut self, m: usize) {
        match &mut self.cur {
            Some(C::Ret(n)) | Some(C::Del(n)) => {
                *n -= m;
                if *n == 0 {
                    self.advance();
                }
            }
            _ => unreachable!(),
        }
    }
}

/// `(a', b')` with `apply(apply(d,a),b') == apply(apply(d,b),a')`; `a`'s insert wins ties.
pub fn transform(a: &[C], b: &[C]) -> Result<(Op, Op), String> {
    let a = normalize(a);
    let b = normalize(b);
    if base_len(&a) != base_len(&b) {
        return Err("transform: base lengths differ".into());
    }
    let (mut ap, mut bp) = (Builder::default(), Builder::default());
    let (mut x, mut y) = (Cur::new(&a), Cur::new(&b));
    loop {
        if let Some(C::Ins(s)) = &x.cur {
            ap.insert(s);
            bp.retain(s.chars().count());
            x.advance();
            continue;
        }
        if let Some(C::Ins(s)) = &y.cur {
            ap.retain(s.chars().count());
            bp.insert(s);
            y.advance();
            continue;
        }
        match (&x.cur, &y.cur) {
            (None, None) => break,
            (Some(C::Ret(p)), Some(C::Ret(q))) => {
                let m = (*p).min(*q);
                ap.retain(m);
                bp.retain(m);
                x.eat(m);
                y.eat(m);
            }
            (Some(C::Del(p)), Some(C::Del(q))) => {
                let m = (*p).min(*q);
                x.eat(m);
                y.eat(m);
            }
            (Some(C::Del(p)), Some(C::Ret(q))) => {
                let m = (*p).min(*q);
                ap.delete(m);
                x.eat(m);
                y.eat(m);
            }
            (Some(C::Ret(p)), Some(C::Del(q))) => {
                let m = (*p).min(*q);
                bp.delete(m);
                x.eat(m);
                y.eat(m);
            }
            _ => return Err("transform: operations too short".into()),
        }
    }
    Ok((ap.ops, bp.ops))
}

pub fn from_json(v: &Value) -> Result<Op, String> {
    let arr = v.as_array().ok_or("op must be an array")?;
    let mut raw = Vec::new();
    for c in arr {
        if let Some(s) = c.as_str() {
            raw.push(C::Ins(s.to_string()));
        } else if let Some(n) = c.as_i64() {
            if n > 0 {
                raw.push(C::Ret(n as usize));
            } else if n < 0 {
                raw.push(C::Del((-n) as usize));
            }
        } else {
            return Err(format!("bad component {c}"));
        }
    }
    Ok(normalize(&raw))
}

pub fn to_json(op: &[C]) -> Value {
    Value::Array(
        op.iter()
            .map(|c| match c {
                C::Ret(n) => Value::from(*n as i64),
                C::Ins(s) => Value::from(s.as_str()),
                C::Del(n) => Value::from(-(*n as i64)),
            })
            .collect(),
    )
}

/// Single-replacement op turning `old` into `new` (common prefix/suffix).
pub fn diff(old: &[char], new: &[char]) -> Op {
    let mut p = 0;
    while p < old.len() && p < new.len() && old[p] == new[p] {
        p += 1;
    }
    let mut s = 0;
    while s < old.len() - p && s < new.len() - p && old[old.len() - 1 - s] == new[new.len() - 1 - s]
    {
        s += 1;
    }
    let mut b = Builder::default();
    b.retain(p);
    b.delete(old.len() - p - s);
    b.insert(&new[p..new.len() - s].iter().collect::<String>());
    b.retain(s);
    b.ops
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cs(s: &str) -> Vec<char> {
        s.chars().collect()
    }

    #[test]
    fn spec_example() {
        let op = vec![C::Ret(1), C::Ins("a".into()), C::Del(2), C::Ret(2)];
        assert_eq!(apply(&cs("hello"), &op).unwrap(), cs("halo"));
    }

    #[test]
    fn tie_a_first() {
        let a = vec![C::Ins("A".into()), C::Ret(1)];
        let b = vec![C::Ins("B".into()), C::Ret(1)];
        let (a2, b2) = transform(&a, &b).unwrap();
        let d = cs("x");
        let l = apply(&apply(&d, &a).unwrap(), &b2).unwrap();
        let r = apply(&apply(&d, &b).unwrap(), &a2).unwrap();
        assert_eq!(l, r);
        assert_eq!(l, cs("ABx"));
    }

    #[test]
    fn insert_before_delete() {
        let mut b = Builder::default();
        b.delete(2);
        b.insert("x");
        assert_eq!(b.ops, vec![C::Ins("x".into()), C::Del(2)]);
    }

    #[test]
    fn diff_roundtrip() {
        let (o, n) = (cs("a😀bc"), cs("a😀xxc"));
        assert_eq!(apply(&o, &diff(&o, &n)).unwrap(), n);
    }
}
