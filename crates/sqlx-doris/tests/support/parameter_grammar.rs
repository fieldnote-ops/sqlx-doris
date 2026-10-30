//! Independent test grammar and three-valued truth oracle. No driver internals.
use sqlx_doris::sqlx::{
    self, MySql,
    mysql::{
        MySqlArguments,
        types::{MySqlTime, MySqlTimeSign},
    },
    query::Query,
};
#[derive(Clone, Copy, Debug)]
pub enum Domain {
    Bytes,
    Time,
}
#[derive(Clone, Debug)]
pub enum Value {
    Bytes(Option<Vec<u8>>),
    Time(Option<i64>),
    Text(&'static str),
    Literal(&'static str),
    Null,
}
#[derive(Clone, Debug)]
pub enum Expr {
    Compare(Domain, &'static str, Value, bool),
    In(Domain, Vec<Value>, bool),
    Between(Domain, Value, Value, bool),
    And(Box<Expr>, Box<Expr>),
    Or(Box<Expr>, Box<Expr>),
    Not(Box<Expr>),
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Data {
    pub id: i64,
    pub bytes: Option<Vec<u8>>,
    pub time: Option<i64>,
    pub n: i64,
}
pub fn fixture() -> Vec<Data> {
    let bytes = vec![
        None,
        Some(vec![255, 0, 128]),
        Some(b"A".to_vec()),
        Some(b"a".to_vec()),
        Some(b"A ".to_vec()),
        Some(b"A\0".to_vec()),
        Some(vec![]),
        Some((0..=255).collect()),
        Some("中😀'\\\0?; -- payload".as_bytes().to_vec()),
    ];
    let times = [
        None,
        Some(-1),
        Some(0),
        Some(90_000_123_456),
        Some(-90_000_123_456),
        Some(444_444_123_456),
        Some(0),
        Some(3_020_398_999_999),
        Some(1),
    ];
    bytes
        .into_iter()
        .zip(times)
        .enumerate()
        .map(|(i, (bytes, time))| Data {
            id: i as i64,
            bytes,
            time,
            n: 0,
        })
        .collect()
}
pub fn native_time(us: i64) -> MySqlTime {
    let n = us.unsigned_abs();
    MySqlTime::new(
        if us < 0 {
            MySqlTimeSign::Negative
        } else {
            MySqlTimeSign::Positive
        },
        (n / 3_600_000_000) as u32,
        ((n / 60_000_000) % 60) as u8,
        ((n / 1_000_000) % 60) as u8,
        (n % 1_000_000) as u32,
    )
    .unwrap()
}
pub fn query(sql: String, args: &[Value]) -> Query<'static, MySql, MySqlArguments> {
    let mut q = sqlx::query(sqlx::AssertSqlSafe(sql));
    for v in args {
        q = match v {
            Value::Bytes(v) => q.bind(v.clone()),
            Value::Time(v) => q.bind(v.map(native_time)),
            Value::Text(v) => q.bind(*v),
            _ => panic!("literal cannot be a parameter"),
        };
    }
    q
}
#[derive(PartialEq, Eq, PartialOrd, Ord)]
enum Scalar {
    Bytes(Vec<u8>),
    Time(i64),
}
fn scalar(v: &Value, domain: Domain) -> Option<Scalar> {
    match v {
        Value::Bytes(v) => v.clone().map(Scalar::Bytes),
        Value::Time(v) => v.map(Scalar::Time),
        Value::Null => None,
        Value::Text(t) | Value::Literal(t) => Some(match domain {
            Domain::Bytes => Scalar::Bytes(t.as_bytes().to_vec()),
            Domain::Time => Scalar::Time(match *t {
                "25:00:00" => 90_000_000_000,
                "00:00:00" => 0,
                _ => panic!("oracle has no such text duration"),
            }),
        }),
    }
}
fn compare(a: Option<Scalar>, b: Option<Scalar>, op: &str) -> Option<bool> {
    if op == "<=>" {
        return Some(a == b);
    }
    let (a, b) = (a?, b?);
    Some(match op {
        "=" => a == b,
        "<>" | "!=" => a != b,
        "<" => a < b,
        "<=" => a <= b,
        ">" => a > b,
        ">=" => a >= b,
        _ => panic!("operator"),
    })
}
fn and(a: Option<bool>, b: Option<bool>) -> Option<bool> {
    if a == Some(false) || b == Some(false) {
        Some(false)
    } else if a == Some(true) && b == Some(true) {
        Some(true)
    } else {
        None
    }
}
fn or(a: Option<bool>, b: Option<bool>) -> Option<bool> {
    if a == Some(true) || b == Some(true) {
        Some(true)
    } else if a == Some(false) && b == Some(false) {
        Some(false)
    } else {
        None
    }
}
fn col(row: &Data, d: Domain) -> Option<Scalar> {
    match d {
        Domain::Bytes => row.bytes.clone().map(Scalar::Bytes),
        Domain::Time => row.time.map(Scalar::Time),
    }
}
impl Expr {
    pub fn eval(&self, row: &Data) -> Option<bool> {
        match self {
            Self::Compare(d, op, v, reverse) => {
                let (a, b) = (col(row, *d), scalar(v, *d));
                if *reverse {
                    compare(b, a, op)
                } else {
                    compare(a, b, op)
                }
            }
            Self::In(d, values, not) => {
                let mut any = Some(false);
                for v in values {
                    any = or(any, compare(col(row, *d), scalar(v, *d), "="));
                }
                any.map(|x| x != *not)
            }
            Self::Between(d, low, high, not) => and(
                compare(col(row, *d), scalar(low, *d), ">="),
                compare(col(row, *d), scalar(high, *d), "<="),
            )
            .map(|v| v != *not),
            Self::And(a, b) => and(a.eval(row), b.eval(row)),
            Self::Or(a, b) => or(a.eval(row), b.eval(row)),
            Self::Not(a) => a.eval(row).map(|v| !v),
        }
    }
    pub fn render(&self, style: usize, args: &mut Vec<Value>) -> String {
        fn wrap(s: &str, style: usize) -> String {
            format!("{}{}{}", "(".repeat(style), s, ")".repeat(style))
        }
        fn column(d: Domain, style: usize) -> String {
            wrap(
                match (d, style % 2) {
                    (Domain::Bytes, 0) => "r.b",
                    (Domain::Bytes, _) => "r.`b`",
                    (Domain::Time, 0) => "r.t",
                    (Domain::Time, _) => "r.`t`",
                },
                style,
            )
        }
        fn value(v: &Value, style: usize, args: &mut Vec<Value>) -> String {
            match v {
                Value::Literal(v) => format!("'{v}'"),
                Value::Null => "NULL".into(),
                _ => {
                    args.push(v.clone());
                    wrap("?", style)
                }
            }
        }
        let gap = match style {
            0 => " ",
            1 => " /* ? ; 中 */ ",
            _ => "\n",
        };
        match self {
            Self::Compare(d, op, v, reverse) => {
                let c = column(*d, style);
                let v = value(v, style, args);
                if *reverse {
                    format!("{v}{gap}{op}{gap}{c}")
                } else {
                    format!("{c}{gap}{op}{gap}{v}")
                }
            }
            Self::In(d, values, not) => format!(
                "{} {}IN({})",
                column(*d, style),
                if *not { "NOT " } else { "" },
                values
                    .iter()
                    .map(|v| value(v, style, args))
                    .collect::<Vec<_>>()
                    .join(", /* ? */ ")
            ),
            Self::Between(d, low, high, not) => format!(
                "{} {}BETWEEN {} AND {}",
                column(*d, style),
                if *not { "NOT " } else { "" },
                value(low, style, args),
                value(high, style, args)
            ),
            Self::And(a, b) => format!(
                "({}) AND ({})",
                a.render(style, args),
                b.render(style, args)
            ),
            Self::Or(a, b) => format!("({}) OR ({})", a.render(style, args), b.render(style, args)),
            Self::Not(a) => format!("NOT ({})", a.render(style, args)),
        }
    }
}
pub fn corpus() -> Vec<(Expr, usize)> {
    let mut out = Vec::new();
    for domain in [Domain::Bytes, Domain::Time] {
        let ops: &[&str] = match domain {
            Domain::Bytes => &["=", "<>", "!=", "<=>", "<", "<=", ">", ">="],
            Domain::Time => &["=", "<>", "!=", "<=>", "<", "<=", ">", ">="],
        };
        for &op in ops {
            for reverse in [false, true] {
                for style in 0..3 {
                    let v = match (domain, style) {
                        (Domain::Bytes, 0) => Value::Bytes(None),
                        (Domain::Bytes, 1) => Value::Bytes(Some(vec![255, 0, 128])),
                        (Domain::Bytes, _) => Value::Bytes(Some(b"A ".to_vec())),
                        (Domain::Time, 0) => Value::Time(None),
                        (Domain::Time, 1) => Value::Time(Some(-1)),
                        (Domain::Time, _) => Value::Time(Some(90_000_123_456)),
                    };
                    out.push((Expr::Compare(domain, op, v, reverse), style));
                }
            }
        }
    }
    for domain in [Domain::Bytes, Domain::Time] {
        for not in [false, true] {
            for nullable in [false, true] {
                let mut v = match domain {
                    Domain::Bytes => vec![
                        Value::Bytes(Some(vec![255, 0, 128])),
                        Value::Text("A"),
                        Value::Literal("a"),
                    ],
                    Domain::Time => vec![
                        Value::Time(Some(-1)),
                        Value::Time(Some(0)),
                        Value::Text("25:00:00"),
                        Value::Literal("00:00:00"),
                    ],
                };
                if nullable {
                    v.push(match domain {
                        Domain::Bytes => Value::Bytes(None),
                        Domain::Time => Value::Time(None),
                    });
                    v.push(Value::Null);
                }
                out.push((Expr::In(domain, v, not), usize::from(nullable)));
            }
        }
    }
    for i in 0..8 {
        let a = out[i].0.clone();
        let b = out[24 + i].0.clone();
        out.push((Expr::And(Box::new(a.clone()), Box::new(b.clone())), i % 3));
        out.push((
            Expr::Not(Box::new(Expr::Or(Box::new(a), Box::new(b)))),
            i % 3,
        ));
    }
    for not in [false, true] {
        for style in 0..3 {
            for (low, high) in [
                (Value::Bytes(Some(vec![0])), Value::Bytes(Some(vec![255]))),
                (Value::Bytes(None), Value::Bytes(Some(vec![]))),
                (Value::Literal("A"), Value::Bytes(Some(vec![97]))),
                (Value::Bytes(Some(vec![255])), Value::Null),
            ] {
                out.push((Expr::Between(Domain::Bytes, low, high, not), style));
            }
        }
    }
    assert_eq!(out.len(), 144);
    out
}
