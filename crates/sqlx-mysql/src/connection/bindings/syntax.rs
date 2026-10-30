//! Finite, source-preserving SQL structure for parameter adaptation.
use super::{invalid, Error};

#[derive(Debug, PartialEq)]
pub(super) struct Token {
    pub(super) start: usize,
    pub(super) end: usize,
    pub(super) symbol: Option<u8>,
}
impl Token {
    pub(super) fn word(&self, sql: &str, word: &str) -> bool {
        self.symbol.is_none() && sql[self.start..self.end].eq_ignore_ascii_case(word)
    }
}
pub(super) struct Scan {
    pub(super) sites: Vec<usize>,
    pub(super) tokens: Vec<Token>,
}

/// Source positions of bind markers in the finite SELECT pagination grammar.
/// Doris accepts bare integer tokens here, not placeholders or parenthesized
/// expressions. Leave unrelated SQL on its original protocol path.
pub(super) fn pagination_sites(sql: &str) -> Result<Vec<usize>, Error> {
    let lexical = match scan(sql, true, false) {
        Ok(v) => v,
        Err(_) => return Ok(Vec::new()),
    };
    let t = &lexical.tokens;
    if lexical.sites.is_empty() || !t.first().is_some_and(|v| v.word(sql, "SELECT")) {
        return Ok(Vec::new());
    }
    let mut result = Vec::new();
    for (index, token) in t.iter().enumerate() {
        if qualified(t, index) || !token.word(sql, "LIMIT") {
            continue;
        }
        let scalar = |v: &Token| {
            v.symbol == Some(b'?')
                || v.symbol.is_none() && sql[v.start..v.end].bytes().all(|c| c.is_ascii_digit())
        };
        let Some(value) = t.get(index + 1).filter(|v| scalar(v)) else {
            // Unknown forms retain native server validation.
            continue;
        };
        let mut values = Vec::new();
        if value.symbol == Some(b'?') {
            values.push(value.start);
        }
        let mut i = index + 2;
        if t.get(i)
            .is_some_and(|v| v.word(sql, "OFFSET") || v.symbol == Some(b','))
        {
            i += 1;
            let Some(value) = t.get(i).filter(|v| scalar(v)) else {
                return Err(invalid(
                    "pagination requires scalar integer markers or literals",
                ));
            };
            if value.symbol == Some(b'?') {
                values.push(value.start);
            }
            i += 1;
        }
        if !values.is_empty() {
            if t.get(i)
                .is_some_and(|v| !matches!(v.symbol, Some(b')' | b';')))
            {
                return Err(invalid("unsupported bound pagination suffix"));
            }
            result.extend(values);
        }
    }
    if !result.is_empty() {
        // Check quote modes, statement boundaries and paired parentheses before
        // any SQL-mode query or business packet is sent.
        Syntax::parse(sql)?;
    }
    Ok(result)
}

/// A deliberately finite model of the parameter expressions we adapt. Unknown
/// expressions stay opaque; the adapter rejects them when byte semantics would
/// otherwise be guessed. It does not validate arbitrary server SQL.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum StatementKind {
    Select,
    Insert,
    Update,
    Delete,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct Span {
    pub(super) start: usize,
    pub(super) end: usize,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Comparison {
    Equal,
    NotEqual,
    NullSafe,
    Ordered,
}
#[derive(Debug)]
pub(super) enum Expression {
    Assignment,
    NullTest,
    Comparison {
        op: Comparison,
        column: Option<Span>,
        boundary: bool,
    },
    Value {
        predicate: bool,
        operator_nearby: bool,
    },
}
#[derive(Debug)]
pub(super) struct Parameter {
    pub(super) index: usize,
    pub(super) token: usize,
    pub(super) expression: Expression,
    pub(super) in_case: bool,
    pub(super) explicit_hex: bool,
}
#[derive(Debug)]
pub(super) enum Scalar {
    Parameter(usize),
    String,
    Null,
    Opaque,
}
#[derive(Debug)]
pub(super) struct Candidate {
    pub(super) span: Span,
    pub(super) scalar: Scalar,
}
#[derive(Debug)]
pub(super) struct Membership {
    pub(super) keyword: usize,
    pub(super) open: usize,
    pub(super) close: usize,
    pub(super) subquery: bool,
    pub(super) candidates: Vec<Candidate>,
}
#[derive(Debug)]
pub(super) struct Between {
    // Token interval includes the subject and both bounds. Opaque expressions
    // retain their markers here so byte adaptation can fail before any send.
    pub(super) start: usize,
    pub(super) end: usize,
    pub(super) column: Option<Span>,
    pub(super) lower: Candidate,
    pub(super) upper: Candidate,
    pub(super) null_test: bool,
    pub(super) boundary: bool,
}
pub(super) struct Syntax {
    pub(super) scan: Scan,
    pub(super) pairs: Vec<usize>,
    pub(super) kind: StatementKind,
    pub(super) parameters: Vec<Parameter>,
    pub(super) indices: Vec<Option<usize>>,
    pub(super) memberships: Vec<Membership>,
    pub(super) betweens: Vec<Between>,
}
impl Syntax {
    pub(super) fn parse(sql: &str) -> Result<Self, Error> {
        let scan = scan(sql, true, false)?;
        if scan.tokens != self::scan(sql, false, false)?.tokens {
            return Err(invalid("parameter SQL layout depends on backslash mode"));
        }
        let t = &scan.tokens;
        let kind = match t.first() {
            Some(v) if v.word(sql, "SELECT") => StatementKind::Select,
            Some(v) if v.word(sql, "INSERT") => StatementKind::Insert,
            Some(v) if v.word(sql, "UPDATE") => StatementKind::Update,
            Some(v) if v.word(sql, "DELETE") => StatementKind::Delete,
            _ => {
                return Err(invalid(
                    "parameter adaptation supports SELECT/INSERT/UPDATE/DELETE only",
                ))
            }
        };
        let pairs = parentheses(t)?;
        let (predicates, assignments) = clause_context(sql, t)?;
        let mut parameters = Vec::new();
        let mut indices = vec![None; t.len()];
        let mut cases = 0usize;
        for (i, token) in t.iter().enumerate() {
            if !qualified(t, i) && token.word(sql, "CASE") {
                cases += 1;
            }
            if !qualified(t, i) && token.word(sql, "END") {
                cases = cases.saturating_sub(1);
            }
            if token.symbol != Some(b'?') {
                continue;
            }
            let index = parameters.len();
            indices[i] = Some(index);
            let (start, end) = parameter_span(sql, t, &pairs, i);
            let before = comparison_before(sql, t, start);
            let after = comparison_at(sql, t, end);
            let assignment = before.is_some_and(|op| assignments[op]);
            let comparison = if let Some(op) = before.filter(|_| !assignment) {
                Some((
                    op,
                    start,
                    identifier_before(sql, t, &pairs, op),
                    edge_after(sql, t, end),
                ))
            } else {
                after.filter(|_| !assignments[end]).map(|after| {
                    (
                        end,
                        after,
                        identifier_after(sql, t, &pairs, after),
                        edge_before(sql, t, start),
                    )
                })
            };
            let null_end = if t.get(end).is_some_and(|v| v.word(sql, "IS")) {
                let value =
                    end + 1 + usize::from(t.get(end + 1).is_some_and(|v| v.word(sql, "NOT")));
                t.get(value)
                    .is_some_and(|v| v.word(sql, "NULL"))
                    .then_some(value + 1)
            } else {
                None
            };
            let expression =
                if null_end.is_some_and(|e| edge_before(sql, t, start) && edge_after(sql, t, e)) {
                    Expression::NullTest
                } else if let Some((a, b, column, boundary)) = comparison {
                    let op = match &sql[t[a].start..t[b - 1].end] {
                        "=" => Comparison::Equal,
                        "<>" | "!=" => Comparison::NotEqual,
                        "<=>" => Comparison::NullSafe,
                        _ => Comparison::Ordered,
                    };
                    Expression::Comparison {
                        op,
                        column: column.map(|(a, b)| Span {
                            start: t[a].start,
                            end: t[b - 1].end,
                        }),
                        boundary,
                    }
                } else if assignment {
                    Expression::Assignment
                } else {
                    Expression::Value {
                        predicate: predicates[i],
                        operator_nearby: (start > 0 && comparison_symbol(&t[start - 1]))
                            || t.get(end).is_some_and(comparison_symbol)
                            || enclosing_comparison(sql, t, &pairs, i)
                            || t.get(end).is_some_and(|v| v.word(sql, "IN"))
                            || (t.get(end).is_some_and(|v| v.word(sql, "NOT"))
                                && t.get(end + 1).is_some_and(|v| v.word(sql, "IN"))),
                    }
                };
            parameters.push(Parameter {
                index,
                token: i,
                expression,
                in_case: cases > 0,
                explicit_hex: inside_hex(sql, t, &pairs, i),
            });
        }
        let mut memberships = Vec::new();
        for (i, token) in t.iter().enumerate() {
            if qualified(t, i)
                || !token.word(sql, "IN")
                || !t.get(i + 1).is_some_and(|v| v.symbol == Some(b'('))
            {
                continue;
            }
            let open = i + 1;
            let close = pairs[open];
            let subquery = t.get(open + 1).is_some_and(|v| v.word(sql, "SELECT"));
            let mut candidates = Vec::new();
            if !subquery {
                let mut start = open + 1;
                loop {
                    let mut end = start;
                    while end < close && t[end].symbol != Some(b',') {
                        end = if t[end].symbol == Some(b'(') {
                            pairs[end] + 1
                        } else {
                            end + 1
                        };
                    }
                    candidates.push(candidate(sql, t, &pairs, &indices, start, end));
                    if end == close {
                        break;
                    }
                    start = end + 1;
                }
            }
            memberships.push(Membership {
                keyword: i,
                open,
                close,
                subquery,
                candidates,
            });
        }
        let mut betweens = Vec::new();
        for (i, token) in t.iter().enumerate() {
            if qualified(t, i) || !token.word(sql, "BETWEEN") {
                continue;
            }
            let subject_end = if i > 0 && t[i - 1].word(sql, "NOT") {
                i - 1
            } else {
                i
            };
            let mut start = subject_end;
            while start > 0 && !edge_before(sql, t, start) {
                start = if t[start - 1].symbol == Some(b')') {
                    pairs[start - 1]
                } else {
                    start - 1
                };
            }
            let separator = bound_end(sql, t, &pairs, i + 1);
            let upper_start = if t.get(separator).is_some_and(|v| v.word(sql, "AND")) {
                separator + 1
            } else {
                separator
            };
            let upper_end = bound_end(sql, t, &pairs, upper_start);
            let mut end = upper_end;
            let has_suffix = t.get(end).is_some_and(|v| v.word(sql, "IS"));
            if has_suffix {
                end += 1;
                if t.get(end).is_some_and(|v| v.word(sql, "NOT")) {
                    end += 1;
                }
            }
            let null_test = has_suffix && t.get(end).is_some_and(|v| v.word(sql, "NULL"));
            if null_test {
                end += 1;
            }
            betweens.push(Between {
                start,
                end,
                column: identifier_before(sql, t, &pairs, subject_end).map(|(a, b)| Span {
                    start: t[a].start,
                    end: t[b - 1].end,
                }),
                lower: candidate(sql, t, &pairs, &indices, i + 1, separator),
                upper: candidate(sql, t, &pairs, &indices, upper_start, upper_end),
                null_test,
                boundary: (!has_suffix || null_test) && edge_after(sql, t, end),
            });
        }
        Ok(Self {
            scan,
            pairs,
            kind,
            parameters,
            indices,
            memberships,
            betweens,
        })
    }
}

fn candidate(
    sql: &str,
    t: &[Token],
    pairs: &[usize],
    indices: &[Option<usize>],
    start: usize,
    end: usize,
) -> Candidate {
    let (mut a, mut b) = (start, end);
    while a < b && t[a].symbol == Some(b'(') && pairs[a] + 1 == b {
        a += 1;
        b -= 1;
    }
    let scalar = if b != a + 1 {
        Scalar::Opaque
    } else if let Some(p) = indices[a] {
        Scalar::Parameter(p)
    } else if t[a].symbol == Some(b'\'') {
        Scalar::String
    } else if t[a].word(sql, "NULL") {
        Scalar::Null
    } else {
        Scalar::Opaque
    };
    let position = t.get(start).map_or(sql.len(), |v| v.start);
    Candidate {
        span: Span {
            start: position,
            end: if start < end {
                t[end - 1].end
            } else {
                position
            },
        },
        scalar,
    }
}
// Split the BETWEEN's own AND from later boolean AND/OR. Parentheses (including
// opaque functions/subqueries) are skipped as units, never split at their ANDs.
// Unknown bound expressions stay opaque rather than becoming guessed scalars.
fn bound_end(sql: &str, t: &[Token], pairs: &[usize], mut i: usize) -> usize {
    while i < t.len() {
        if matches!(t[i].symbol, Some(b')' | b',' | b';'))
            || !qualified(t, i)
                && keyword(
                    sql,
                    &t[i],
                    &[
                        "AND", "OR", "SELECT", "FROM", "WHERE", "ON", "GROUP", "ORDER", "HAVING",
                        "LIMIT", "UNION", "AS", "THEN", "ELSE", "END", "IS",
                    ],
                )
        {
            break;
        }
        i = if t[i].symbol == Some(b'(') {
            pairs[i] + 1
        } else {
            i + 1
        };
    }
    i
}

/// Source edits are planned before argument mutation and checked as a whole.
/// Parameters may never be created, removed, duplicated or reordered by edits.
#[derive(Default)]
pub(super) struct EditPlan(Vec<(usize, usize, String)>);
impl EditPlan {
    pub(super) fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
    pub(super) fn push(&mut self, edit: (usize, usize, String)) {
        self.0.push(edit);
    }
    pub(super) fn render(mut self, sql: &str, original: &Syntax) -> Result<String, Error> {
        self.0.sort_by_key(|v| v.0);
        let mut out = String::with_capacity(sql.len());
        let mut position = 0;
        let old = &original.scan.sites;
        for (start, end, value) in self.0 {
            if start < position
                || end < start
                || end > sql.len()
                || !sql.is_char_boundary(start)
                || !sql.is_char_boundary(end)
            {
                return Err(invalid(
                    "overlapping or invalid parameter expression rewrites",
                ));
            }
            // A rewritten fragment must retain exactly its original markers.
            // Fragments may be identifiers or scalar expressions, not statements.
            let prior = old.iter().filter(|&&p| p >= start && p < end).count();
            let next = scan(&value, true, false)?.sites.len();
            if prior > 1 || prior != next {
                return Err(invalid("parameter expression rewrite changes marker count"));
            }
            out.push_str(&sql[position..start]);
            out.push_str(&value);
            position = end;
        }
        out.push_str(&sql[position..]);
        let parsed = Syntax::parse(&out)?;
        if parsed.kind != original.kind || parsed.scan.sites.len() != old.len() {
            return Err(invalid("parameter expression rewrite changes SQL layout"));
        }
        Ok(out)
    }
}

pub(super) fn parentheses(t: &[Token]) -> Result<Vec<usize>, Error> {
    let mut pairs = vec![usize::MAX; t.len()];
    let mut stack = Vec::new();
    for (i, v) in t.iter().enumerate() {
        match v.symbol {
            Some(b'(') => stack.push(i),
            Some(b')') => {
                let left = stack
                    .pop()
                    .ok_or_else(|| invalid("unbalanced BLOB SQL parentheses"))?;
                pairs[left] = i;
                pairs[i] = left;
            }
            _ => {}
        }
    }
    if !stack.is_empty() {
        return Err(invalid("unbalanced BLOB SQL parentheses"));
    }
    Ok(pairs)
}
pub(super) fn keyword(sql: &str, t: &Token, words: &[&str]) -> bool {
    words.iter().any(|w| t.word(sql, w))
}
pub(super) fn qualified(t: &[Token], i: usize) -> bool {
    i.checked_sub(1).is_some_and(|j| t[j].symbol == Some(b'.'))
        || t.get(i + 1).is_some_and(|v| v.symbol == Some(b'.'))
}
pub(super) fn name(sql: &str, t: &Token) -> bool {
    t.symbol == Some(b'`')
        || t.symbol.is_none()
            && sql.as_bytes()[t.start].is_ascii_alphabetic()
            && !keyword(
                sql,
                t,
                &[
                    "NULL", "TRUE", "FALSE", "DEFAULT", "SELECT", "FROM", "WHERE", "AND", "OR",
                    "NOT", "ON", "HAVING", "SET", "THEN", "ELSE", "END",
                ],
            )
        || t.symbol.is_none() && (sql.as_bytes()[t.start] == b'_' || sql.as_bytes()[t.start] >= 128)
}
pub(super) fn grouping(sql: &str, t: &[Token], left: usize) -> bool {
    left == 0 || !name(sql, &t[left - 1])
}
pub(super) fn parameter_span(sql: &str, t: &[Token], pairs: &[usize], i: usize) -> (usize, usize) {
    let (mut a, mut b) = (i, i + 1);
    while a > 0 && t[a - 1].symbol == Some(b'(') && pairs[a - 1] == b && grouping(sql, t, a - 1) {
        a -= 1;
        b += 1;
    }
    (a, b)
}
pub(super) fn comparison_symbol(t: &Token) -> bool {
    matches!(t.symbol, Some(b'=' | b'<' | b'>' | b'!'))
}
// A marker nested in arithmetic or a function is not a simple comparison
// operand. Keep it opaque, but remember surrounding comparisons so a byte
// adapter cannot mistake SELECT b > (?+1) for a standalone value expression.
fn enclosing_comparison(sql: &str, t: &[Token], pairs: &[usize], marker: usize) -> bool {
    (0..marker).any(|open| {
        if t[open].symbol != Some(b'(') || pairs[open] <= marker {
            return false;
        }
        let start = if open > 0 && name(sql, &t[open - 1]) && !grouping(sql, t, open) {
            open - 1
        } else {
            open
        };
        (start > 0 && comparison_symbol(&t[start - 1]))
            || t.get(pairs[open] + 1).is_some_and(comparison_symbol)
    })
}
// Match a complete operator run, never '=' inside '<=>' or an invalid prefix.
// Requiring exact source text also rejects split/comment-separated operators.
// Keeping the original operator preserves ordinary and NULL-safe truth values.
pub(super) fn comparison_at(sql: &str, t: &[Token], start: usize) -> Option<usize> {
    let mut end = start;
    while t.get(end).is_some_and(comparison_symbol) {
        end += 1;
    }
    if end == start {
        return None;
    }
    matches!(
        &sql[t[start].start..t[end - 1].end],
        "=" | "<>" | "!=" | "<=>" | "<" | "<=" | ">" | ">="
    )
    .then_some(end)
}
pub(super) fn comparison_before(sql: &str, t: &[Token], end: usize) -> Option<usize> {
    let mut start = end;
    while start > 0 && comparison_symbol(&t[start - 1]) {
        start -= 1;
    }
    (comparison_at(sql, t, start) == Some(end)).then_some(start)
}
pub(super) fn edge_before(sql: &str, t: &[Token], i: usize) -> bool {
    i == 0
        || matches!(t[i - 1].symbol, Some(b'(' | b','))
        || keyword(
            sql,
            &t[i - 1],
            &[
                "WHERE", "AND", "OR", "NOT", "ON", "HAVING", "WHEN", "THEN", "ELSE", "SELECT",
            ],
        )
}
pub(super) fn edge_after(sql: &str, t: &[Token], i: usize) -> bool {
    i == t.len()
        || matches!(t[i].symbol, Some(b')' | b',' | b';'))
        || keyword(
            sql,
            &t[i],
            &[
                "AND", "OR", "ORDER", "GROUP", "HAVING", "LIMIT", "UNION", "THEN", "ELSE", "END",
                "FROM", "WHERE", "AS",
            ],
        )
}
pub(super) fn identifier(
    sql: &str,
    t: &[Token],
    pairs: &[usize],
    mut a: usize,
    mut b: usize,
) -> bool {
    while a < b && t[a].symbol == Some(b'(') && pairs[a] + 1 == b {
        if !grouping(sql, t, a) {
            return false;
        }
        a += 1;
        b -= 1;
    }
    if a >= b || !name(sql, &t[a]) {
        return false;
    }
    a += 1;
    while a < b {
        if a + 1 >= b || t[a].symbol != Some(b'.') || !name(sql, &t[a + 1]) {
            return false;
        }
        a += 2;
    }
    true
}
pub(super) fn identifier_before(
    sql: &str,
    t: &[Token],
    pairs: &[usize],
    end: usize,
) -> Option<(usize, usize)> {
    if end == 0 {
        return None;
    }
    let mut start = end - 1;
    if t[start].symbol == Some(b')') {
        start = pairs[start];
    } else {
        while start >= 2 && t[start - 1].symbol == Some(b'.') && name(sql, &t[start - 2]) {
            start -= 2;
        }
    }
    (edge_before(sql, t, start) && identifier(sql, t, pairs, start, end)).then_some((start, end))
}
pub(super) fn identifier_after(
    sql: &str,
    t: &[Token],
    pairs: &[usize],
    start: usize,
) -> Option<(usize, usize)> {
    if start >= t.len() {
        return None;
    }
    let mut end = start + 1;
    if t[start].symbol == Some(b'(') {
        end = pairs[start] + 1;
    } else {
        while end + 1 < t.len() && t[end].symbol == Some(b'.') && name(sql, &t[end + 1]) {
            end += 2;
        }
    }
    (edge_after(sql, t, end) && identifier(sql, t, pairs, start, end)).then_some((start, end))
}
pub(super) fn inside_hex(sql: &str, t: &[Token], pairs: &[usize], i: usize) -> bool {
    let (mut a, mut b) = (i, i + 1);
    loop {
        if a == 0 || t[a - 1].symbol != Some(b'(') || pairs[a - 1] != b {
            return false;
        }
        a -= 1;
        b += 1;
        if a > 0 && t[a - 1].word(sql, "HEX") {
            return true;
        }
        if !grouping(sql, t, a) {
            return false;
        }
    }
}
pub(super) fn clause_context(sql: &str, t: &[Token]) -> Result<(Vec<bool>, Vec<bool>), Error> {
    let mut contexts = vec![false];
    let mut predicate = vec![false; t.len()];
    let mut assignments = vec![false; t.len()];
    let mut set: Option<(usize, bool)> = None;
    let mutation = t
        .first()
        .is_some_and(|v| keyword(sql, v, &["UPDATE", "INSERT"]));
    for (i, v) in t.iter().enumerate() {
        let depth = contexts.len() - 1;
        if !qualified(t, i) && keyword(sql, v, &["WHERE", "ON", "HAVING"]) {
            *contexts.last_mut().unwrap() = true;
            if set.is_some_and(|(d, _)| d == depth) {
                set = None;
            }
        }
        if !qualified(t, i)
            && keyword(
                sql,
                v,
                &[
                    "SELECT", "FROM", "JOIN", "GROUP", "ORDER", "LIMIT", "UNION", "SET",
                ],
            )
        {
            *contexts.last_mut().unwrap() = false;
        }
        if mutation && !qualified(t, i) && v.word(sql, "SET") {
            set = Some((depth, true));
        }
        if let Some((d, first)) = set {
            if d == depth && v.symbol == Some(b',') {
                set = Some((d, true));
            }
            if d == depth && first && v.symbol == Some(b'=') {
                assignments[i] = true;
                set = Some((d, false));
            }
        }
        predicate[i] = *contexts.last().unwrap();
        match v.symbol {
            Some(b'(') => contexts.push(*contexts.last().unwrap()),
            Some(b')') => {
                if contexts.len() == 1 {
                    return Err(invalid("unbalanced BLOB condition"));
                }
                contexts.pop();
            }
            _ => {}
        }
    }
    Ok((predicate, assignments))
}

/// Scan the common Doris/MySQL lexical subset under both backslash modes.
/// Unsupported/ambiguous forms fail before any query is sent.
pub(super) fn sites(sql: &str) -> Result<Vec<usize>, Error> {
    let normal = scan(sql, true, false)?.sites;
    if normal != scan(sql, false, false)?.sites {
        return Err(invalid(
            "parameter positions depend on NO_BACKSLASH_ESCAPES",
        ));
    }
    Ok(normal)
}

pub(super) fn scan(sql: &str, escapes: bool, literal_nul: bool) -> Result<Scan, Error> {
    let b = sql.as_bytes();
    let (mut i, mut ended) = (0, false);
    let mut positions = Vec::new();
    let mut tokens = Vec::new();
    if !literal_nul && b.contains(&0) {
        return Err(invalid("NUL in SQL text"));
    }
    while i < b.len() {
        if b[i] == 0 {
            return Err(invalid("NUL outside SQL literal"));
        }
        if b[i].is_ascii_whitespace() {
            i += 1;
            continue;
        }
        // Doris accepts --x as a comment; MySQL requires whitespace after --.
        // Refuse that cross-dialect ambiguity rather than hide active markers.
        if b[i..].starts_with(b"--") && !b.get(i + 2).is_some_and(u8::is_ascii_whitespace) {
            return Err(invalid("double-dash comments require following whitespace"));
        }
        if b[i] == b'#' || b[i..].starts_with(b"--") {
            while i < b.len() && b[i] != b'\n' && b[i] != b'\r' {
                if b[i] == b'\\' && matches!(b.get(i + 1), Some(b'\n' | b'\r')) {
                    return Err(invalid(
                        "continued line comments are outside the common SQL subset",
                    ));
                }
                i += 1;
            }
            continue;
        }
        if b[i..].starts_with(b"/*") {
            if matches!(b.get(i + 2), Some(b'!' | b'+')) || b[i..].starts_with(b"/*M!") {
                return Err(invalid(
                    "executable comments and optimizer hints are not bindable",
                ));
            }
            i += 2;
            loop {
                if i >= b.len() {
                    return Err(invalid("unterminated SQL comment"));
                }
                if b[i..].starts_with(b"/*") {
                    return Err(invalid("nested SQL comments"));
                }
                if b[i..].starts_with(b"*/") {
                    i += 2;
                    break;
                }
                i += 1;
            }
            continue;
        }
        if ended {
            return Err(invalid("multiple SQL statements are not bindable"));
        }
        let start = i;
        let symbol = match b[i] {
            b'\'' | b'"' | b'`' => {
                let quote = b[i];
                if !literal_nul && b.get(i..i + 3).is_some_and(|s| s == [quote; 3]) {
                    return Err(invalid("triple-quoted SQL is outside the binding subset"));
                }
                i += 1;
                loop {
                    let Some(&c) = b.get(i) else {
                        return Err(invalid("unterminated SQL quote"));
                    };
                    if c == b'\\' && escapes {
                        if b.get(i + 1).is_none() {
                            return Err(invalid("unterminated SQL escape"));
                        }
                        i += 2;
                    } else if c == quote {
                        i += 1;
                        if b.get(i) == Some(&quote) {
                            i += 1;
                        } else {
                            break;
                        }
                    } else {
                        i += 1;
                    }
                }
                Some(quote)
            }
            b'?' => {
                positions.push(i);
                i += 1;
                Some(b'?')
            }
            b';' => {
                ended = true;
                i += 1;
                Some(b';')
            }
            c if c.is_ascii_alphanumeric() || c == b'_' || c >= 128 => {
                i += 1;
                while b
                    .get(i)
                    .is_some_and(|c| c.is_ascii_alphanumeric() || *c == b'_' || *c >= 128)
                {
                    i += 1;
                }
                None
            }
            c => {
                i += 1;
                Some(c)
            }
        };
        tokens.push(Token {
            start,
            end: i,
            symbol,
        });
    }
    Ok(Scan {
        sites: positions,
        tokens,
    })
}
