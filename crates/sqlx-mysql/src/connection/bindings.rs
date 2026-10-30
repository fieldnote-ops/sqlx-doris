//! Checked Doris parameter adaptation and typed public-protocol binding.
//! The logger keeps original SQL. Typed literals use the session's actual sql_mode.
use crate::protocol::text::{ColumnFlags, ColumnType};
use crate::{Error, MySqlArguments};
use std::fmt::Write;

mod syntax;
use syntax::*;

fn invalid(message: &str) -> Error {
    err_protocol!("Doris client binding: {message}")
}

fn is_blob(ty: ColumnType) -> bool {
    matches!(
        ty,
        ColumnType::Blob | ColumnType::TinyBlob | ColumnType::MediumBlob | ColumnType::LongBlob
    )
}

#[derive(Clone, Copy, PartialEq)]
enum ParameterKind {
    Bytes,
    Time,
}
fn parameter_kind(ty: ColumnType) -> Option<ParameterKind> {
    if is_blob(ty) {
        Some(ParameterKind::Bytes)
    } else if ty == ColumnType::Time {
        Some(ParameterKind::Time)
    } else {
        None
    }
}
fn operand(kind: ParameterKind, sql: &str) -> String {
    match kind {
        ParameterKind::Bytes => format!("HEX({sql})"),
        ParameterKind::Time => format!("CAST({sql} AS TIME(6))"),
    }
}
// Preserve a VARCHAR context before MySQL's TIME cast: direct parameter type
// propagation can preconvert a valid whole-day duration as DATETIME. Strip only
// the optional validated plus sign; every argument remains one native scalar.
fn canonical_time_key(expression: &str) -> String {
    format!("REPLACE(CAST({expression} AS CHAR),'-00:00:00.000000','00:00:00.000000')")
}
fn membership_operand(kind: ParameterKind, sql: &str) -> String {
    if kind == ParameterKind::Time {
        canonical_time_key(&operand(kind, sql))
    } else {
        operand(kind, sql)
    }
}
// Keep VARCHAR context before MySQL's TIME cast. Direct parameter propagation
// can preconvert a whole duration as DATETIME. TRIM handles a validated '+';
// canonical keys retain six fractional digits and normalize signed zero only.
fn string_operand(kind: ParameterKind, sql: &str) -> String {
    if kind == ParameterKind::Time {
        canonical_time_key(&format!(
            "CAST(CONCAT('',TRIM(LEADING '+' FROM {sql})) AS TIME(6))"
        ))
    } else {
        operand(kind, sql)
    }
}
pub(super) fn has_time(args: &MySqlArguments) -> bool {
    args.types.iter().any(|ty| ty.r#type == ColumnType::Time)
}

pub(super) fn has_bound_pagination(sql: &str) -> Result<bool, Error> {
    Ok(!pagination_sites(sql)?.is_empty())
}

// Normalize only the common INSERT INTO qualified_table [(columns)] VALUE (...)
// alias, never a quoted identifier, string, label or nested expression. Native
// MySQL does not call this path. Unknown syntax retains server validation.
pub(super) fn normalize_insert_value(sql: &str) -> Result<Option<String>, Error> {
    let lexical = match scan(sql, true, false) {
        Ok(v) => v,
        Err(_) => return Ok(None),
    };
    let t = &lexical.tokens;
    if !t.first().is_some_and(|v| v.word(sql, "INSERT"))
        || !t.get(1).is_some_and(|v| v.word(sql, "INTO"))
        || !t.get(2).is_some_and(|v| name(sql, v))
    {
        return Ok(None);
    }
    let mut i = 3;
    while t.get(i).is_some_and(|v| v.symbol == Some(b'.'))
        && t.get(i + 1).is_some_and(|v| name(sql, v))
    {
        i += 2;
    }
    if t.get(i).is_some_and(|v| v.symbol == Some(b'(')) {
        let Ok(pairs) = parentheses(t) else {
            return Ok(None);
        };
        i = pairs[i] + 1;
    }
    if !t.get(i).is_some_and(|v| v.word(sql, "VALUE"))
        || !t.get(i + 1).is_some_and(|v| v.symbol == Some(b'('))
    {
        return Ok(None);
    }
    let model = Syntax::parse(sql)?;
    let mut edits = EditPlan::default();
    edits.push((t[i].start, t[i].end, "VALUES".into()));
    edits.render(sql, &model).map(Some)
}

pub(super) fn adapt_query(
    sql: &str,
    args: Option<&mut MySqlArguments>,
) -> Result<Option<String>, Error> {
    let normalized = normalize_insert_value(sql)?;
    let adapted = args
        .map(|a| adapt_parameters(normalized.as_deref().unwrap_or(sql), a))
        .transpose()?
        .flatten();
    Ok(adapted.or(normalized))
}

// Adapt byte and duration expressions without changing native argument identity.
// Bytes use ASCII hex in prepared statements; TIME uses checked typed literals
// when the executor selects the Doris public text protocol.
pub(super) fn adapt_parameters(
    sql: &str,
    args: &mut MySqlArguments,
) -> Result<Option<String>, Error> {
    if !args
        .types
        .iter()
        .any(|v| parameter_kind(v.r#type).is_some())
    {
        return Ok(None);
    }
    let model = Syntax::parse(sql)?;
    if model.parameters.len() != args.types.len()
        || args.null_bitmap.len() != args.types.len().div_ceil(8)
    {
        return Err(invalid("parameter count or null bitmap differs"));
    }
    let t = &model.scan.tokens;
    let pagination = pagination_sites(sql)?;
    let mut edits = EditPlan::default();
    let mut handled = adapt_memberships(sql, &model, args, &mut edits, false)?;
    adapt_between(sql, &model, args, &mut edits, &mut handled)?;
    for site in &model.parameters {
        let i = site.token;
        let token = &t[i];
        let Some(kind) = parameter_kind(args.types[site.index].r#type) else {
            continue;
        };
        if pagination.contains(&token.start) {
            return Err(invalid(
                "pagination count and offset require native integers",
            ));
        }
        let blob = kind == ParameterKind::Bytes;
        if blob && site.in_case && !site.explicit_hex {
            return Err(invalid(
                "BLOB parameters inside CASE require explicit byte functions",
            ));
        }
        if handled[i] {
            continue;
        }
        if matches!(site.expression, Expression::NullTest) {
            // Re-encoding BLOB as ASCII hex preserves NULL/non-NULL exactly;
            // a direct IS [NOT] NULL needs no byte conversion expression.
            continue;
        }
        let column = match site.expression {
            Expression::Comparison {
                op:
                    Comparison::Equal
                    | Comparison::NotEqual
                    | Comparison::NullSafe
                    | Comparison::Ordered,
                column,
                boundary,
            } => {
                // Uppercase hex preserves unsigned byte lexicographic order,
                // including prefixes and significant trailing NUL/space bytes.
                // Keep the original operator and one native scalar per bind.
                if !boundary {
                    return Err(invalid("unsupported chained parameter comparison"));
                }
                if blob && column.is_none() {
                    return Err(invalid("BLOB comparison requires a simple column operand"));
                }
                column
            }
            Expression::Assignment | Expression::NullTest => None,
            Expression::Value {
                predicate,
                operator_nearby,
            } => {
                if blob && (predicate || operator_nearby) && !site.explicit_hex {
                    return Err(invalid("binary predicates require a simple column comparison, scalar IN/NOT IN list or BETWEEN/NOT BETWEEN bounds; use explicit byte functions for other forms"));
                }
                None
            }
        };
        if let Some(span) = column {
            edits.push((
                span.start,
                span.end,
                operand(kind, &sql[span.start..span.end]),
            ));
        } else if blob {
            // Avoid the FE/BE UTF-8 constant-fold bridge without changing data.
            edits.push((
                token.start,
                token.end,
                "UNHEX(CONCAT(?,SUBSTRING(UUID(),1,0)))".into(),
            ));
        }
    }
    let rewritten = edits.render(sql, &model)?;
    let mut input = Input(&args.values);
    let mut values = Vec::with_capacity(args.values.len());
    let mut types = args.types.clone();
    for (index, ty) in args.types.iter().enumerate() {
        let blob = is_blob(ty.r#type);
        if blob {
            types[index] = <str as crate::types::Type<crate::MySql>>::type_info();
        }
        if args.null_bitmap[index / 8] & (1 << (index % 8)) != 0 {
            continue;
        }
        let before = input.0;
        if blob {
            use crate::io::MySqlBufMutExt;
            let bytes = input.lenenc()?;
            let mut hex = String::with_capacity(bytes.len().saturating_mul(2));
            for b in bytes {
                write!(hex, "{b:02X}").unwrap();
            }
            values.put_bytes_lenenc(hex.as_bytes());
        } else {
            skip_argument(&mut input, ty.r#type)?;
            values.extend_from_slice(&before[..before.len() - input.0.len()]);
        }
    }
    if !input.0.is_empty() {
        return Err(invalid("trailing BLOB argument bytes"));
    }
    args.values = values;
    args.types = types;
    Ok(Some(rewritten))
}

// Keep BETWEEN/NOT BETWEEN and its own AND intact. In particular, one NULL
// bound does not imply a NULL result: the other comparison can be false.
// HEX preserves the inclusive unsigned byte order without duplicating binds.
fn adapt_between(
    sql: &str,
    model: &Syntax,
    args: &MySqlArguments,
    edits: &mut EditPlan,
    handled: &mut [bool],
) -> Result<(), Error> {
    for range in &model.betweens {
        let target = (range.start..range.end).any(|i| {
            model.indices[i]
                .is_some_and(|p| is_blob(args.types[p].r#type) && !model.parameters[p].explicit_hex)
        });
        if !target {
            continue;
        }
        if !range.boundary {
            return Err(invalid(
                "unsupported BLOB BETWEEN suffix or chained expression",
            ));
        }
        let column = range
            .column
            .ok_or_else(|| invalid("BLOB BETWEEN/NOT BETWEEN requires a simple column subject"))?;
        edits.push((
            column.start,
            column.end,
            format!(
                "{}{}",
                if range.null_test { "(" } else { "" },
                operand(ParameterKind::Bytes, &sql[column.start..column.end])
            ),
        ));
        if range.null_test {
            edits.push((range.upper.span.end, range.upper.span.end, ")".into()));
        }
        for candidate in [&range.lower, &range.upper] {
            let span = candidate.span;
            match candidate.scalar {
                Scalar::Parameter(p) => {
                    let ty = args.types[p].r#type;
                    if is_blob(ty) {
                        handled[model.parameters[p].token] = true;
                    } else if matches!(
                        ty,
                        ColumnType::VarChar | ColumnType::VarString | ColumnType::String
                    ) {
                        edits.push((
                            span.start,
                            span.end,
                            operand(ParameterKind::Bytes, &sql[span.start..span.end]),
                        ));
                    } else if ty == ColumnType::Time
                        || args.null_bitmap[p / 8] & (1 << (p % 8)) == 0
                    {
                        return Err(invalid("BLOB BETWEEN cannot infer numeric, TIME or other non-string boundary coercion"));
                    }
                }
                Scalar::String => edits.push((
                    span.start,
                    span.end,
                    operand(ParameterKind::Bytes, &sql[span.start..span.end]),
                )),
                Scalar::Null => {}
                Scalar::Opaque => {
                    return Err(invalid(
                        "BLOB BETWEEN boundaries must be scalar binds, strings or NULL",
                    ))
                }
            }
        }
    }
    Ok(())
}

// MySQL TIME IN compares some values as strings. Normalize this declared TIME
// domain before PREPARE, without changing native argument types or byte values.
// Ordinary queries and string-only membership retain the upstream MySQL path.
pub(super) fn adapt_mysql_time_memberships(
    sql: &str,
    args: &MySqlArguments,
) -> Result<Option<String>, Error> {
    if !has_time(args) {
        return Ok(None);
    }
    let lexical = match scan(sql, true, false) {
        Ok(v) => v,
        Err(_) => return Ok(None),
    };
    if !lexical.tokens.iter().any(|v| v.word(sql, "IN"))
        || !lexical
            .tokens
            .first()
            .is_some_and(|v| keyword(sql, v, &["SELECT", "INSERT", "UPDATE", "DELETE"]))
    {
        return Ok(None);
    }
    let model = Syntax::parse(sql)?;
    if model.parameters.len() != args.types.len()
        || args.null_bitmap.len() != args.types.len().div_ceil(8)
    {
        return Err(invalid(
            "TIME membership parameter count or null bitmap differs",
        ));
    }
    // Leave existing explicit CAST/complex-left MySQL expressions native. The
    // shared normalization subset is simple columns and scalar candidates.
    for list in &model.memberships {
        if list.subquery
            || !model.indices[list.open + 1..list.close]
                .iter()
                .flatten()
                .any(|&p| args.types[p].r#type == ColumnType::Time)
        {
            continue;
        }
        let end = if list.keyword > 0 && model.scan.tokens[list.keyword - 1].word(sql, "NOT") {
            list.keyword - 1
        } else {
            list.keyword
        };
        if identifier_before(sql, &model.scan.tokens, &model.pairs, end).is_none() {
            return Ok(None);
        }
        for candidate in &list.candidates {
            if matches!(candidate.scalar, Scalar::Opaque) {
                let part = &sql[candidate.span.start..candidate.span.end];
                let tokens = scan(part, true, false)?;
                if tokens
                    .tokens
                    .iter()
                    .find(|v| v.symbol.is_none())
                    .is_some_and(|v| keyword(part, v, &["CAST", "CONVERT"]))
                {
                    return Ok(None);
                }
            }
        }
    }
    let mut edits = EditPlan::default();
    adapt_memberships(sql, &model, args, &mut edits, true)?;
    if edits.is_empty() {
        Ok(None)
    } else {
        edits.render(sql, &model).map(Some)
    }
}

fn skip_argument(input: &mut Input<'_>, ty: ColumnType) -> Result<(), Error> {
    use ColumnType::*;
    match ty {
        Tiny => {
            input.take(1)?;
        }
        Short | Year => {
            input.take(2)?;
        }
        Long | Int24 | Float => {
            input.take(4)?;
        }
        LongLong | Double => {
            input.take(8)?;
        }
        Time => {
            read_time(input)?;
        }
        Date | Datetime | Timestamp => {
            let n = usize::from(input.take(1)?[0]);
            input.take(n)?;
        }
        Decimal | NewDecimal | VarChar | VarString | String | Enum | Set | Json | Bit
        | Geometry | Blob | TinyBlob | MediumBlob | LongBlob => {
            input.lenenc()?;
        }
        Null => {}
    }
    Ok(())
}
fn argument_texts(args: &MySqlArguments) -> Result<Vec<Option<&str>>, Error> {
    let mut input = Input(&args.values);
    let mut out = Vec::with_capacity(args.types.len());
    for (i, ty) in args.types.iter().enumerate() {
        let mut text = None;
        if args.null_bitmap[i / 8] & (1 << (i % 8)) == 0 {
            if matches!(
                ty.r#type,
                ColumnType::VarChar | ColumnType::VarString | ColumnType::String
            ) {
                text = Some(
                    std::str::from_utf8(input.lenenc()?)
                        .map_err(|_| invalid("invalid UTF-8 TIME membership string"))?,
                );
            } else {
                skip_argument(&mut input, ty.r#type)?;
            }
        }
        out.push(text);
    }
    if !input.0.is_empty() {
        return Err(invalid("trailing TIME membership argument bytes"));
    }
    Ok(out)
}
fn validate_time_text(text: &str) -> Result<(), Error> {
    use crate::decode::Decode;
    crate::types::MySqlTime::decode(crate::MySqlValueRef {
        value: Some(text.as_bytes()),
        row: None,
        type_info: crate::MySqlTypeInfo::binary(ColumnType::Time),
        format: crate::MySqlValueFormat::Text,
    })
    .map(|_| ())
    .map_err(|_| {
        invalid(
            "TIME membership string requires [-]HH:MM:SS[.ffffff] in the native MySQL TIME range",
        )
    })
}

// Adapt the whole list together. Leaving even one ordinary string in its raw
// representation would change membership, and filtering NULLs would break NOT
// IN's three-valued logic. Keep the original operator, order and NULL entries.
fn adapt_memberships(
    sql: &str,
    model: &Syntax,
    args: &MySqlArguments,
    edits: &mut EditPlan,
    time_only: bool,
) -> Result<Vec<bool>, Error> {
    let t = &model.scan.tokens;
    let pairs = &model.pairs;
    let indices = &model.indices;
    let mut handled = vec![false; t.len()];
    let mut texts = None;
    for list in &model.memberships {
        let i = list.keyword;
        let open = list.open;
        let close = list.close;
        // A subquery can contain independent byte equalities/list predicates.
        // It is not a constant candidate list and must not be wrapped as one.
        if list.subquery {
            if time_only {
                continue;
            }
            let mut j = open + 2;
            let mut depth = 0_usize;
            while j < close {
                if depth == 0 && !qualified(t, j) && t[j].word(sql, "FROM") {
                    break;
                }
                if indices[j].is_some_and(|p| is_blob(args.types[p].r#type))
                    && !inside_hex(sql, t, pairs, j)
                {
                    return Err(invalid(
                        "BLOB subquery output membership requires explicit byte functions",
                    ));
                }
                match t[j].symbol {
                    Some(b'(') => depth += 1,
                    Some(b')') => depth -= 1,
                    _ => {}
                }
                j += 1;
            }
            continue;
        }
        let mut kind = None;
        for j in open + 1..close {
            if let Some(k) = indices[j].and_then(|p| parameter_kind(args.types[p].r#type)) {
                if k == ParameterKind::Bytes && inside_hex(sql, t, pairs, j) {
                    continue;
                }
                if kind.is_some_and(|old| old != k) {
                    return Err(invalid(
                        "cannot mix byte and TIME domains in one membership list",
                    ));
                }
                kind = Some(k);
            }
        }
        let Some(kind) = kind else {
            continue;
        };
        if time_only && kind != ParameterKind::Time {
            continue;
        }
        if kind == ParameterKind::Time && texts.is_none() {
            texts = Some(argument_texts(args)?);
        }
        let operand_end = if i > 0 && t[i - 1].word(sql, "NOT") {
            i - 1
        } else {
            i
        };
        let (a, b) = identifier_before(sql, t, pairs, operand_end)
            .ok_or_else(|| invalid("BLOB IN/NOT IN requires a simple column operand"))?;
        let mut tail = close + 1;
        let null_test = t.get(tail).is_some_and(|v| v.word(sql, "IS"));
        if null_test {
            tail += 1;
            if t.get(tail).is_some_and(|v| v.word(sql, "NOT")) {
                tail += 1;
            }
            if !t.get(tail).is_some_and(|v| v.word(sql, "NULL")) {
                return Err(invalid("unsupported BLOB membership suffix"));
            }
            tail += 1;
        }
        // AS also terminates this scalar expression in CAST(... AS type)
        // and SELECT ... AS alias; the server still validates its syntax.
        if !edge_after(sql, t, tail) && !t.get(tail).is_some_and(|v| v.word(sql, "AS")) {
            return Err(invalid("unsupported chained BLOB membership"));
        }
        edits.push((
            t[a].start,
            t[b - 1].end,
            format!(
                "{}{}",
                if null_test { "(" } else { "" },
                membership_operand(kind, &sql[t[a].start..t[b - 1].end])
            ),
        ));
        // Doris requires parentheses for membership IS [NOT] NULL, while
        // MySQL also accepts the unparenthesized form. Keep the same truth value.
        if null_test {
            edits.push((t[close].end, t[close].end, ")".into()));
        }
        for candidate in &list.candidates {
            let span = candidate.span;
            match candidate.scalar {
                Scalar::Parameter(p) => {
                    if parameter_kind(args.types[p].r#type) == Some(kind) {
                        handled[model.parameters[p].token] = true;
                        if kind == ParameterKind::Time {
                            edits.push((
                                span.start,
                                span.end,
                                membership_operand(kind, &sql[span.start..span.end]),
                            ));
                        }
                    } else if args.null_bitmap[p / 8] & (1 << (p % 8)) == 0 {
                        if !matches!(
                            args.types[p].r#type,
                            ColumnType::VarChar | ColumnType::VarString | ColumnType::String
                        ) {
                            return Err(invalid(
                                "membership cannot infer numeric or other non-string coercion",
                            ));
                        }
                        if kind == ParameterKind::Time {
                            validate_time_text(
                                texts.as_ref().unwrap()[p]
                                    .ok_or_else(|| invalid("missing TIME membership string"))?,
                            )?;
                        }
                        edits.push((
                            span.start,
                            span.end,
                            string_operand(kind, &sql[span.start..span.end]),
                        ));
                    } else if kind == ParameterKind::Time {
                        edits.push((
                            span.start,
                            span.end,
                            string_operand(kind, &sql[span.start..span.end]),
                        ));
                    }
                }
                Scalar::String => {
                    if kind == ParameterKind::Time {
                        let fragment = &sql[span.start..span.end];
                        let literal = scan(fragment, true, false)?;
                        let token = literal
                            .tokens
                            .iter()
                            .find(|v| v.symbol == Some(b'\''))
                            .ok_or_else(|| invalid("missing TIME literal"))?;
                        validate_time_text(&fragment[token.start + 1..token.end - 1])?;
                    }
                    edits.push((
                        span.start,
                        span.end,
                        string_operand(kind, &sql[span.start..span.end]),
                    ));
                }
                Scalar::Null => {}
                Scalar::Opaque => return Err(invalid(
                    "membership supports only scalar parameters, single-quoted strings and NULL",
                )),
            }
        }
    }
    Ok(handled)
}

/// Positions for checked client binding; opaque server SQL is not reprinted.
pub(super) fn sites(sql: &str) -> Result<Vec<usize>, Error> {
    syntax::sites(sql)
}

/// Route common native transaction VALUES to the execution engine before send.
/// A public session variable makes the parsed InlineTable non-constant, while
/// IF(x, value, value) preserves its value and per-row target-type conversion.
/// This avoids both the FE's literal-only batch engine and UNION type coercion.
/// Apply to text AND bound VALUES from the first write; never switch after ERR.
pub(super) fn route_values(sql: &str, no_backslash: bool) -> Result<Option<String>, Error> {
    // Generated literals can start with doubled quotes and contain a real NUL
    // under NO_BACKSLASH_ESCAPES. Parse those with the actual session mode.
    let scan = scan(sql, !no_backslash, true)?;
    if !scan.sites.is_empty() {
        return Err(invalid("unbound transaction parameter marker"));
    }
    let t = &scan.tokens;
    if !t.first().is_some_and(|v| v.word(sql, "INSERT"))
        || !t.get(1).is_some_and(|v| v.word(sql, "INTO"))
    {
        return Ok(None);
    }
    let mut depth = 0_usize;
    let mut values = None;
    for (index, token) in t.iter().enumerate().skip(2) {
        match token.symbol {
            Some(b'(') => depth += 1,
            Some(b')') => {
                depth = depth
                    .checked_sub(1)
                    .ok_or_else(|| invalid("unbalanced INSERT header"))?
            }
            None if depth == 0 && token.word(sql, "SELECT") => return Ok(None),
            None if depth == 0 && token.word(sql, "VALUES") => {
                values = Some(index + 1);
                break;
            }
            _ => {}
        }
    }
    let Some(mut index) = values else {
        return Ok(None);
    };
    let mut candidate: Option<(usize, usize)> = None;
    let mut columns = None;
    loop {
        if t.get(index).and_then(|v| v.symbol) != Some(b'(') {
            return Err(invalid("expected a VALUES row"));
        }
        let mut stack = vec![b')'];
        index += 1;
        let mut field = index;
        let mut count = 0;
        loop {
            let token = t
                .get(index)
                .ok_or_else(|| invalid("unterminated VALUES row"))?;
            let close_row = stack.len() == 1 && token.symbol == Some(b')');
            if close_row || stack.len() == 1 && token.symbol == Some(b',') {
                if field == index {
                    return Err(invalid("empty VALUES expression"));
                }
                if !(index == field + 1 && t[field].word(sql, "DEFAULT")) {
                    let span = (t[field].start, t[index - 1].end);
                    if candidate.is_none_or(|v| span.1 - span.0 < v.1 - v.0) {
                        candidate = Some(span);
                    }
                }
                count += 1;
                if close_row {
                    index += 1;
                    break;
                }
                index += 1;
                field = index;
                continue;
            }
            match token.symbol {
                Some(b'(') => stack.push(b')'),
                Some(b'[') => stack.push(b']'),
                Some(b'{') => stack.push(b'}'),
                Some(c @ (b')' | b']' | b'}')) => {
                    if stack.pop() != Some(c) {
                        return Err(invalid("unbalanced VALUES expression"));
                    }
                }
                _ => {}
            }
            index += 1;
        }
        if columns.replace(count).is_some_and(|n| n != count) {
            return Err(invalid("VALUES row widths differ"));
        }
        if t.get(index).and_then(|v| v.symbol) == Some(b',') {
            index += 1;
            continue;
        }
        break;
    }
    if t.get(index).and_then(|v| v.symbol) == Some(b';') {
        index += 1;
    }
    if index != t.len() {
        return Err(invalid("unsupported trailing VALUES clause"));
    }
    // DefaultValueSlot is a non-literal leaf in the official FE. A non-empty
    // all-DEFAULT InlineTable already selects InsertIntoTableCommand; let the
    // server bind every default under its own table metadata/lock. Never guess
    // schema/defaults or wrap DEFAULT in IF(), which would lose its slot role.
    let Some((start, end)) = candidate else {
        return Ok(Some(sql.to_owned()));
    };
    let value = &sql[start..end];
    Ok(Some(format!(
        "{}IF(@@sql_mode IS NULL,{value},{value}){}",
        &sql[..start],
        &sql[end..]
    )))
}

#[cfg(test)]
fn compile(sql: &str, args: &MySqlArguments) -> Result<String, Error> {
    compile_mode(sql, args, false)
}
pub(super) fn compile_mode(
    sql: &str,
    args: &MySqlArguments,
    no_backslash: bool,
) -> Result<String, Error> {
    let positions = sites(sql)?;
    let pagination = pagination_sites(sql)?;
    if positions.len() != args.types.len() {
        return Err(invalid("parameter count differs from SQL marker count"));
    }
    if args.null_bitmap.len() != args.types.len().div_ceil(8) {
        return Err(invalid("invalid null bitmap length"));
    }
    let mut bytes = Input(&args.values);
    let mut out = String::with_capacity(sql.len());
    let mut start = 0;
    for (index, (&position, ty)) in positions.iter().zip(&args.types).enumerate() {
        out.push_str(&sql[start..position]);
        if pagination.contains(&position) {
            if args.null_bitmap[index / 8] & (1 << (index % 8)) != 0 {
                return Err(invalid("pagination count and offset cannot be NULL"));
            }
            let size = match ty.r#type {
                ColumnType::Tiny => 1,
                ColumnType::Short | ColumnType::Year => 2,
                ColumnType::Long | ColumnType::Int24 => 4,
                ColumnType::LongLong => 8,
                _ => {
                    return Err(invalid(
                        "pagination count and offset require native integers",
                    ))
                }
            };
            let mut value = String::new();
            integer(
                &mut value,
                bytes.take(size)?,
                ty.flags.contains(ColumnFlags::UNSIGNED),
            );
            if value.parse::<i64>().ok().is_none_or(|v| v < 0) {
                return Err(invalid(
                    "pagination count or offset exceeds Doris nonnegative BIGINT range",
                ));
            }
            // The validated decimal integer is a complete grammar token. The
            // usual scalar parentheses are invalid in Doris LIMIT/OFFSET.
            out.push_str(&value);
            start = position + 1;
            continue;
        }
        out.push('('); // Prevent adjacent minus signs/identifiers changing tokens.
        if args
            .null_bitmap
            .get(index / 8)
            .is_some_and(|v| v & (1 << (index % 8)) != 0)
        {
            out.push_str("NULL");
        } else {
            use ColumnType::*;
            let unsigned = ty.flags.contains(ColumnFlags::UNSIGNED);
            match ty.r#type {
                Tiny => integer(&mut out, bytes.take(1)?, unsigned),
                Short | Year => integer(&mut out, bytes.take(2)?, unsigned),
                Long | Int24 => integer(&mut out, bytes.take(4)?, unsigned),
                LongLong => integer(&mut out, bytes.take(8)?, unsigned),
                Float => {
                    let v = f32::from_le_bytes(bytes.take(4)?.try_into().unwrap());
                    if !v.is_finite() {
                        return Err(invalid("non-finite float"));
                    }
                    if v == 0.0 && v.is_sign_negative() {
                        // A numeric -0.0 literal becomes Decimal zero in the FE.
                        // Its public string cast retains the original IEEE sign.
                        out.push_str("CAST('-0.0' AS FLOAT)");
                    } else {
                        write!(out, "CAST({v:?} AS FLOAT)").unwrap();
                    }
                }
                Double => {
                    let v = f64::from_le_bytes(bytes.take(8)?.try_into().unwrap());
                    if !v.is_finite() {
                        return Err(invalid("non-finite double"));
                    }
                    if v == 0.0 && v.is_sign_negative() {
                        out.push_str("CAST('-0.0' AS DOUBLE)");
                    } else {
                        write!(out, "CAST({v:?} AS DOUBLE)").unwrap();
                    }
                }
                Decimal | NewDecimal => out.push_str(&decimal(bytes.lenenc()?)?),
                VarChar | VarString | String | Enum | Set | Json => {
                    let value = bytes.lenenc()?;
                    std::str::from_utf8(value).map_err(|_| invalid("invalid UTF-8 argument"))?;
                    string_literal(&mut out, std::str::from_utf8(value).unwrap(), no_backslash);
                }
                Blob | TinyBlob | MediumBlob | LongBlob => {
                    binary_literal(&mut out, bytes.lenenc()?, no_backslash);
                }
                Time => time_literal(&mut out, &mut bytes)?,
                Date | Datetime | Timestamp => temporal(&mut out, &mut bytes, ty.r#type == Date)?,
                Null => out.push_str("NULL"),
                _ => return Err(invalid("unsupported argument type in transaction")),
            }
        }
        out.push(')');
        start = position + 1;
    }
    if !bytes.0.is_empty() {
        return Err(invalid("trailing argument bytes"));
    }
    out.push_str(&sql[start..]);
    Ok(out)
}

// Marker scanning already rejects SQL whose parameter positions disagree
// between lexical modes. Compare the complete typed output as well: when both
// encoders produce identical bytes, reading the mutable session mode cannot
// change the request. Never cache this decision across argument values.
pub(super) struct ModeBindings {
    normal: String,
    no_backslash: Result<String, Error>,
}

impl ModeBindings {
    pub(super) fn independent(&self) -> bool {
        self.no_backslash
            .as_ref()
            .is_ok_and(|value| *value == self.normal)
    }

    pub(super) fn select(self, no_backslash: bool) -> Result<String, Error> {
        if no_backslash {
            self.no_backslash
        } else {
            Ok(self.normal)
        }
    }
}

pub(super) fn compile_modes(sql: &str, args: &MySqlArguments) -> Result<ModeBindings, Error> {
    Ok(ModeBindings {
        normal: compile_mode(sql, args, false)?,
        // An alternative-mode error must not reject a valid normal-mode query.
        // If the outputs differ, the executor reads the actual mode and selects
        // that result before sending the single business command.
        no_backslash: compile_mode(sql, args, true),
    })
}

struct Input<'a>(&'a [u8]);
impl<'a> Input<'a> {
    fn take(&mut self, n: usize) -> Result<&'a [u8], Error> {
        let Some((head, tail)) = self.0.split_at_checked(n) else {
            return Err(invalid("truncated argument"));
        };
        self.0 = tail;
        Ok(head)
    }
    fn lenenc(&mut self) -> Result<&'a [u8], Error> {
        let len = match self.take(1)?[0] {
            n @ 0..=250 => u64::from(n),
            252 => u64::from(u16::from_le_bytes(self.take(2)?.try_into().unwrap())),
            253 => {
                let b = self.take(3)?;
                u64::from(u32::from_le_bytes([b[0], b[1], b[2], 0]))
            }
            254 => u64::from_le_bytes(self.take(8)?.try_into().unwrap()),
            _ => return Err(invalid("invalid argument length")),
        };
        self.take(usize::try_from(len).map_err(|_| invalid("argument length overflow"))?)
    }
}
fn string_literal(out: &mut String, value: &str, no_backslash: bool) {
    out.push('\'');
    for c in value.chars() {
        match c {
            '\'' => out.push_str("''"),
            '\\' if !no_backslash => out.push_str("\\\\"),
            '\0' if !no_backslash => out.push_str("\\0"),
            '\n' if !no_backslash => out.push_str("\\n"),
            '\r' if !no_backslash => out.push_str("\\r"),
            '\t' if !no_backslash => out.push_str("\\t"),
            '\u{8}' if !no_backslash => out.push_str("\\b"),
            '\u{1a}' if !no_backslash => out.push_str("\\Z"),
            _ => out.push(c),
        }
    }
    out.push('\'');
}

// Doris internal STRING storage uses UTF-8 literals in the VALUES transaction
// engine. Never cast X'...' to STRING: FE converts it to printable hex digits.
// UNHEX preserves arbitrary bytes. route_values selects execution-engine DML
// before the first VALUES write, including text-only statements, so FE's
// literal-only batch path is never entered by supported native transactions.
fn binary_literal(out: &mut String, value: &[u8], no_backslash: bool) {
    if let Ok(text) = std::str::from_utf8(value) {
        string_literal(out, text, no_backslash);
    } else {
        hex_literal(out, value);
    }
}
fn hex_literal(out: &mut String, value: &[u8]) {
    const HEX: &[u8; 16] = b"0123456789ABCDEF";
    out.push_str("UNHEX('");
    for &byte in value {
        out.push(char::from(HEX[usize::from(byte >> 4)]));
        out.push(char::from(HEX[usize::from(byte & 15)]));
    }
    out.push_str("')");
}

fn read_time(input: &mut Input<'_>) -> Result<crate::types::MySqlTime, Error> {
    use crate::types::{MySqlTime, MySqlTimeSign};
    let len = input.take(1)?[0] as usize;
    let value = if len == 0 {
        MySqlTime::ZERO
    } else {
        if !matches!(len, 8 | 12) {
            return Err(invalid("unsupported TIME encoding length"));
        }
        let b = input.take(len)?;
        let sign = match b[0] {
            0 => MySqlTimeSign::Positive,
            1 => MySqlTimeSign::Negative,
            _ => return Err(invalid("invalid TIME sign")),
        };
        let days = u32::from_le_bytes(b[1..5].try_into().unwrap());
        if b[5] > 23 {
            return Err(invalid("invalid TIME hour within day"));
        }
        let hours = days
            .checked_mul(24)
            .and_then(|hours| hours.checked_add(u32::from(b[5])))
            .ok_or_else(|| invalid("TIME day/hour overflow"))?;
        let micros = if len == 12 {
            u32::from_le_bytes(b[8..12].try_into().unwrap())
        } else {
            0
        };
        MySqlTime::new(sign, hours, b[6], b[7], micros)
            .map_err(|_| invalid("TIME fields outside native MySqlTime range"))?
    };
    Ok(value)
}
fn time_literal(out: &mut String, input: &mut Input<'_>) -> Result<(), Error> {
    let value = read_time(input)?;
    // Formatting only validated numeric fields avoids text-parser sign loss
    // for sub-hour negatives, and keeps TIME(6) precision in Doris casts.
    write!(out, "CAST('{value:.6}' AS TIME(6))").unwrap();
    Ok(())
}
fn integer(out: &mut String, value: &[u8], unsigned: bool) {
    let mut b = [0; 8];
    b[..value.len()].copy_from_slice(value);
    if unsigned {
        write!(out, "{}", u64::from_le_bytes(b)).unwrap();
    } else {
        if value[value.len() - 1] & 128 != 0 {
            b[value.len()..].fill(255);
        }
        write!(out, "{}", i64::from_le_bytes(b)).unwrap();
    }
}
fn decimal(value: &[u8]) -> Result<String, Error> {
    let s = std::str::from_utf8(value).map_err(|_| invalid("invalid decimal"))?;
    let (negative, s) = if let Some(s) = s.strip_prefix('-') {
        (true, s)
    } else {
        (false, s.strip_prefix('+').unwrap_or(s))
    };
    let mut parts = s.split(['e', 'E']);
    let mantissa = parts.next().unwrap_or("");
    let exponent = parts
        .next()
        .map(str::parse::<i32>)
        .transpose()
        .map_err(|_| invalid("invalid decimal exponent"))?
        .unwrap_or(0);
    if parts.next().is_some() || exponent.unsigned_abs() > 4096 {
        return Err(invalid("decimal exponent outside binding range"));
    }
    let mut point = mantissa.split('.');
    let whole = point.next().unwrap_or("");
    let fraction = point.next().unwrap_or("");
    if point.next().is_some()
        || whole.is_empty()
        || !whole.bytes().all(|b| b.is_ascii_digit())
        || !fraction.bytes().all(|b| b.is_ascii_digit())
        || whole.len() + fraction.len() > 4096
    {
        return Err(invalid("invalid decimal"));
    }
    let digits = format!("{whole}{fraction}");
    let scale = fraction.len() as i64 - i64::from(exponent);
    let mut out = if negative {
        String::from("-")
    } else {
        String::new()
    };
    if scale <= 0 {
        out.push_str(&digits);
        out.extend(std::iter::repeat_n('0', (-scale) as usize));
    } else if scale as usize >= digits.len() {
        out.push_str("0.");
        out.extend(std::iter::repeat_n('0', scale as usize - digits.len()));
        out.push_str(&digits);
    } else {
        let split = digits.len() - scale as usize;
        out.push_str(&digits[..split]);
        out.push('.');
        out.push_str(&digits[split..]);
    }
    Ok(out)
}
fn temporal(out: &mut String, input: &mut Input<'_>, date: bool) -> Result<(), Error> {
    let len = input.take(1)?[0] as usize;
    if !matches!(len, 4 | 7 | 11) || date && len != 4 {
        return Err(invalid("unsupported date/time encoding"));
    }
    let b = input.take(len)?;
    let year = u16::from_le_bytes([b[0], b[1]]);
    let month = b[2];
    let day = b[3];
    let leap = year % 4 == 0 && (year % 100 != 0 || year % 400 == 0);
    let days = match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 => {
            if leap {
                29
            } else {
                28
            }
        }
        _ => 0,
    };
    if year > 9999 || day == 0 || day > days {
        return Err(invalid("invalid calendar date"));
    }
    if date {
        write!(out, "CAST('{year:04}-{month:02}-{day:02}' AS DATE)").unwrap();
        return Ok(());
    }
    let (h, m, s) = if len >= 7 {
        (b[4], b[5], b[6])
    } else {
        (0, 0, 0)
    };
    let micros = if len == 11 {
        u32::from_le_bytes(b[7..11].try_into().unwrap())
    } else {
        0
    };
    if h > 23 || m > 59 || s > 59 || micros > 999999 {
        return Err(invalid("invalid clock time"));
    }
    write!(
        out,
        "CAST('{year:04}-{month:02}-{day:02} {h:02}:{m:02}:{s:02}.{micros:06}' AS DATETIME(6))"
    )
    .unwrap();
    Ok(())
}

/// Emulated PREPARE metadata is available for ordinary DML, validated on EXECUTE.
pub(super) fn dml_parameters(sql: &str) -> Result<usize, Error> {
    let positions = sites(sql)?;
    let keyword = sql
        .trim_start()
        .split(|c: char| !c.is_ascii_alphabetic())
        .next()
        .unwrap_or("");
    if !["INSERT", "UPDATE", "DELETE"]
        .iter()
        .any(|v| keyword.eq_ignore_ascii_case(v))
    {
        return Err(invalid(
            "explicit transaction prepare supports INSERT/UPDATE/DELETE only",
        ));
    }
    Ok(positions.len())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::MySqlTypeInfo;

    fn raw(ty: ColumnType, bytes: Vec<u8>) -> MySqlArguments {
        let mut a = MySqlArguments::default();
        a.add(0_i64).unwrap();
        a.types[0] = MySqlTypeInfo::binary(ty);
        a.values = bytes;
        a
    }
    fn text(ty: ColumnType, value: &str) -> MySqlArguments {
        let mut a = MySqlArguments::default();
        a.add(value).unwrap();
        a.types[0] = MySqlTypeInfo::binary(ty);
        a
    }
    #[test]
    fn lexical_quotes_identifiers_comments_and_trailing_terminator() {
        let s = "INSERT INTO `t?` VALUES (?, '?', \"?\", /* ? */ ?, ?) ; -- ?\n# ?";
        let p = sites(s).unwrap();
        assert_eq!(p.len(), 3);
        assert_eq!(
            sites("UPDATE t SET `a``?`=? WHERE v='it''s ?' AND x= ?")
                .unwrap()
                .len(),
            2
        );
        assert_eq!(
            sites("DELETE FROM t WHERE x=? /* safe */").unwrap().len(),
            1
        );
        assert_eq!(dml_parameters("INSERT INTO t VALUES(?,?)").unwrap(), 2);
        assert!(dml_parameters("SELECT ?").is_err());
    }
    #[test]
    fn ambiguous_and_executable_lexical_forms_fail_closed() {
        for s in [
            "SELECT 'x\\' ?, ?",
            "SELECT /* ?",
            "SELECT 'x",
            "SELECT ?; DELETE FROM t",
            "SELECT /*! ? */ ?",
            "SELECT /*+ SET_VAR(x=?) */ ?",
            "SELECT /*M! ? */ ?",
            "SELECT /* nested /* */ ?",
            "SELECT '''?'''",
            "SELECT ?\0",
            "SELECT `x\\` ? `",
        ] {
            assert!(sites(s).is_err(), "{s:?}");
        }
        // Both modes agree on the marker after this quoted backslash.
        assert_eq!(sites("INSERT INTO t VALUES('a\\b',?)").unwrap().len(), 1);
    }
    fn decode_literal(sql: &str, no_backslash: bool) -> String {
        let mut chars = sql[1..sql.len() - 1].chars().peekable();
        let mut out = String::new();
        while let Some(c) = chars.next() {
            if c == '\'' {
                assert_eq!(chars.next(), Some('\''));
                out.push(c);
            } else if c == '\\' && !no_backslash {
                let e = chars.next().unwrap();
                out.push(match e {
                    '0' => '\0',
                    'n' => '\n',
                    'r' => '\r',
                    't' => '\t',
                    'b' => '\u{8}',
                    'Z' => '\u{1a}',
                    '\\' => '\\',
                    _ => panic!("invalid generated escape"),
                });
            } else {
                out.push(c);
            }
        }
        out
    }
    #[test]
    fn arbitrary_text_is_a_single_mode_correct_literal() {
        let mut payload = String::from("中😀'\";DROP TABLE t; -- # /* ? \\");
        payload.extend((0_u8..=127).map(char::from));
        for mode in [false, true] {
            let mut literal = String::new();
            string_literal(&mut literal, &payload, mode);
            assert_eq!(decode_literal(&literal, mode), payload);
            let mut a = MySqlArguments::default();
            a.add(payload.as_str()).unwrap();
            assert_eq!(
                compile_mode("INSERT INTO t VALUES(?)", &a, mode).unwrap(),
                format!("INSERT INTO t VALUES(({literal}))")
            );
        }
    }
    #[test]
    fn mode_independent_bindings_include_exact_time_null_numbers_and_plain_text() {
        use crate::types::{MySqlTime, MySqlTimeSign};
        let mut a = MySqlArguments::default();
        a.add(i64::MIN).unwrap();
        a.add(u64::MAX).unwrap();
        a.add(MySqlTime::new(MySqlTimeSign::Negative, 27, 2, 3, 123456).unwrap())
            .unwrap();
        a.add(None::<String>).unwrap();
        a.add("O'Reilly 中文🙂").unwrap();
        a.add(vec![0xFF_u8, 0, 92]).unwrap();
        a.add(32_i64).unwrap();
        for mode in [false, true] {
            let bound = compile_modes("SELECT ?,?,?,?,?,? LIMIT ?", &a).unwrap();
            assert!(bound.independent());
            let text = bound.select(mode).unwrap();
            assert!(text.ends_with("LIMIT 32"));
            assert!(text.contains("-27:02:03.123456"));
            assert!(text.contains("18446744073709551615"));
            assert!(text.contains("O''Reilly 中文🙂"));
        }
    }
    #[test]
    fn mode_sensitive_bindings_are_selected_per_value_and_lexical_errors_stay_local() {
        for value in ["a\\b", "\0", "\n", "\r", "\t", "\u{8}", "\u{1a}"] {
            let mut a = MySqlArguments::default();
            a.add(value).unwrap();
            for mode in [false, true] {
                let bound = compile_modes("SELECT ?", &a).unwrap();
                assert!(!bound.independent(), "{value:?}");
                let text = bound.select(mode).unwrap();
                let literal = text
                    .strip_prefix("SELECT (")
                    .unwrap()
                    .strip_suffix(')')
                    .unwrap();
                assert_eq!(decode_literal(literal, mode), value);
            }
        }
        let mut a = MySqlArguments::default();
        a.add("ordinary").unwrap();
        assert!(compile_modes("SELECT ?", &a).unwrap().independent());
        assert!(compile_modes("SELECT ?,?", &a).is_err());
        assert!(compile_modes("SELECT 'x\\' ?, ?", &a).is_err());
    }
    #[test]
    fn empty_and_null_text_and_binary_are_distinct() {
        let mut a = MySqlArguments::default();
        a.add("").unwrap();
        a.add(None::<String>).unwrap();
        assert_eq!(
            compile("INSERT INTO t VALUES(?,?)", &a).unwrap(),
            "INSERT INTO t VALUES((''),(NULL))"
        );
        let mut a = MySqlArguments::default();
        a.add(Vec::<u8>::new()).unwrap();
        a.add(None::<Vec<u8>>).unwrap();
        assert_eq!(
            compile("INSERT INTO t VALUES(?,?)", &a).unwrap(),
            "INSERT INTO t VALUES((''),(NULL))"
        );
    }
    #[test]
    fn signed_unsigned_boolean_and_adjacency() {
        let mut a = MySqlArguments::default();
        a.add(i8::MIN).unwrap();
        a.add(i16::MIN).unwrap();
        a.add(i32::MIN).unwrap();
        a.add(i64::MIN).unwrap();
        a.add(u64::MAX).unwrap();
        a.add(true).unwrap();
        assert_eq!(compile("INSERT INTO t VALUES(?,?,?,?,?,?)",&a).unwrap(),"INSERT INTO t VALUES((-128),(-32768),(-2147483648),(-9223372036854775808),(18446744073709551615),(1))");
        let mut a = MySqlArguments::default();
        a.add(-1_i64).unwrap();
        a.add(-2_i64).unwrap();
        assert_eq!(compile("SELECT ?-?", &a).unwrap(), "SELECT (-1)-(-2)");
    }
    #[test]
    fn floats_preserve_negative_zero_and_reject_non_finite() {
        let mut a = MySqlArguments::default();
        a.add(-0.0_f64).unwrap();
        a.add(-0.0_f32).unwrap();
        a.add(f32::MAX).unwrap();
        let sql = compile("INSERT INTO t VALUES(?,?,?)", &a).unwrap();
        // Doris parses numeric -0.0 as positive zero; string casts retain its sign.
        assert!(sql.contains("CAST('-0.0' AS DOUBLE)"));
        assert!(sql.contains("CAST('-0.0' AS FLOAT)"));
        for v in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
            let mut a = MySqlArguments::default();
            a.add(v).unwrap();
            assert!(compile("INSERT INTO t VALUES(?)", &a).is_err());
        }
    }
    #[test]
    fn decimal_is_exact_without_floating_point_conversion() {
        for (s, want) in [
            ("1E+3", "1000"),
            ("-1.20e-4", "-0.000120"),
            (
                "+123456789012345678.123456789",
                "123456789012345678.123456789",
            ),
            (
                "1e-76",
                "0.0000000000000000000000000000000000000000000000000000000000000000000000000001",
            ),
        ] {
            assert_eq!(decimal(s.as_bytes()).unwrap(), want);
            assert_eq!(
                compile("INSERT INTO t VALUES(?)", &text(ColumnType::NewDecimal, s)).unwrap(),
                format!("INSERT INTO t VALUES(({want}))")
            );
        }
        for s in [
            "0);DROP TABLE t;--",
            "NaN",
            "1e",
            "1e1e2",
            "1..0",
            " 1",
            ".5",
            "1e9999999999",
        ] {
            assert!(decimal(s.as_bytes()).is_err(), "{s}");
        }
    }
    #[test]
    fn dates_and_microseconds_are_checked() {
        let a = raw(
            ColumnType::Datetime,
            vec![11, 0xea, 7, 10, 27, 12, 34, 56, 64, 226, 1, 0],
        );
        assert_eq!(
            compile("INSERT INTO t VALUES(?)", &a).unwrap(),
            "INSERT INTO t VALUES((CAST('2026-10-27 12:34:56.123456' AS DATETIME(6))))"
        );
        assert!(compile(
            "INSERT INTO t VALUES(?)",
            &raw(ColumnType::Date, vec![4, 0xe9, 7, 2, 29])
        )
        .is_err());
        assert!(compile(
            "INSERT INTO t VALUES(?)",
            &raw(ColumnType::Date, vec![4, 0, 0, 1, 1])
        )
        .is_ok());
        assert!(compile(
            "INSERT INTO t VALUES(?)",
            &raw(ColumnType::Datetime, vec![7, 0xea, 7, 10, 27, 24, 0, 0])
        )
        .is_err());
    }
    #[test]
    fn malformed_and_unsupported_encodings_are_rejected() {
        for b in [
            vec![],
            vec![255],
            vec![251],
            vec![254, 255, 255, 255, 255, 255, 255, 255, 255],
            vec![252, 1],
            vec![2, b'a'],
            vec![0, 0],
        ] {
            assert!(compile("INSERT INTO t VALUES(?)", &raw(ColumnType::VarString, b)).is_err());
        }
        assert!(compile(
            "INSERT INTO t VALUES(?)",
            &raw(ColumnType::LongLong, vec![0; 7])
        )
        .is_err());
        assert!(compile("INSERT INTO t VALUES(?)", &raw(ColumnType::Bit, vec![0])).is_err());
        assert!(compile(
            "INSERT INTO t VALUES(?)",
            &raw(ColumnType::VarString, vec![1, 255])
        )
        .is_err());
    }
    #[test]
    fn mismatched_count_is_preflight_error() {
        let a = MySqlArguments::default();
        assert!(compile("INSERT INTO t VALUES(?)", &a).is_err());
        assert_eq!(
            compile("INSERT INTO t VALUES(1)", &a).unwrap(),
            "INSERT INTO t VALUES(1)"
        );
    }
    #[test]
    fn deterministic_payload_corpus_round_trips_both_sql_modes() {
        let mut seed = 7_u64;
        for length in 0..512 {
            let value: String = (0..length)
                .map(|_| {
                    seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
                    char::from((seed >> 32) as u8 & 127)
                })
                .collect();
            for mode in [false, true] {
                let mut literal = String::new();
                string_literal(&mut literal, &value, mode);
                assert_eq!(decode_literal(&literal, mode), value);
            }
        }
    }

    #[test]
    fn binary_bytes_preserve_all_octets_without_lossy_utf8_or_hex_casts() {
        for ty in [
            ColumnType::Blob,
            ColumnType::TinyBlob,
            ColumnType::MediumBlob,
            ColumnType::LongBlob,
        ] {
            for mode in [false, true] {
                let payload = "中😀'\"; -- \\ \0\n\r\t\u{1a}";
                let mut a = MySqlArguments::default();
                a.add(payload.as_bytes()).unwrap();
                a.types[0] = MySqlTypeInfo::binary(ty);
                let query = compile_mode("INSERT INTO t VALUES(?)", &a, mode).unwrap();
                let literal = query
                    .strip_prefix("INSERT INTO t VALUES((")
                    .unwrap()
                    .strip_suffix("))")
                    .unwrap();
                assert_eq!(decode_literal(literal, mode).as_bytes(), payload.as_bytes());
            }
        }
        let mut payload: Vec<u8> = (0..=255).collect();
        payload.extend_from_slice(b"');DROP TABLE t;--\\?");
        let expected: String = payload.iter().map(|v| format!("{v:02X}")).collect();
        let mut a = MySqlArguments::default();
        a.add(payload).unwrap();
        for mode in [false, true] {
            assert_eq!(
                compile_mode("UPDATE t SET v=?", &a, mode).unwrap(),
                format!("UPDATE t SET v=(UNHEX('{expected}'))")
            );
        }
    }

    #[test]
    fn binary_length_prefixes_and_invalid_utf8_are_checked_without_panics() {
        for n in [0, 1, 250, 251, 255, 65535, 65536] {
            let value = vec![255_u8; n];
            let mut a = MySqlArguments::default();
            a.add(value).unwrap();
            let query = compile("UPDATE t SET v=?", &a).unwrap();
            if n == 0 {
                assert_eq!(query, "UPDATE t SET v=('')");
            } else {
                assert_eq!(
                    query,
                    format!("UPDATE t SET v=(UNHEX('{}'))", "FF".repeat(n))
                );
            }
        }
        for bytes in [vec![], vec![252, 1], vec![2, 255], vec![0, 0]] {
            assert!(compile("UPDATE t SET v=?", &raw(ColumnType::Blob, bytes)).is_err());
        }
    }

    #[test]
    fn native_time_zero_signed_subhour_days_and_boundaries_are_exact() {
        use crate::types::{MySqlTime, MySqlTimeSign};
        for value in [
            MySqlTime::ZERO,
            MySqlTime::MIN,
            MySqlTime::MAX,
            MySqlTime::new(MySqlTimeSign::Negative, 0, 0, 0, 1).unwrap(),
            MySqlTime::new(MySqlTimeSign::Positive, 25, 2, 3, 123456).unwrap(),
            MySqlTime::new(MySqlTimeSign::Negative, 838, 59, 58, 999999).unwrap(),
        ] {
            let mut a = MySqlArguments::default();
            a.add(value).unwrap();
            assert_eq!(
                compile("INSERT INTO t VALUES(?)", &a).unwrap(),
                format!("INSERT INTO t VALUES((CAST('{value:.6}' AS TIME(6))))")
            );
        }
        let mut a = MySqlArguments::default();
        a.add(None::<MySqlTime>).unwrap();
        assert_eq!(
            compile("INSERT INTO t VALUES(?)", &a).unwrap(),
            "INSERT INTO t VALUES((NULL))"
        );
    }

    #[test]
    fn malformed_native_time_is_rejected_before_request() {
        for bytes in [
            vec![],
            vec![1],
            vec![7, 0, 0, 0, 0, 0, 0, 0],
            vec![8, 0],
            vec![8, 2, 0, 0, 0, 0, 1, 0, 0],         // invalid sign
            vec![8, 0, 255, 255, 255, 255, 0, 0, 0], // days overflow
            vec![8, 0, 0, 0, 0, 0, 24, 0, 0],
            vec![8, 0, 0, 0, 0, 0, 1, 60, 0],
            vec![8, 0, 0, 0, 0, 0, 1, 0, 60],
            vec![8, 1, 0, 0, 0, 0, 0, 0, 0],  // negative zero
            vec![8, 0, 35, 0, 0, 0, 0, 0, 0], // 840 hours
            vec![12, 0, 0, 0, 0, 0, 1, 0, 0, 64, 66, 15, 0], // 1,000,000 micros
            vec![12, 0, 34, 0, 0, 0, 22, 59, 59, 1, 0, 0, 0], // native max + 1us
            vec![0, 0],                       // trailing data
        ] {
            assert!(
                compile(
                    "INSERT INTO t VALUES(?)",
                    &raw(ColumnType::Time, bytes.clone())
                )
                .is_err(),
                "{bytes:?}"
            );
        }
    }

    #[test]
    fn values_route_uses_identity_without_union_or_target_type_coercion() {
        for sql in [
            "INSERT INTO t VALUES(1,'x')",
            "INSERT INTO t(a,b) VALUES(1,9007199254740993),(2,CAST(1.25 AS DOUBLE))",
        ] {
            let q = route_values(sql, false).unwrap().unwrap();
            assert!(q.contains("IF(@@sql_mode IS NULL,1,1)"), "{q}");
            assert!(!q.contains("UNION"));
            assert!(q.contains("VALUES"));
        }
        assert_eq!(
            route_values("INSERT INTO t VALUES(NULL)", false)
                .unwrap()
                .unwrap(),
            "INSERT INTO t VALUES(IF(@@sql_mode IS NULL,NULL,NULL))"
        );
        assert_eq!(
            route_values("INSERT INTO t VALUES(DEFAULT,42)", false)
                .unwrap()
                .unwrap(),
            "INSERT INTO t VALUES(DEFAULT,IF(@@sql_mode IS NULL,42,42))"
        );
        for sql in [
            "INSERT INTO t SELECT 1",
            "UPDATE t SET v=1",
            "DELETE FROM t WHERE id=1",
            "SELECT 1",
        ] {
            assert!(route_values(sql, false).unwrap().is_none());
        }
    }

    #[test]
    fn values_route_preserves_quotes_comments_labels_partitions_and_nested_expressions() {
        let sql="/* VALUES */ INSERT INTO `t VALUES` PARTITION(p) WITH LABEL l (`a`,`b`) VALUES(/*x*/concat('a,b',')'),[1,2]), (42,'x;?') ; -- trailing";
        let q = route_values(sql, false).unwrap().unwrap();
        assert!(q.contains("IF(@@sql_mode IS NULL,42,42)"));
        assert!(q.starts_with("/* VALUES */ INSERT INTO `t VALUES` PARTITION(p) WITH LABEL l"));
        assert!(q.ends_with("; -- trailing"));
        let sql = "INSERT INTO t VALUES(1 -- comment\n,'x')";
        assert_eq!(
            route_values(sql, false).unwrap().unwrap(),
            "INSERT INTO t VALUES(IF(@@sql_mode IS NULL,1,1) -- comment\n,'x')"
        );
    }

    #[test]
    fn malformed_values_routes_fail_before_wire() {
        for sql in [
            "INSERT INTO t VALUES()",
            "INSERT INTO t VALUES(DEFAULT,)",
            "INSERT INTO t VALUES(1),(2,3)",
            "INSERT INTO t VALUES(,1)",
            "INSERT INTO t VALUES(1,)",
            "INSERT INTO t VALUES([1,2)",
            "INSERT INTO t VALUES(1),",
            "INSERT INTO t VALUES(1); DELETE FROM t",
            "INSERT INTO t VALUES(?)",
            "INSERT INTO t VALUES(1) ON DUPLICATE KEY UPDATE v=1",
        ] {
            assert!(route_values(sql, false).is_err(), "{sql}");
        }
    }

    #[test]
    fn bound_binary_and_text_literals_remain_safe_after_values_routing() {
        for mode in [false, true] {
            for bytes in [
                vec![0, 255, 128, 39, 92],
                "'\\\0;-- ? 中".as_bytes().to_vec(),
            ] {
                let mut a = MySqlArguments::default();
                a.add(bytes).unwrap();
                a.add(signed_time_fixture()).unwrap();
                let q = compile_mode("INSERT INTO t VALUES(?,?)", &a, mode).unwrap();
                let routed = route_values(&q, mode).unwrap().unwrap();
                assert!(routed.contains("IF(@@sql_mode IS NULL,"));
                assert!(scan(&routed, !mode, true).unwrap().sites.is_empty());
            }
        }
        assert!(route_values("INSERT INTO t VALUES(\0)", true).is_err());
    }
    fn signed_time_fixture() -> crate::types::MySqlTime {
        crate::types::MySqlTime::new(crate::types::MySqlTimeSign::Negative, 0, 0, 0, 1).unwrap()
    }

    #[test]
    fn blob_equalities_preserve_placeholders_and_wrap_only_column_operands() {
        for (sql, want) in [
            (
                "SELECT id FROM t WHERE b = ?",
                "SELECT id FROM t WHERE HEX(b) = ?",
            ),
            (
                "SELECT id FROM t a WHERE ? = a.`b?`",
                "SELECT id FROM t a WHERE ? = HEX(a.`b?`)",
            ),
            (
                "DELETE FROM t WHERE ((b)) = ((?))",
                "DELETE FROM t WHERE HEX(((b))) = ((?))",
            ),
            ("SELECT b=? FROM t", "SELECT HEX(b)=? FROM t"),
            (
                "SELECT id FROM t WHERE NOT (b /*c*/ = /*p*/ ?)",
                "SELECT id FROM t WHERE NOT (HEX(b) /*c*/ = /*p*/ ?)",
            ),
        ] {
            let mut a = MySqlArguments::default();
            a.add(vec![0_u8, 255, 128]).unwrap();
            assert_eq!(adapt_parameters(sql, &mut a).unwrap().unwrap(), want);
            let mut expected = MySqlArguments::default();
            expected.add("00FF80").unwrap();
            assert_eq!(a.values, expected.values);
            assert_eq!(a.types, expected.types);
            assert_eq!(sites(want).unwrap().len(), 1);
        }
    }

    #[test]
    fn blob_set_assignments_remain_values_while_where_becomes_byte_comparison() {
        let mut a = MySqlArguments::default();
        a.add(vec![255_u8, 0]).unwrap();
        a.add(7_i64).unwrap();
        a.add(vec![0_u8, 128]).unwrap();
        let sql = "UPDATE t SET b=?,n=? WHERE t.b=?";
        assert_eq!(
            adapt_parameters(sql, &mut a).unwrap().unwrap(),
            "UPDATE t SET b=UNHEX(CONCAT(?,SUBSTRING(UUID(),1,0))),n=? WHERE HEX(t.b)=?"
        );
        let mut want = MySqlArguments::default();
        want.add("FF00").unwrap();
        want.add(7_i64).unwrap();
        want.add("0080").unwrap();
        assert_eq!(a.values, want.values);
        assert_eq!(a.types, want.types);
        let mut nested = MySqlArguments::default();
        for _ in 0..3 {
            nested.add(vec![255_u8]).unwrap();
        }
        assert_eq!(
            adapt_parameters(
                "UPDATE t SET n=(SELECT COUNT(*) FROM t WHERE b=?),b=? WHERE b=?",
                &mut nested
            )
            .unwrap()
            .unwrap(),
            "UPDATE t SET n=(SELECT COUNT(*) FROM t WHERE HEX(b)=?),b=UNHEX(CONCAT(?,SUBSTRING(UUID(),1,0))) WHERE HEX(b)=?"
        );
        assert_eq!(
            adapt_parameters("SELECT 1", &mut MySqlArguments::default()).unwrap(),
            None
        );
    }

    #[test]
    fn blob_compound_conditions_subquery_scopes_and_existing_hex_work() {
        let mut a = MySqlArguments::default();
        a.add(vec![0_u8, 255]).unwrap();
        a.add(vec![128_u8]).unwrap();
        assert_eq!(
            adapt_parameters("SELECT a.id FROM t a JOIN t c ON a.b=? WHERE c.b=?", &mut a)
                .unwrap()
                .unwrap(),
            "SELECT a.id FROM t a JOIN t c ON HEX(a.b)=? WHERE HEX(c.b)=?"
        );
        let mut a = MySqlArguments::default();
        a.add(vec![255_u8]).unwrap();
        a.add(vec![128_u8]).unwrap();
        assert_eq!(
            adapt_parameters(
                "SELECT id FROM t WHERE id IN(SELECT id FROM t WHERE b=?) AND b=?",
                &mut a
            )
            .unwrap()
            .unwrap(),
            "SELECT id FROM t WHERE id IN(SELECT id FROM t WHERE HEX(b)=?) AND HEX(b)=?"
        );
        let mut a = MySqlArguments::default();
        a.add(vec![255_u8]).unwrap();
        assert_eq!(
            adapt_parameters("DELETE FROM t WHERE HEX(b)=HEX(?)", &mut a)
                .unwrap()
                .unwrap(),
            "DELETE FROM t WHERE HEX(b)=HEX(UNHEX(CONCAT(?,SUBSTRING(UUID(),1,0))))"
        );
    }

    #[test]
    fn blob_null_empty_utf8_and_four_types_keep_distinct_values() {
        for ty in [
            ColumnType::Blob,
            ColumnType::TinyBlob,
            ColumnType::MediumBlob,
            ColumnType::LongBlob,
        ] {
            let mut a = MySqlArguments::default();
            a.add(None::<Vec<u8>>).unwrap();
            a.add(Vec::<u8>::new()).unwrap();
            a.add("中'\0".as_bytes()).unwrap();
            for info in &mut a.types {
                info.r#type = ty;
            }
            let q = adapt_parameters("SELECT b FROM t WHERE b=? OR b=? OR b=?", &mut a)
                .unwrap()
                .unwrap();
            assert_eq!(q.matches("HEX(b)").count(), 3);
            let mut want = MySqlArguments::default();
            want.add(None::<String>).unwrap();
            want.add("").unwrap();
            want.add("E4B8AD2700").unwrap();
            assert_eq!(a.values, want.values);
            assert_eq!(&*a.null_bitmap, &*want.null_bitmap);
            assert_eq!(a.types, want.types);
        }
    }

    #[test]
    fn blob_reencoding_preserves_other_binary_arguments_exactly() {
        let mut a = MySqlArguments::default();
        a.add(i64::MIN).unwrap();
        a.add(vec![255_u8, 0]).unwrap();
        a.add("'\\\0").unwrap();
        a.add(signed_time_fixture()).unwrap();
        a.add(None::<i32>).unwrap();
        a.add(-0.0_f64).unwrap();
        let q = adapt_parameters("INSERT INTO t VALUES(?,?,?,?,?,?)", &mut a)
            .unwrap()
            .unwrap();
        assert_eq!(
            q,
            "INSERT INTO t VALUES(?,UNHEX(CONCAT(?,SUBSTRING(UUID(),1,0))),?,?,?,?)"
        );
        let mut want = MySqlArguments::default();
        want.add(i64::MIN).unwrap();
        want.add("FF00").unwrap();
        want.add("'\\\0").unwrap();
        want.add(signed_time_fixture()).unwrap();
        want.add(None::<i32>).unwrap();
        want.add(-0.0_f64).unwrap();
        assert_eq!(a.values, want.values);
        assert_eq!(a.types, want.types);
        assert_eq!(&*a.null_bitmap, &*want.null_bitmap);
    }

    #[test]
    fn unsupported_blob_conditions_fail_closed_without_mutating_arguments() {
        for sql in [
            "SELECT id FROM t WHERE b<=(?+1)",
            "SELECT id FROM t WHERE b IN(?+1)",
            "SELECT id FROM t WHERE concat(b,'x')=?",
            "SELECT id FROM t WHERE b+z=?",
            "SELECT id FROM t WHERE ?=b+z",
            "SELECT id FROM t WHERE b=?=x",
            "SELECT id FROM t WHERE x=b=?",
            "SELECT id FROM t WHERE b LIKE ?",
            "SELECT CASE WHEN b=? THEN 1 ELSE 0 END FROM t",
            "SELECT id FROM t WHERE b=?;DELETE FROM t",
            "WITH x AS(SELECT 1) SELECT * FROM x WHERE b=?",
        ] {
            let mut a = MySqlArguments::default();
            a.add(vec![255_u8, 0]).unwrap();
            let original = a.clone();
            assert!(adapt_parameters(sql, &mut a).is_err(), "{sql}");
            assert_eq!(a.values, original.values);
            assert_eq!(a.types, original.types);
        }
    }

    #[test]
    fn malformed_blob_encoding_or_count_is_rejected_before_request() {
        for bytes in [
            vec![],
            vec![2, 255],
            vec![0, 0],
            vec![252, 1],
            vec![254, 255, 255, 255, 255, 255, 255, 255, 255],
        ] {
            let mut a = raw(ColumnType::Blob, bytes);
            assert!(adapt_parameters("SELECT id FROM t WHERE b=?", &mut a).is_err());
        }
        let mut a = MySqlArguments::default();
        a.add(vec![255_u8]).unwrap();
        assert!(adapt_parameters("SELECT 1", &mut a).is_err());
    }

    #[test]
    fn binary_rewrite_treats_payload_and_comment_question_marks_as_data() {
        let mut value: Vec<u8> = (0..=255).collect();
        value.extend_from_slice(b"';DROP TABLE t;--?");
        let mut a = MySqlArguments::default();
        a.add(value.clone()).unwrap();
        let q = adapt_parameters("SELECT '?' FROM `t?` WHERE `b?`= ? /* ? */", &mut a)
            .unwrap()
            .unwrap();
        assert_eq!(q, "SELECT '?' FROM `t?` WHERE HEX(`b?`)= ? /* ? */");
        assert!(!q.contains("DROP"));
        let mut expected = MySqlArguments::default();
        expected
            .add(value.iter().map(|b| format!("{b:02X}")).collect::<String>())
            .unwrap();
        assert_eq!(a.values, expected.values);
    }

    #[test]
    fn blob_membership_wraps_column_once_and_keeps_scalar_markers() {
        for (sql, want) in [
            ("SELECT b IN(?,?) FROM t", "SELECT HEX(b) IN(?,?) FROM t"),
            (
                "DELETE FROM t WHERE b NOT IN(?,NULL,?)",
                "DELETE FROM t WHERE HEX(b) NOT IN(?,NULL,?)",
            ),
            (
                "SELECT id FROM t a WHERE NOT ((a.`b?`) IN(((?)), /* ? */ (?)))",
                "SELECT id FROM t a WHERE NOT (HEX((a.`b?`)) IN(((?)), /* ? */ (?)))",
            ),
            (
                "SELECT id FROM t WHERE b IN(?,?) IS NOT NULL",
                "SELECT id FROM t WHERE (HEX(b) IN(?,?)) IS NOT NULL",
            ),
            (
                "SELECT CAST(b IN(?,?) AS SIGNED) FROM t",
                "SELECT CAST(HEX(b) IN(?,?) AS SIGNED) FROM t",
            ),
        ] {
            let mut args = MySqlArguments::default();
            args.add(vec![255_u8, 0]).unwrap();
            args.add(Vec::<u8>::new()).unwrap();
            assert_eq!(adapt_parameters(sql, &mut args).unwrap().unwrap(), want);
            let mut expected = MySqlArguments::default();
            expected.add("FF00").unwrap();
            expected.add("").unwrap();
            assert_eq!(args.values, expected.values);
            assert_eq!(args.types, expected.types);
            assert_eq!(sites(want).unwrap().len(), 2);
        }
    }

    #[test]
    fn blob_membership_hexes_mixed_strings_but_never_filters_nulls() {
        let mut args = MySqlArguments::default();
        args.add(None::<Vec<u8>>).unwrap();
        args.add("a").unwrap();
        args.add(vec![255_u8]).unwrap();
        args.add(None::<i64>).unwrap();
        let mut expected = MySqlArguments::default();
        expected.add(None::<String>).unwrap();
        expected.add("a").unwrap();
        expected.add("FF").unwrap();
        expected.add(None::<i64>).unwrap();
        assert_eq!(
            adapt_parameters("SELECT b NOT IN(?,(?),NULL,'A',?,?) FROM t", &mut args)
                .unwrap()
                .unwrap(),
            "SELECT HEX(b) NOT IN(?,HEX((?)),NULL,HEX('A'),?,?) FROM t"
        );
        assert_eq!(args.values, expected.values);
        assert_eq!(args.types, expected.types);
        assert_eq!(&*args.null_bitmap, &*expected.null_bitmap);
    }

    #[test]
    fn blob_membership_scopes_coexist_with_assignments_equalities_and_subqueries() {
        let mut args = MySqlArguments::default();
        for _ in 0..5 {
            args.add(vec![255_u8]).unwrap();
        }
        let sql="UPDATE t SET b=? WHERE id IN(SELECT id FROM t WHERE b IN(?,?)) AND (b=? OR b NOT IN(?))";
        assert_eq!(adapt_parameters(sql,&mut args).unwrap().unwrap(),"UPDATE t SET b=UNHEX(CONCAT(?,SUBSTRING(UUID(),1,0))) WHERE id IN(SELECT id FROM t WHERE HEX(b) IN(?,?)) AND (HEX(b)=? OR HEX(b) NOT IN(?))");
        let mut args = MySqlArguments::default();
        args.add(vec![255_u8]).unwrap();
        args.add(1_i64).unwrap();
        assert_eq!(
            adapt_parameters(
                "SELECT a.id FROM t a JOIN t c ON a.b IN(?) WHERE c.id IN(?)",
                &mut args
            )
            .unwrap()
            .unwrap(),
            "SELECT a.id FROM t a JOIN t c ON HEX(a.b) IN(?) WHERE c.id IN(?)"
        );
    }

    #[test]
    fn blob_membership_rejects_ambiguous_lists_and_coercion_without_mutation() {
        for sql in [
            "SELECT id FROM t WHERE b IN(?+1)",
            "SELECT id FROM t WHERE b IN(?,1)",
            "SELECT id FROM t WHERE b IN(?,id)",
            "SELECT id FROM t WHERE b NOT IN(?,concat('a','b'))",
            "SELECT id FROM t WHERE concat(b,'x') IN(?)",
            "SELECT id FROM t WHERE (b,id) IN((?,1))",
            "SELECT id FROM t WHERE b IN(? COLLATE utf8mb4_bin)",
            "SELECT id FROM t WHERE b IN(?)=1",
            "SELECT id FROM t WHERE b IN(?,)",
            "SELECT ? IN(b) FROM t",
            "SELECT CASE WHEN b IN(?) THEN 1 ELSE 0 END FROM t",
            "SELECT id FROM t WHERE b IN(?,\"name\")",
            "SELECT id FROM t WHERE b IN(SELECT ?)",
            "SELECT id FROM t WHERE b NOT IN(SELECT ? FROM t)",
            "SELECT id FROM t WHERE b IN(SELECT (SELECT id FROM t),? FROM t)",
            "SELECT id FROM t WHERE b IN(SELECT c.from,? FROM t c)",
        ] {
            let mut args = MySqlArguments::default();
            args.add(vec![255_u8]).unwrap();
            let before = args.clone();
            assert!(adapt_parameters(sql, &mut args).is_err(), "{sql}");
            assert_eq!(args.values, before.values);
            assert_eq!(args.types, before.types);
        }
        let mut args = MySqlArguments::default();
        args.add(vec![255_u8]).unwrap();
        args.add(1_i64).unwrap();
        assert!(adapt_parameters("SELECT id FROM t WHERE b IN(?,?)", &mut args).is_err());
    }

    #[test]
    fn blob_membership_many_parameters_keep_all_null_bitmap_bits_and_order() {
        let mut args = MySqlArguments::default();
        let mut expected = MySqlArguments::default();
        for i in 0..257_u16 {
            let b = (i % 3 != 0).then_some(vec![255_u8, (i % 256) as u8]);
            expected
                .add(b.as_ref().map(|b| format!("{:02X}{:02X}", b[0], b[1])))
                .unwrap();
            args.add(b).unwrap();
        }
        let sql = format!(
            "SELECT id FROM t WHERE b NOT IN({})",
            vec!["?"; 257].join(",")
        );
        let adapted = adapt_parameters(&sql, &mut args).unwrap().unwrap();
        assert_eq!(adapted.matches("HEX(b)").count(), 1);
        assert_eq!(sites(&adapted).unwrap().len(), 257);
        assert_eq!(args.values, expected.values);
        assert_eq!(args.types, expected.types);
        assert_eq!(&*args.null_bitmap, &*expected.null_bitmap);
    }

    #[test]
    fn blob_membership_payload_is_never_sql_and_four_blob_kinds_remain_native() {
        for kind in [
            ColumnType::Blob,
            ColumnType::TinyBlob,
            ColumnType::MediumBlob,
            ColumnType::LongBlob,
        ] {
            let mut args = MySqlArguments::default();
            args.add(b"\xff');DROP TABLE t;--?".as_slice()).unwrap();
            args.types[0].r#type = kind;
            assert_eq!(
                adapt_parameters("SELECT '?' FROM `t?` WHERE b IN(?,'?') /* ? */", &mut args)
                    .unwrap()
                    .unwrap(),
                "SELECT '?' FROM `t?` WHERE HEX(b) IN(?,HEX('?')) /* ? */"
            );
            let q =
                compile_mode("SELECT b FROM t WHERE HEX(b) IN(?,HEX('?'))", &args, true).unwrap();
            assert!(!q.contains("DROP TABLE"));
            assert!(scan(&q, false, true).unwrap().sites.is_empty());
        }
    }

    #[test]
    fn blob_comparisons_keep_each_operator_and_both_operand_orders() {
        for op in ["<>", "!=", "<=>"] {
            for (sql, want) in [
                (
                    format!("SELECT id FROM t WHERE b {op} ?"),
                    format!("SELECT id FROM t WHERE HEX(b) {op} ?"),
                ),
                (
                    format!("SELECT CAST(?{op}a.`b?` AS SIGNED) FROM t a"),
                    format!("SELECT CAST(?{op}HEX(a.`b?`) AS SIGNED) FROM t a"),
                ),
                (
                    format!("SELECT ((a.b)) {op} ((?)) AS p FROM t a"),
                    format!("SELECT HEX(((a.b))) {op} ((?)) AS p FROM t a"),
                ),
            ] {
                let mut args = MySqlArguments::default();
                args.add(vec![255_u8, 0, 128]).unwrap();
                assert_eq!(adapt_parameters(&sql, &mut args).unwrap().unwrap(), want);
                let mut expected = MySqlArguments::default();
                expected.add("FF0080").unwrap();
                assert_eq!(args.values, expected.values);
                assert_eq!(args.types, expected.types);
                assert_eq!(sites(&want).unwrap().len(), 1);
            }
        }
    }

    #[test]
    fn blob_null_safe_and_not_equal_keep_null_bits_and_all_blob_kinds() {
        for kind in [
            ColumnType::Blob,
            ColumnType::TinyBlob,
            ColumnType::MediumBlob,
            ColumnType::LongBlob,
        ] {
            let mut args = MySqlArguments::default();
            args.add(None::<Vec<u8>>).unwrap();
            args.add(Vec::<u8>::new()).unwrap();
            args.add(vec![255_u8, 0]).unwrap();
            for ty in &mut args.types {
                ty.r#type = kind;
            }
            assert_eq!(
                adapt_parameters("SELECT b<=>?,b<>?,?!=b FROM t", &mut args)
                    .unwrap()
                    .unwrap(),
                "SELECT HEX(b)<=>?,HEX(b)<>?,?!=HEX(b) FROM t"
            );
            let mut expected = MySqlArguments::default();
            expected.add(None::<String>).unwrap();
            expected.add("").unwrap();
            expected.add("FF00").unwrap();
            assert_eq!(args.types, expected.types);
            assert_eq!(args.values, expected.values);
            assert_eq!(&*args.null_bitmap, &*expected.null_bitmap);
            assert_eq!(
                compile("SELECT HEX(b)<=>?,HEX(b)<>?,?!=HEX(b) FROM t", &args).unwrap(),
                "SELECT HEX(b)<=>(NULL),HEX(b)<>(''),('FF00')!=HEX(b) FROM t"
            );
        }
    }

    #[test]
    fn blob_comparisons_do_not_treat_assignments_or_membership_as_operators() {
        let mut args = MySqlArguments::default();
        args.add(vec![255_u8]).unwrap();
        args.add(7_i64).unwrap();
        for _ in 0..4 {
            args.add(vec![128_u8]).unwrap();
        }
        let sql = "UPDATE t SET b=?,n=? WHERE b<>? AND ?!=b AND (b<=>? OR b IN(?))";
        assert_eq!(adapt_parameters(sql,&mut args).unwrap().unwrap(),"UPDATE t SET b=UNHEX(CONCAT(?,SUBSTRING(UUID(),1,0))),n=? WHERE HEX(b)<>? AND ?!=HEX(b) AND (HEX(b)<=>? OR HEX(b) IN(?))");
        let mut expected = MySqlArguments::default();
        expected.add("FF").unwrap();
        expected.add(7_i64).unwrap();
        for _ in 0..4 {
            expected.add("80").unwrap();
        }
        assert_eq!(args.values, expected.values);
        assert_eq!(args.types, expected.types);
        let mut args = MySqlArguments::default();
        args.add(vec![255_u8]).unwrap();
        assert_eq!(
            adapt_parameters(
                "SELECT id FROM t WHERE id IN(SELECT id FROM t WHERE b<=>?)",
                &mut args
            )
            .unwrap()
            .unwrap(),
            "SELECT id FROM t WHERE id IN(SELECT id FROM t WHERE HEX(b)<=>?)"
        );
    }

    #[test]
    fn blob_comparisons_reject_split_invalid_chained_or_complex_operators() {
        for sql in [
            "SELECT b>(?+1) FROM t",
            "SELECT ?<=concat(b,'x') FROM t",
            "SELECT b ! = ? FROM t",
            "SELECT b < = > ? FROM t",
            "SELECT b !/*x*/=? FROM t",
            "SELECT b<==>? FROM t",
            "SELECT b<=>=? FROM t",
            "SELECT b==? FROM t",
            "SELECT b<=>?=x FROM t",
            "SELECT x=b!=? FROM t",
            "SELECT concat(b,'x')<>? FROM t",
            "SELECT ?<=>b+x FROM t",
            "SELECT CASE WHEN b<=>? THEN 1 ELSE 0 END FROM t",
        ] {
            let mut args = MySqlArguments::default();
            args.add(vec![255_u8]).unwrap();
            let original = args.clone();
            assert!(adapt_parameters(sql, &mut args).is_err(), "{sql}");
            assert_eq!(args.values, original.values);
            assert_eq!(args.types, original.types);
            assert_eq!(&*args.null_bitmap, &*original.null_bitmap);
        }
    }

    #[test]
    fn blob_comparison_payloads_are_not_sql_and_operator_comments_are_preserved() {
        let mut args = MySqlArguments::default();
        args.add(b"\xff');DROP TABLE t;--?".as_slice()).unwrap();
        let sql = "SELECT '?' FROM `t?` WHERE `b?` /* left */ <=> /* right ? */ ?";
        let q = adapt_parameters(sql, &mut args).unwrap().unwrap();
        assert_eq!(
            q,
            "SELECT '?' FROM `t?` WHERE HEX(`b?`) /* left */ <=> /* right ? */ ?"
        );
        assert!(!q.contains("DROP"));
        assert_eq!(sites(&q).unwrap().len(), 1);
        for mode in [false, true] {
            let q = compile_mode(&q, &args, mode).unwrap();
            assert!(!q.contains("DROP"));
            assert!(scan(&q, !mode, true).unwrap().sites.is_empty());
        }
        let mut args = MySqlArguments::default();
        args.add(vec![255_u8]).unwrap();
        assert_eq!(
            adapt_parameters("SELECT id FROM t WHERE HEX(b)!=HEX(?)", &mut args)
                .unwrap()
                .unwrap(),
            "SELECT id FROM t WHERE HEX(b)!=HEX(UNHEX(CONCAT(?,SUBSTRING(UUID(),1,0))))"
        );
    }

    #[test]
    fn blob_comparison_null_bitmap_and_native_argument_order_cross_byte_boundaries() {
        let mut args = MySqlArguments::default();
        let mut expected = MySqlArguments::default();
        args.add(i64::MIN).unwrap();
        expected.add(i64::MIN).unwrap();
        for i in 0..17_u8 {
            let b = (i % 3 != 0).then_some(vec![255_u8, i]);
            expected
                .add(b.as_ref().map(|b| format!("{:02X}{:02X}", b[0], b[1])))
                .unwrap();
            args.add(b).unwrap();
        }
        args.add(signed_time_fixture()).unwrap();
        expected.add(signed_time_fixture()).unwrap();
        let sql = format!(
            "UPDATE t SET n=?,b=b WHERE {} AND t=?",
            vec!["b<=>?"; 17].join(" OR ")
        );
        let q = adapt_parameters(&sql, &mut args).unwrap().unwrap();
        assert_eq!(q.matches("HEX(b)<=>?").count(), 17);
        assert_eq!(sites(&q).unwrap().len(), 19);
        assert_eq!(args.values, expected.values);
        assert_eq!(args.types, expected.types);
        assert_eq!(&*args.null_bitmap, &*expected.null_bitmap);
    }
    #[test]
    fn time_adaptation_preserves_native_bytes_and_orders_columns_as_durations() {
        for op in ["=", "<>", "!=", "<=>", "<", ">", "<=", ">="] {
            let mut a = MySqlArguments::default();
            a.add(signed_time_fixture()).unwrap();
            let bytes = a.values.clone();
            let q = adapt_parameters(&format!("SELECT id FROM t WHERE t {op} ?"), &mut a)
                .unwrap()
                .unwrap();
            assert_eq!(
                q,
                format!("SELECT id FROM t WHERE CAST(t AS TIME(6)) {op} ?")
            );
            assert_eq!(a.values, bytes);
            assert_eq!(a.types[0].r#type, ColumnType::Time);
            assert!(compile(&q, &a)
                .unwrap()
                .contains("CAST('-0:00:00.000001' AS TIME(6))"));
        }
    }
    #[test]
    fn time_membership_null_and_mixed_parameter_domains_are_checked() {
        let mut a = MySqlArguments::default();
        a.add(signed_time_fixture()).unwrap();
        a.add(None::<crate::types::MySqlTime>).unwrap();
        let q = adapt_parameters("SELECT t IN(?,NULL,?,'25:00:00') FROM t", &mut a)
            .unwrap()
            .unwrap();
        assert!(q.starts_with("SELECT REPLACE(CAST(CAST(t AS TIME(6)) AS CHAR)"));
        assert_eq!(q.matches("REPLACE(").count(), 4);
        assert_eq!(sites(&q).unwrap().len(), 2);
        assert!(compile(&q, &a).unwrap().contains("(NULL)"));
        let mut a = MySqlArguments::default();
        a.add(signed_time_fixture()).unwrap();
        a.add(vec![255_u8]).unwrap();
        let bytes = a.values.clone();
        assert!(adapt_parameters("SELECT t IN(?,?) FROM t", &mut a).is_err());
        assert_eq!(a.values, bytes);
    }
}

#[cfg(test)]
mod generated;
