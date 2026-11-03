//! Checked Doris parameter adaptation and typed public-protocol binding.
//! The logger keeps original SQL. Typed literals use the session's actual sql_mode.
use super::DorisArguments;
use super::{ColumnFlags, ColumnType};
use sqlx_core::Error;
use std::fmt::Write;

mod syntax;
use syntax::*;

fn invalid(message: &str) -> Error {
    Error::Protocol(format!("Doris client binding: {message}"))
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
// Preserve a VARCHAR context before Doris's TIME cast: direct parameter type
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
// Keep VARCHAR context before Doris's TIME cast. Direct parameter propagation
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
pub(crate) fn has_time(args: &DorisArguments) -> bool {
    args.types.iter().any(|ty| ty.r#type == ColumnType::Time)
}

pub(crate) fn has_bound_pagination(sql: &str) -> Result<bool, Error> {
    Ok(!pagination_sites(sql)?.is_empty())
}

// Normalize only the common INSERT INTO qualified_table [(columns)] VALUE (...)
// alias, never a quoted identifier, string, label or nested expression.
// Unknown syntax retains server validation.
pub(crate) fn normalize_insert_value(sql: &str) -> Result<Option<String>, Error> {
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

pub(crate) fn adapt_query(
    sql: &str,
    args: Option<&mut DorisArguments>,
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
pub(crate) fn adapt_parameters(
    sql: &str,
    args: &mut DorisArguments,
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
                    return Err(invalid(
                        "binary predicates require a simple column comparison, scalar IN/NOT IN list or BETWEEN/NOT BETWEEN bounds; use explicit byte functions for other forms",
                    ));
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
            types[index] = <str as sqlx_core::types::Type<crate::Doris>>::type_info();
        }
        if args.null_bitmap[index / 8] & (1 << (index % 8)) != 0 {
            continue;
        }
        let before = input.0;
        if blob {
            use super::super::codec::LenEnc;
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
    args: &DorisArguments,
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
                        return Err(invalid(
                            "BLOB BETWEEN cannot infer numeric, TIME or other non-string boundary coercion",
                        ));
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
                    ));
                }
            }
        }
    }
    Ok(())
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
fn argument_texts(args: &DorisArguments) -> Result<Vec<Option<&str>>, Error> {
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
    text.parse::<crate::types::Time>()
        .map(|_| ())
        .map_err(|_| invalid("TIME string exceeds Doris duration syntax or range"))
}

// Adapt the whole list together. Leaving even one ordinary string in its raw
// representation would change membership, and filtering NULLs would break NOT
// IN's three-valued logic. Keep the original operator, order and NULL entries.
fn adapt_memberships(
    sql: &str,
    model: &Syntax,
    args: &DorisArguments,
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
        for (j, index) in indices.iter().enumerate().take(close).skip(open + 1) {
            if let Some(k) = index.and_then(|p| parameter_kind(args.types[p].r#type)) {
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
        // Keep the same truth value for the unparenthesized form.
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
                Scalar::Opaque => {
                    return Err(invalid(
                        "membership supports only scalar parameters, single-quoted strings and NULL",
                    ));
                }
            }
        }
    }
    Ok(handled)
}

/// Positions for checked client binding; opaque server SQL is not reprinted.
pub(crate) fn sites(sql: &str) -> Result<Vec<usize>, Error> {
    syntax::sites(sql)
}

/// Route common native transaction VALUES to the execution engine before send.
/// A public session variable makes the parsed InlineTable non-constant, while
/// IF(x, value, value) preserves its value and per-row target-type conversion.
/// This avoids both the FE's literal-only batch engine and UNION type coercion.
/// Apply to text AND bound VALUES from the first write; never switch after ERR.
pub(crate) fn route_values(sql: &str, no_backslash: bool) -> Result<Option<String>, Error> {
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
                Some(c @ (b')' | b']' | b'}')) if stack.pop() != Some(c) => {
                    return Err(invalid("unbalanced VALUES expression"));
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
fn compile(sql: &str, args: &DorisArguments) -> Result<String, Error> {
    compile_mode(sql, args, false)
}
pub(crate) fn compile_mode(
    sql: &str,
    args: &DorisArguments,
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
                    ));
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
pub(crate) struct ModeBindings {
    normal: String,
    no_backslash: Result<String, Error>,
}

impl ModeBindings {
    pub(crate) fn independent(&self) -> bool {
        self.no_backslash
            .as_ref()
            .is_ok_and(|value| *value == self.normal)
    }

    pub(crate) fn select(self, no_backslash: bool) -> Result<String, Error> {
        if no_backslash {
            self.no_backslash
        } else {
            Ok(self.normal)
        }
    }
}

pub(crate) fn compile_modes(sql: &str, args: &DorisArguments) -> Result<ModeBindings, Error> {
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

fn read_time(input: &mut Input<'_>) -> Result<crate::types::Time, Error> {
    let len = usize::from(input.take(1)?[0]);
    if len == 0 {
        return Ok(crate::types::Time::from_microseconds(0).unwrap());
    }
    if !matches!(len, 8 | 12) {
        return Err(invalid("unsupported TIME encoding length"));
    }
    let b = input.take(len)?;
    if b[0] > 1 || b[5] > 23 || b[6] > 59 || b[7] > 59 {
        return Err(invalid("invalid TIME fields"));
    }
    let days = u32::from_le_bytes(b[1..5].try_into().unwrap());
    if days > 34 {
        return Err(invalid("TIME exceeds Doris duration range"));
    }
    let micros = if len == 12 {
        u32::from_le_bytes(b[8..12].try_into().unwrap())
    } else {
        0
    };
    if micros > 999999 {
        return Err(invalid("invalid TIME microseconds"));
    }
    let n = (((i64::from(days) * 24 + i64::from(b[5])) * 60 + i64::from(b[6])) * 60
        + i64::from(b[7]))
        * 1_000_000
        + i64::from(micros);
    if n == 0 && b[0] == 1 {
        return Err(invalid("negative zero TIME"));
    }
    crate::types::Time::from_microseconds(if b[0] == 1 { -n } else { n })
        .map_err(|_| invalid("TIME exceeds Doris duration range"))
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
pub(crate) fn dml_parameters(sql: &str) -> Result<usize, Error> {
    let positions = sites(sql)?;
    if !matches!(
        Syntax::parse(sql)?.kind,
        StatementKind::Insert | StatementKind::Update | StatementKind::Delete
    ) {
        return Err(invalid(
            "explicit transaction prepare supports INSERT/UPDATE/DELETE only",
        ));
    }
    Ok(positions.len())
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod grammar_tests;
