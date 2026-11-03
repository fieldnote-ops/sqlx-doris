//! Lexical handling of trusted Doris migration files.
use sqlx_core::Error;

#[derive(Debug, Clone)]
pub(crate) struct Token {
    pub text: String,
    pub quoted: bool,
}
#[derive(Debug)]
pub(crate) struct Step {
    #[cfg_attr(not(feature = "migrate"), allow(dead_code))]
    pub sql: String,
    pub tokens: Vec<Token>,
}
fn invalid(message: &str) -> Error {
    Error::Protocol(format!("Doris migration: {message}"))
}
pub(crate) fn split(sql: &str, no_backslash: bool) -> Result<Vec<Step>, Error> {
    let b = sql.as_bytes();
    let mut i = 0;
    let mut start = 0;
    let mut depth = 0;
    let mut tokens = Vec::new();
    let mut out = Vec::new();
    while i < b.len() {
        match b[i] {
            b if b.is_ascii_whitespace() => i += 1,
            b'#' => {
                while i < b.len() && b[i] != b'\n' {
                    i += 1;
                }
            }
            b'-' if b.get(i + 1) == Some(&b'-') => {
                while i < b.len() && b[i] != b'\n' {
                    i += 1;
                }
            }
            b'/' if b.get(i + 1) == Some(&b'*') => {
                if matches!(b.get(i + 2), Some(b'!' | b'+')) {
                    return Err(invalid(
                        "executable comments and hints are not supported in migration scripts",
                    ));
                }
                i += 2;
                let mut closed = false;
                while i + 1 < b.len() {
                    if &b[i..i + 2] == b"/*" {
                        return Err(invalid("nested block comment"));
                    }
                    if &b[i..i + 2] == b"*/" {
                        i += 2;
                        closed = true;
                        break;
                    }
                    i += 1;
                }
                if !closed {
                    return Err(invalid("unterminated block comment"));
                }
            }
            q @ (b'\'' | b'"' | b'`') => {
                i += 1;
                let mut name = Vec::new();
                let mut closed = false;
                while i < b.len() {
                    if b[i] == q {
                        if b.get(i + 1) == Some(&q) {
                            name.push(q);
                            i += 2;
                            continue;
                        }
                        i += 1;
                        closed = true;
                        break;
                    }
                    if b[i] == b'\\' && !no_backslash && q != b'`' {
                        i += 1;
                        if i == b.len() {
                            break;
                        }
                    }
                    name.push(b[i]);
                    i += 1;
                }
                if !closed {
                    return Err(invalid("unterminated quoted value"));
                }
                tokens.push(Token {
                    text: if q == b'`' {
                        String::from_utf8(name).map_err(|_| invalid("invalid identifier"))?
                    } else {
                        String::new()
                    },
                    quoted: true,
                });
            }
            b';' => {
                if depth != 0 {
                    return Err(invalid("semicolon inside an unfinished expression"));
                }
                if !tokens.is_empty() {
                    out.push(Step {
                        sql: sql[start..i].trim().into(),
                        tokens: std::mem::take(&mut tokens),
                    });
                }
                i += 1;
                start = i;
            }
            c @ (b'(' | b')' | b'.' | b',') => {
                if c == b'(' {
                    depth += 1;
                } else if c == b')' {
                    if depth == 0 {
                        return Err(invalid("unmatched parenthesis"));
                    }
                    depth -= 1;
                }
                tokens.push(Token {
                    text: char::from(c).to_string(),
                    quoted: false,
                });
                i += 1;
            }
            _ => {
                let a = i;
                i += 1;
                while i < b.len() && (b[i].is_ascii_alphanumeric() || b[i] == b'_' || b[i] >= 128) {
                    i += 1;
                }
                tokens.push(Token {
                    text: sql[a..i].into(),
                    quoted: false,
                });
            }
        }
    }
    if depth != 0 {
        return Err(invalid("unfinished parenthesis"));
    }
    if !tokens.is_empty() {
        out.push(Step {
            sql: sql[start..].trim().into(),
            tokens,
        });
    }
    for step in &out {
        let first = &step.tokens[0];
        if first.quoted
            || ![
                "CREATE", "ALTER", "DROP", "TRUNCATE", "INSERT", "UPDATE", "DELETE", "SELECT",
                "WITH", "SET", "COMMENT",
            ]
            .iter()
            .any(|s| first.text.eq_ignore_ascii_case(s))
        {
            return Err(invalid(
                "unsupported statement; transaction control and asynchronous loads are not accepted",
            ));
        }
        if step.keyword(0, "CREATE") && step.keyword(1, "MATERIALIZED") {
            return Err(invalid(
                "CREATE MATERIALIZED VIEW needs an explicit task-completion workflow",
            ));
        }
        if ["CREATE", "ALTER", "DROP"]
            .iter()
            .any(|k| step.keyword(0, k))
        {
            let ordinary_object = ["TABLE", "VIEW", "INDEX"]
                .iter()
                .any(|k| step.keyword(1, k));
            let replace_view = step.keyword(0, "CREATE")
                && step.keyword(1, "OR")
                && step.keyword(2, "REPLACE")
                && step.keyword(3, "VIEW");
            if !ordinary_object && !replace_view {
                return Err(invalid(
                    "only table, view and index DDL is supported in migration files",
                ));
            }
        }
        if step.keyword(0, "SET") {
            return Err(invalid(
                "SET belongs in connection initialization, not a migration file",
            ));
        }
    }
    Ok(out)
}
impl Step {
    pub(crate) fn keyword(&self, n: usize, s: &str) -> bool {
        self.tokens
            .get(n)
            .is_some_and(|t| !t.quoted && t.text.eq_ignore_ascii_case(s))
    }
    #[cfg(feature = "migrate")]
    pub(crate) fn table_after(&self, start: usize) -> Result<Vec<String>, Error> {
        let mut n = start;
        let mut names = Vec::new();
        loop {
            let t = self
                .tokens
                .get(n)
                .ok_or_else(|| invalid("missing table identifier"))?;
            if t.text.is_empty()
                || (!t.quoted
                    && !t
                        .text
                        .bytes()
                        .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b >= 128))
            {
                return Err(invalid("invalid table identifier"));
            }
            names.push(t.text.clone());
            n += 1;
            if self.tokens.get(n).is_none_or(|t| t.text != ".") {
                break;
            }
            n += 1;
        }
        if names.len() > 2 {
            return Err(invalid(
                "migration DDL must target the current Doris database, without an external catalog",
            ));
        }
        Ok(names)
    }
}
#[cfg(feature = "migrate")]
pub(crate) fn quote(name: &str) -> Result<String, Error> {
    if name.is_empty() || name.contains('\0') {
        return Err(invalid("empty identifier or NUL"));
    }
    Ok(format!("`{}`", name.replace('`', "``")))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    #[cfg(feature = "migrate")]
    fn strings_comments_quoted_names_and_semicolons_stay_in_their_statements() {
        let steps = split(
            "-- no-transaction\nCREATE TABLE `a;b`(id INT); /* ; */ INSERT INTO `a;b` VALUES(1,'a;''b'); # ;\nSELECT ';';",
            false
        )
        .unwrap();
        assert_eq!(steps.len(), 3);
        assert_eq!(steps[0].table_after(2).unwrap(), ["a;b"]);
        assert_eq!(split("SELECT 'a\\\';b'; SELECT 1", false).unwrap().len(), 2);
        assert!(split("SELECT 'a\\\';b'; SELECT 1", true).is_err());
    }
    #[test]
    fn invalid_scripts_fail_before_any_statement_can_run() {
        for s in [
            "CREATE TABLE a(id INT); BEGIN",
            "SELECT 'x",
            "SELECT (1;",
            "SELECT 1 /*",
            "/*! DROP TABLE a */",
            "SELECT (1))",
            "SET group_commit=async_mode",
            "CREATE JOB background_job ON SCHEDULE EVERY 1 SECOND DO SELECT 1",
            "CREATE OR REPLACE MATERIALIZED VIEW mv AS SELECT 1",
        ] {
            assert!(split(s, false).is_err(), "{s}");
        }
        assert!(split("-- comment only", false).unwrap().is_empty());
    }
}
