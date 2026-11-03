//! Conservative projection aliases, retaining the existing lexical policy.
use super::migration::{Step, split};
use sqlx_core::Error;

#[cfg(feature = "macro-support")]
pub(crate) fn projection_names(sql: &str) -> Result<Option<Vec<Option<String>>>, Error> {
    let steps = split(sql, false)?;
    if steps.len() != 1 {
        return Ok(None);
    }
    let step = &steps[0];
    let mut depth = 0;
    let mut begin = None;
    for (i, t) in step.tokens.iter().enumerate() {
        if !t.quoted && matches!(t.text.as_str(), "(" | "[" | "{") {
            depth += 1;
        } else if !t.quoted && matches!(t.text.as_str(), ")" | "]" | "}") {
            depth -= 1;
        }
        if depth == 0 && step.keyword(i, "SELECT") {
            begin = Some(i + 1);
            break;
        }
    }
    let Some(mut start) = begin else {
        return Ok(None);
    };
    let mut names = Vec::new();
    let mut finish = step.tokens.len();
    depth = 0;
    let projection_start = start;
    for i in projection_start..step.tokens.len() {
        let t = &step.tokens[i];
        if !t.quoted && matches!(t.text.as_str(), "(" | "[" | "{") {
            depth += 1;
        } else if !t.quoted && matches!(t.text.as_str(), ")" | "]" | "}") {
            depth -= 1;
        }
        if depth == 0
            && [
                "FROM", "WHERE", "UNION", "LIMIT", "ORDER", "GROUP", "HAVING", "QUALIFY",
            ]
            .iter()
            .any(|k| step.keyword(i, k))
        {
            finish = i;
            break;
        }
        if depth == 0 && !t.quoted && t.text == "," {
            names.push(alias(step, start, i));
            start = i + 1;
        }
    }
    names.push(alias(step, start, finish));
    Ok(Some(names))
}

#[cfg(feature = "macro-support")]
fn alias(step: &Step, start: usize, end: usize) -> Option<String> {
    let mut depth = 0;
    for i in start..end {
        let t = &step.tokens[i];
        if !t.quoted && matches!(t.text.as_str(), "(" | "[" | "{") {
            depth += 1;
        } else if !t.quoted && matches!(t.text.as_str(), ")" | "]" | "}") {
            depth -= 1;
        }
        if depth == 0 && step.keyword(i, "AS") && i + 2 == end {
            return Some(step.tokens[i + 1].text.clone()).filter(|s| !s.is_empty());
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn projection_aliases_ignore_nested_queries_and_casts() {
        let names = projection_names("WITH c AS (SELECT 1 AS x) SELECT CAST(x AS BIGINT) AS `value!: i64`, ARRAY(1,2) AS `items: Array<i32>`, x FROM c").unwrap().unwrap();
        assert_eq!(
            names,
            [
                Some("value!: i64".into()),
                Some("items: Array<i32>".into()),
                None
            ]
        );
        assert_eq!(projection_names("INSERT INTO t VALUES(1)").unwrap(), None);
        assert_eq!(projection_names("SELECT 1; SELECT 2").unwrap(), None);
        assert_eq!(
            projection_names("SELECT `(` AS `value!: i64`, `,` AS `other!: String` FROM t")
                .unwrap(),
            Some(vec![
                Some("value!: i64".into()),
                Some("other!: String".into())
            ])
        );
    }
}
