//! Deterministic grammar generation and mutation, without database prerequisites.
use super::*;
use crate::types::{MySqlTime, MySqlTimeSign};
fn time() -> MySqlTime {
    MySqlTime::new(MySqlTimeSign::Negative, 0, 0, 0, 1).unwrap()
}
fn args(blob: bool, null: bool) -> MySqlArguments {
    let mut a = MySqlArguments::default();
    if blob {
        a.add((!null).then_some(vec![255_u8, 0, 128, 39, 92, 63]))
            .unwrap();
    } else {
        a.add((!null).then_some(time())).unwrap();
    }
    a
}
fn unchanged(a: &MySqlArguments, b: &MySqlArguments) {
    assert_eq!(a.values, b.values);
    assert_eq!(a.types, b.types);
    assert_eq!(&*a.null_bitmap, &*b.null_bitmap);
}

#[test]
fn pagination_grammar_binds_bare_integer_tokens_without_touching_other_sites() {
    let mut count = 0;
    for gap in [" ", "\n", " /* LIMIT ? ; 中 */ "] {
        for keyword in ["LIMIT", "limit"] {
            for (body, expected, n) in [
                ("?", "7", 1),
                ("? OFFSET ?", "7 OFFSET 8", 2),
                ("?,?", "7,8", 2),
                ("3 OFFSET ?", "3 OFFSET 7", 1),
                ("? OFFSET 3", "7 OFFSET 3", 1),
            ] {
                for nested in [false, true] {
                    let inner =
                        format!("SELECT ? AS `LIMIT?` FROM t WHERE id=? {keyword}{gap}{body}");
                    let sql = if nested {
                        format!("SELECT * FROM ({inner}) a LIMIT ?")
                    } else {
                        inner
                    };
                    let mut a = MySqlArguments::default();
                    a.add("quote' ? \\中").unwrap();
                    a.add(42_i64).unwrap();
                    for i in 0..n {
                        a.add(7_i64 + i).unwrap();
                    }
                    if nested {
                        a.add(2_i64).unwrap();
                    }
                    let before = a.clone();
                    assert!(has_bound_pagination(&sql).unwrap());
                    assert_eq!(
                        pagination_sites(&sql).unwrap().len(),
                        n as usize + usize::from(nested)
                    );
                    for mode in [false, true] {
                        let bound = compile_mode(&sql, &a, mode).unwrap();
                        assert!(
                            bound.contains(&format!("{keyword}{gap}{expected}")),
                            "{bound}"
                        );
                        assert!(bound.contains("id=(42)"));
                        if nested {
                            assert!(bound.ends_with("LIMIT 2"));
                        }
                        assert!(scan(&bound, !mode, true).unwrap().sites.is_empty());
                    }
                    unchanged(&a, &before);
                    count += 1;
                }
            }
        }
    }
    assert_eq!(count, 60);
}

#[test]
fn pagination_integer_widths_signedness_and_null_bitmap_are_checked() {
    let mut a = MySqlArguments::default();
    a.add(0_i8).unwrap();
    a.add(255_u8).unwrap();
    a.add(32767_i16).unwrap();
    a.add(65535_u16).unwrap();
    a.add(i32::MAX).unwrap();
    a.add(u32::MAX).unwrap();
    a.add(i64::MAX).unwrap();
    a.add(i64::MAX as u64).unwrap();
    let sql="SELECT * FROM (SELECT * FROM (SELECT * FROM (SELECT 1 LIMIT ? OFFSET ?) a LIMIT ?,?) b LIMIT ?,?) c LIMIT ?,?";
    let bound = compile(sql, &a).unwrap();
    for expected in [
        "LIMIT 0 OFFSET 255",
        "LIMIT 32767,65535",
        "LIMIT 2147483647,4294967295",
        "LIMIT 9223372036854775807,9223372036854775807",
    ] {
        assert!(bound.contains(expected), "{bound}");
    }
    let mut a = MySqlArguments::default();
    for i in 0..17 {
        a.add((i % 3 != 0).then_some(format!("v'{i} ?"))).unwrap();
    }
    a.add(9_u64).unwrap();
    a.add(1_i32).unwrap();
    let sql = format!("SELECT {} LIMIT ? OFFSET ?", vec!["?"; 17].join(","));
    let before = a.clone();
    let bound = compile(&sql, &a).unwrap();
    assert_eq!(bound.matches("(NULL)").count(), 6);
    assert!(bound.ends_with("LIMIT 9 OFFSET 1"));
    unchanged(&a, &before);
}

#[test]
fn invalid_pagination_values_and_truncated_encodings_do_not_mutate_arguments() {
    let mut variants = Vec::new();
    macro_rules! bad {
        ($v:expr) => {{
            let mut a = MySqlArguments::default();
            a.add($v).unwrap();
            variants.push(a);
        }};
    }
    bad!(-1_i64);
    bad!(u64::MAX);
    bad!(None::<i64>);
    bad!("1");
    bad!("1; DELETE FROM t");
    bad!(1.0_f64);
    bad!(vec![1_u8]);
    for a in variants {
        let before = a.clone();
        assert!(compile("SELECT 1 LIMIT ?", &a).is_err());
        unchanged(&a, &before);
    }
    let mut a = MySqlArguments::default();
    a.add(1_i64).unwrap();
    a.values.pop();
    assert!(compile("SELECT 1 LIMIT ?", &a).is_err());
    let mut a = MySqlArguments::default();
    a.add(1_i64).unwrap();
    a.values.push(0);
    assert!(compile("SELECT 1 LIMIT ?", &a).is_err());
}

#[test]
fn pagination_selection_does_not_capture_literals_identifiers_or_writes() {
    for sql in [
        "SELECT '? LIMIT ?' WHERE id=?",
        "SELECT t.`limit` FROM t WHERE id=? LIMIT 3",
        "UPDATE t SET n=? WHERE id=? LIMIT ?",
        "DELETE FROM t WHERE id=? LIMIT ?",
        "SELECT 1 LIMIT (?)",
        "SELECT `LIMIT?` FROM t WHERE id=?",
    ] {
        assert!(!has_bound_pagination(sql).unwrap(), "{sql}");
    }
    for sql in [
        "SELECT 1 LIMIT ?+1",
        "SELECT 1 LIMIT ? OFFSET",
        "SELECT 1 LIMIT ? OFFSET (?)",
        "SELECT 1 LIMIT ? FOR UPDATE",
        "SELECT 1 LIMIT ?,?,?",
    ] {
        assert!(has_bound_pagination(sql).is_err(), "{sql}");
    }
}

#[test]
fn direct_byte_null_tests_preserve_nullness_without_byte_conversion_expressions() {
    let mut count = 0;
    for value in [None, Some(Vec::new()), Some(vec![255, 0, 128, 39, 92])] {
        for not in ["", "NOT "] {
            for depth in 0..3 {
                for gap in [" ", "\n", " /* ? */ "] {
                    let p = format!("{}?{}", "(".repeat(depth), ")".repeat(depth));
                    for select in [true, false] {
                        let sql = if select {
                            format!("SELECT {p}{gap}IS {not}NULL AS ok")
                        } else {
                            format!("SELECT 1 FROM t WHERE ({p}{gap}IS {not}NULL OR b=?)")
                        };
                        let mut a = MySqlArguments::default();
                        a.add(value.clone()).unwrap();
                        if !select {
                            a.add(value.clone()).unwrap();
                        }
                        let nulls = a.null_bitmap.clone();
                        let adapted = adapt_parameters(&sql, &mut a).unwrap().unwrap();
                        assert!(!adapted.contains("UNHEX"), "{adapted}");
                        assert_eq!(&*nulls, &*a.null_bitmap);
                        assert!(a.types.iter().all(|v| v.r#type == ColumnType::VarString));
                        for mode in [false, true] {
                            assert!(compile_mode(&adapted, &a, mode).is_ok());
                        }
                        count += 1;
                    }
                }
            }
        }
    }
    assert_eq!(count, 108);
    for sql in [
        "SELECT 1 FROM t WHERE (?+1) IS NULL",
        "SELECT 1 FROM t WHERE LENGTH(?) IS NULL",
        "SELECT 1 FROM t WHERE ? IS NULL=0",
    ] {
        let mut a = args(true, false);
        let before = a.clone();
        assert!(adapt_parameters(sql, &mut a).is_err(), "{sql}");
        unchanged(&a, &before);
    }
}

#[test]
fn generated_comparison_grammar_preserves_operators_markers_and_values() {
    let mut count = 0;
    for blob in [true, false] {
        for null in [false, true] {
            for op in ["=", "<>", "!=", "<=>", "<", "<=", ">", ">="] {
                for column in ["b", "a.`b?`", "`中`.`from`"] {
                    for depth in 0..3 {
                        for gap in [" ", "\t", " /* ? ; 中 */ "] {
                            for reverse in [false, true] {
                                let column =
                                    format!("{}{}{}", "(".repeat(depth), column, ")".repeat(depth));
                                let parameter =
                                    format!("{}?{}", "(".repeat(depth), ")".repeat(depth));
                                let expected_column = if blob {
                                    format!("HEX({column})")
                                } else {
                                    format!("CAST({column} AS TIME(6))")
                                };
                                let (left, right, wleft, wright) = if reverse {
                                    (&parameter, &column, &parameter, &expected_column)
                                } else {
                                    (&column, &parameter, &expected_column, &parameter)
                                };
                                let sql=format!("SELECT '?' FROM t a WHERE NOT ({left}{gap}{op}{gap}{right}) /* end ? */");
                                let expected=format!("SELECT '?' FROM t a WHERE NOT ({wleft}{gap}{op}{gap}{wright}) /* end ? */");
                                let mut a = args(blob, null);
                                let before = a.clone();
                                let q = adapt_parameters(&sql, &mut a)
                                    .unwrap_or_else(|e| panic!("case {count}: {sql}: {e}"))
                                    .unwrap();
                                assert_eq!(q, expected, "case {count}");
                                assert_eq!(sites(&q).unwrap().len(), 1);
                                assert_eq!(&*a.null_bitmap, &*before.null_bitmap);
                                let mut wanted = MySqlArguments::default();
                                if blob {
                                    wanted.add((!null).then_some("FF0080275C3F")).unwrap();
                                } else {
                                    wanted.add((!null).then_some(time())).unwrap();
                                }
                                unchanged(&a, &wanted);
                                for mode in [false, true] {
                                    let bound = compile_mode(&q, &a, mode).unwrap();
                                    assert!(scan(&bound, !mode, true).unwrap().sites.is_empty());
                                }
                                count += 1;
                            }
                        }
                    }
                }
            }
        }
    }
    assert_eq!(count, 1728);
    eprintln!("generated comparison grammar: {count} cases");
}
#[test]
fn generated_membership_grammar_keeps_order_nulls_and_mixed_string_candidates() {
    let mut count = 0;
    for blob in [true, false] {
        for not in ["", "NOT "] {
            for size in [1, 2, 8, 9, 17, 65] {
                for column in ["b", "a.`b?`"] {
                    let mut a = MySqlArguments::default();
                    let mut wanted = MySqlArguments::default();
                    let mut candidates = Vec::new();
                    for i in 0..size {
                        let null = i % 3 == 0;
                        if blob {
                            a.add((!null).then_some(vec![255_u8, i as u8])).unwrap();
                            wanted.add((!null).then_some(format!("FF{i:02X}"))).unwrap();
                        } else {
                            a.add((!null).then_some(time())).unwrap();
                            wanted.add((!null).then_some(time())).unwrap();
                        }
                        candidates.push(if i % 2 == 0 { "(?)" } else { "?" });
                    }
                    let text = if blob { "A?中" } else { "25:00:00" };
                    a.add(text).unwrap();
                    wanted.add(text).unwrap();
                    let sql = format!(
                        "SELECT {column} {not}IN({},NULL,?,'{text}') FROM t a",
                        candidates.join(",/* ? */")
                    );
                    let q = adapt_parameters(&sql, &mut a).unwrap().unwrap();
                    unchanged(&a, &wanted);
                    assert_eq!(sites(&q).unwrap().len(), size + 1);
                    assert!(q.contains(&format!("{not}IN(")));
                    assert!(q.contains("NULL"));
                    let syntax = Syntax::parse(&q).unwrap();
                    assert_eq!(syntax.parameters.len(), size + 1);
                    assert_eq!(syntax.memberships[0].candidates.len(), size + 3);
                    for mode in [false, true] {
                        assert!(scan(&compile_mode(&q, &a, mode).unwrap(), !mode, true)
                            .unwrap()
                            .sites
                            .is_empty());
                    }
                    count += 1;
                }
            }
        }
    }
    assert_eq!(count, 48);
    eprintln!("generated membership grammar: {count} cases");
}
#[test]
fn generated_statement_scopes_separate_assignments_comparisons_and_subqueries() {
    for gap in [" ", "\n", " /* ? */ "] {
        for column in ["b", "`where`", "a.b"] {
            let sql=format!("UPDATE t a SET {column}{gap}={gap}?,n=(SELECT COUNT(*) FROM t c WHERE c.b=?),t=? WHERE {column}<>?");
            let model = Syntax::parse(&sql).unwrap();
            assert_eq!(model.kind, StatementKind::Update);
            assert_eq!(model.parameters.len(), 4);
            assert!(matches!(
                model.parameters[0].expression,
                Expression::Assignment
            ));
            assert!(matches!(
                model.parameters[1].expression,
                Expression::Comparison {
                    column: Some(_),
                    ..
                }
            ));
            assert!(matches!(
                model.parameters[2].expression,
                Expression::Assignment
            ));
            assert!(matches!(
                model.parameters[3].expression,
                Expression::Comparison {
                    op: Comparison::NotEqual,
                    ..
                }
            ));
            let mut a = MySqlArguments::default();
            a.add(vec![255_u8]).unwrap();
            a.add(vec![128_u8]).unwrap();
            a.add(time()).unwrap();
            a.add(vec![0_u8]).unwrap();
            let q = adapt_parameters(&sql, &mut a).unwrap().unwrap();
            assert_eq!(q.matches("UNHEX(CONCAT").count(), 1);
            assert_eq!(q.matches("HEX(").count(), 3);
            assert_eq!(a.types[2].r#type, ColumnType::Time);
        }
    }
    for head in [
        "SELECT b=? FROM t",
        "DELETE FROM t WHERE b=?",
        "INSERT INTO t SELECT ?",
    ] {
        assert_eq!(Syntax::parse(head).unwrap().parameters.len(), 1);
    }
}
#[test]
fn generated_invalid_mutations_are_atomic_and_do_not_guess_byte_semantics() {
    let base = "SELECT id FROM t WHERE b=?";
    let mut rejected = 0;
    for op in [
        "<<",
        "<==",
        ">>",
        ">==",
        "==",
        "!==",
        "<==>",
        "= =",
        "< /* ? */ =>",
        "! =",
    ] {
        let sql = base.replacen("=", op, 1);
        let mut a = args(true, false);
        let before = a.clone();
        assert!(adapt_parameters(&sql, &mut a).is_err(), "{sql}");
        unchanged(&a, &before);
        rejected += 1;
    }
    for operand in ["b+1", "CONCAT(b,'x')", "(b,b)", "b COLLATE utf8mb4_bin"] {
        let sql = base.replace("b=?", &format!("{operand}=?"));
        let mut a = args(true, false);
        let before = a.clone();
        assert!(adapt_parameters(&sql, &mut a).is_err(), "{sql}");
        unchanged(&a, &before);
        rejected += 1;
    }
    for suffix in [
        "; DELETE FROM t",
        " /*",
        " '",
        " )",
        " /*!80000 ? */",
        " /*+ X */",
        "\0",
        "--?",
        "--x",
        "-- comment\\\n?",
        " = x",
        " + 1",
    ] {
        let sql = format!("{base}{suffix}");
        let mut a = args(true, false);
        let before = a.clone();
        assert!(adapt_parameters(&sql, &mut a).is_err(), "{sql:?}");
        unchanged(&a, &before);
        rejected += 1;
    }
    for list in [
        "?+1", "?,7", "?,b", "?,ABS(1)", "(?,1)", "?,", ",?", "SELECT ?",
    ] {
        let sql = format!("SELECT id FROM t WHERE b IN({list})");
        let mut a = args(true, false);
        let before = a.clone();
        assert!(adapt_parameters(&sql, &mut a).is_err(), "{sql}");
        unchanged(&a, &before);
        rejected += 1;
    }
    assert_eq!(rejected, 34);
    eprintln!("semantic/lexical invalid mutations: {rejected}");
}

#[test]
fn nested_comparison_values_are_opaque_and_never_partially_reencoded() {
    for op in ["=", "<>", "!=", "<=>", "<", "<=", ">", ">="] {
        for expression in ["(?+1)", "ABS(?)", "ABS(?+1)", "ABS(ABS(?))"] {
            for reverse in [false, true] {
                let condition = if reverse {
                    format!("{expression}{op}b")
                } else {
                    format!("b{op}{expression}")
                };
                for null in [false, true] {
                    let sql = format!("SELECT {condition} FROM t");
                    let mut a = args(true, null);
                    let before = a.clone();
                    assert!(adapt_parameters(&sql, &mut a).is_err(), "{sql}");
                    unchanged(&a, &before);
                    // The new byte adapter never changes ordinary MySQL SQL
                    // or parameters, including the server's coercion choices.
                    assert!(adapt_mysql_time_memberships(&sql, &a).unwrap().is_none());
                    unchanged(&a, &before);
                }
            }
        }
    }
    // An unrelated numeric comparison must not turn a standalone value into a
    // byte predicate; keep existing VALUES/function and explicit HEX behavior.
    for sql in ["SELECT ABS(?),b>1 FROM t", "SELECT HEX(?)<HEX(b) FROM t"] {
        let mut a = args(true, false);
        assert!(adapt_parameters(sql, &mut a).is_ok(), "{sql}");
    }
}
#[test]
fn seeded_utf8_insert_delete_and_substitute_mutations_never_panic_or_corrupt_arguments() {
    let seeds = [
        "SELECT id FROM t WHERE b=?",
        "UPDATE t SET b=? WHERE id=1",
        "SELECT b IN(?) FROM t",
        "SELECT `中?` FROM t WHERE ((b))=((?))",
    ];
    let fragments = [
        "(", ")", "?", "'", "`", "/*", "*/", "--x", ";", "中", "😀", " ", "\n", "=", "!", "\0",
    ];
    let (mut accepted, mut rejected) = (0, 0);
    for seed in seeds {
        let boundaries: Vec<_> = seed
            .char_indices()
            .map(|(i, _)| i)
            .chain([seed.len()])
            .collect();
        for (k, &position) in boundaries.iter().enumerate() {
            for fragment in fragments {
                for delete in [false, true] {
                    let end = if delete {
                        *boundaries.get(k + 1).unwrap_or(&position)
                    } else {
                        position
                    };
                    let sql = format!("{}{}{}", &seed[..position], fragment, &seed[end..]);
                    let mut a = args(true, false);
                    let before = a.clone();
                    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        adapt_parameters(&sql, &mut a)
                    }));
                    let result=result.unwrap_or_else(|_|panic!("mutation panicked: seed={seed:?},position={position},delete={delete},fragment={fragment:?},sql={sql:?}"));
                    match result {
                        Ok(Some(q)) => {
                            assert_eq!(sites(&q).unwrap().len(), 1);
                            assert_eq!(
                                Syntax::parse(&q).unwrap().kind,
                                Syntax::parse(&sql).unwrap().kind
                            );
                            assert_eq!(&*a.null_bitmap, &*before.null_bitmap);
                            let mut expected = MySqlArguments::default();
                            expected.add("FF0080275C3F").unwrap();
                            unchanged(&a, &expected);
                            for mode in [false, true] {
                                assert!(scan(&compile_mode(&q, &a, mode).unwrap(), !mode, true)
                                    .unwrap()
                                    .sites
                                    .is_empty());
                            }
                            accepted += 1;
                        }
                        Err(_) => {
                            unchanged(&a, &before);
                            rejected += 1;
                        }
                        Ok(None) => panic!("byte argument bypassed adaptation"),
                    }
                }
            }
        }
    }
    assert!(accepted > 100 && rejected > 100);
    eprintln!(
        "UTF-8 mutation corpus: accepted={accepted}, rejected={rejected}, total={}",
        accepted + rejected
    );
}
#[test]
fn edit_plan_rejects_overlaps_utf8_splits_marker_changes_and_statement_changes() {
    let sql = "SELECT 中 FROM t WHERE b=?";
    let parsed = Syntax::parse(sql).unwrap();
    let site = *parsed.scan.sites.first().unwrap();
    let unicode = sql.find('中').unwrap();
    for edits in [
        vec![(site, site + 1, "?,?".into())],
        vec![(site, site + 1, "NULL".into())],
        vec![(unicode + 1, unicode + 2, "x".into())],
        vec![(site, site + 1, "?".into()), (site, site + 1, "?".into())],
        vec![(0, 6, "DELETE".into())],
    ] {
        let mut plan = EditPlan::default();
        for edit in edits {
            plan.push(edit);
        }
        assert!(plan.render(sql, &parsed).is_err());
    }
}
#[test]
fn generated_values_routing_keeps_one_statement_for_typed_batches() {
    let mut count = 0;
    for mode in [false, true] {
        for rows in 1..=8 {
            for width in [2, 5, 9] {
                let mut a = MySqlArguments::default();
                let mut tuples = Vec::new();
                for row in 0..rows {
                    let mut fields = Vec::new();
                    for col in 0..width {
                        fields.push("?");
                        match (row + col) % 4 {
                            0 => a.add(vec![255_u8, 0, 128]).unwrap(),
                            1 => a.add("'\\\0?; -- 中").unwrap(),
                            2 => a.add(time()).unwrap(),
                            _ => a.add(None::<MySqlTime>).unwrap(),
                        }
                    }
                    tuples.push(format!("({})", fields.join(",")));
                }
                let sql = format!("INSERT INTO t VALUES{}", tuples.join(","));
                let bound = compile_mode(&sql, &a, mode).unwrap();
                let routed = route_values(&bound, mode).unwrap().unwrap();
                assert!(scan(&routed, !mode, true).unwrap().sites.is_empty());
                assert_eq!(routed.matches("IF(@@sql_mode IS NULL,").count(), 1);
                assert!(!routed.contains("UNION"));
                count += 1;
            }
        }
    }
    assert_eq!(count, 48);
    eprintln!("typed VALUES grammar: {count} batches");
}
#[test]
fn common_comment_boundaries_are_explicit_and_literals_remain_data() {
    for bad in [
        "SELECT ?--x",
        "SELECT ?--?",
        "SELECT ?--",
        "SELECT ? -- x\\\n+1",
        "SELECT ? # x\\\r+1",
    ] {
        assert!(sites(bad).is_err(), "{bad:?}");
    }
    for good in [
        "SELECT ? -- ?\n",
        "SELECT ? /* --x */",
        "SELECT '--x',?",
        "SELECT ? -- x\r\n",
        "SELECT `--?`,?",
    ] {
        assert_eq!(sites(good).unwrap().len(), 1);
    }
}

#[test]
fn mysql_time_membership_casts_the_whole_domain_without_argument_changes() {
    let mut a = MySqlArguments::default();
    a.add(MySqlTime::ZERO).unwrap();
    a.add("-00:00:00.000001").unwrap();
    a.add(None::<String>).unwrap();
    let before = a.clone();
    let sql = "SELECT t IN(?,(?),NULL,?,'25:00:00') FROM t";
    let q = adapt_mysql_time_memberships(sql, &a).unwrap().unwrap();
    assert!(q.starts_with("SELECT REPLACE(CAST(CAST(t AS TIME(6)) AS CHAR)"));
    assert_eq!(q.matches("REPLACE(").count(), 5);
    assert!(q.contains("TRIM(LEADING '+' FROM (?))"));
    unchanged(&a, &before);
    let mut doris = a.clone();
    assert_eq!(adapt_parameters(sql, &mut doris).unwrap().unwrap(), q);
    unchanged(&doris, &before);
}
#[test]
fn time_membership_cache_shape_is_value_independent_and_keeps_null_bits() {
    let mut expected = None;
    for v in [MySqlTime::ZERO, time(), MySqlTime::MAX, MySqlTime::MIN] {
        let mut a = MySqlArguments::default();
        a.add(v).unwrap();
        a.add(None::<i64>).unwrap();
        a.add("00:00:00").unwrap();
        let before = a.clone();
        let q = adapt_mysql_time_memberships("DELETE FROM t WHERE t NOT IN(?,?,?)", &a)
            .unwrap()
            .unwrap();
        assert_eq!(q.matches("CAST(").count(), 8);
        if let Some(expected) = &expected {
            assert_eq!(&q, expected);
        } else {
            expected = Some(q);
        }
        unchanged(&a, &before);
    }
}
#[test]
fn invalid_time_strings_literals_and_domains_fail_atomically() {
    for text in [
        "",
        "bad",
        "00:00:00.1234567",
        "838:59:59.000001",
        "839:00:00",
        "00:60:00",
        "00:00:00\0",
        "0' OR 1=1 --",
        " 00:00:00",
        "00:00:00:00",
    ] {
        let mut a = MySqlArguments::default();
        a.add(MySqlTime::ZERO).unwrap();
        a.add(text).unwrap();
        let before = a.clone();
        assert!(
            adapt_mysql_time_memberships("UPDATE t SET n=n+1 WHERE t IN(?,?)", &a).is_err(),
            "{text:?}"
        );
        assert!(
            adapt_parameters("UPDATE t SET n=n+1 WHERE t IN(?,?)", &mut a).is_err(),
            "{text:?}"
        );
        unchanged(&a, &before);
    }
    for literal in [
        "'bad'",
        "'00:00:00.1234567'",
        "'00:00:00''x'",
        "'838:59:59.000001'",
        "7",
        "t",
        "ABS(1)",
    ] {
        let mut a = args(false, false);
        let before = a.clone();
        let sql = format!("SELECT t IN(?,{literal}) FROM t");
        assert!(adapt_mysql_time_memberships(&sql, &a).is_err(), "{sql}");
        assert!(adapt_parameters(&sql, &mut a).is_err(), "{sql}");
        unchanged(&a, &before);
    }
}
#[test]
fn ordinary_mysql_queries_and_string_or_byte_membership_are_unchanged() {
    for sql in [
        "SELECT ?",
        "SELECT id FROM t WHERE t=?",
        "CALL test(?)",
        "WITH c AS(SELECT ?) SELECT * FROM c",
        "SELECT CAST(t AS TIME(6)) IN(CAST(? AS TIME(6))) FROM t",
        "SELECT t IN(CAST(? AS TIME(6))) FROM t",
    ] {
        let a = args(false, false);
        assert!(adapt_mysql_time_memberships(sql, &a).unwrap().is_none());
    }
    let mut a = MySqlArguments::default();
    a.add("00:00:00").unwrap();
    assert!(adapt_mysql_time_memberships("SELECT t IN(?) FROM t", &a)
        .unwrap()
        .is_none());
    let mut a = MySqlArguments::default();
    a.add(MySqlTime::ZERO).unwrap();
    a.add(vec![255_u8]).unwrap();
    let before = a.clone();
    assert!(
        adapt_mysql_time_memberships("SELECT ? FROM t WHERE b IN(?)", &a)
            .unwrap()
            .is_none()
    );
    unchanged(&a, &before);
}
#[test]
fn large_time_lists_preserve_wire_types_values_and_cross_byte_null_bitmaps() {
    for count in [1, 8, 9, 17, 65] {
        let mut a = MySqlArguments::default();
        for i in 0..count {
            match i % 3 {
                0 => a.add(Some(MySqlTime::ZERO)).unwrap(),
                1 => a.add(None::<String>).unwrap(),
                _ => a.add("25:00:00").unwrap(),
            }
        }
        let before = a.clone();
        let sql = format!("SELECT t NOT IN({}) FROM t", vec!["?"; count].join(","));
        let q = adapt_mysql_time_memberships(&sql, &a).unwrap().unwrap();
        assert_eq!(q.matches("CAST(").count(), 2 * (count + 1));
        assert_eq!(sites(&q).unwrap().len(), count);
        unchanged(&a, &before);
    }
}
#[test]
fn generated_common_time_strings_and_membership_operators_share_one_plan() {
    let mut count = 0;
    for text in [
        "0:00:00",
        "00:00:00.000000",
        "+00:00:00",
        "-00:00:00",
        "-00:00:00.000001",
        "25:00:00",
        "-123:45:06.123456",
        "838:59:59",
    ] {
        for op in ["IN", "NOT IN"] {
            for column in ["t", "r.`t`", "((r.t))"] {
                let mut a = MySqlArguments::default();
                a.add(MySqlTime::ZERO).unwrap();
                a.add(text).unwrap();
                let sql = format!("SELECT {column} {op}(?,?,'{text}',NULL) FROM t r");
                let before = a.clone();
                let q = adapt_mysql_time_memberships(&sql, &a).unwrap().unwrap();
                let mut d = a.clone();
                assert_eq!(adapt_parameters(&sql, &mut d).unwrap().unwrap(), q);
                unchanged(&d, &before);
                for mode in [false, true] {
                    assert!(scan(&compile_mode(&q, &d, mode).unwrap(), !mode, true)
                        .unwrap()
                        .sites
                        .is_empty());
                }
                count += 1;
            }
        }
    }
    assert_eq!(count, 48);
    eprintln!("normalized time IN/NOT IN grammar: {count} cases");
}

#[test]
fn generated_byte_between_preserves_bounds_markers_and_null_bits() {
    let mut count = 0;
    for column in ["b", "r.`b?`", "((r.b))"] {
        for op in ["BETWEEN", "NOT BETWEEN"] {
            for gap in [" ", " /* ? AND ; 中 */ ", "\n"] {
                for lower_null in [false, true] {
                    for upper_null in [false, true] {
                        let sql = format!(
                            "SELECT ({column}{gap}{op}{gap}((?)){gap}AND{gap}(?)) IS NULL FROM t r"
                        );
                        let expected=format!("SELECT (HEX({column}){gap}{op}{gap}((?)){gap}AND{gap}(?)) IS NULL FROM t r");
                        let mut a = MySqlArguments::default();
                        a.add((!lower_null).then_some(vec![255_u8, 0])).unwrap();
                        a.add((!upper_null).then_some(vec![255_u8, 32])).unwrap();
                        let original = a.clone();
                        assert!(adapt_mysql_time_memberships(&sql, &a).unwrap().is_none());
                        unchanged(&a, &original);
                        assert_eq!(adapt_parameters(&sql, &mut a).unwrap().unwrap(), expected);
                        let mut wanted = MySqlArguments::default();
                        wanted.add((!lower_null).then_some("FF00")).unwrap();
                        wanted.add((!upper_null).then_some("FF20")).unwrap();
                        unchanged(&a, &wanted);
                        for mode in [false, true] {
                            assert!(
                                scan(&compile_mode(&expected, &a, mode).unwrap(), !mode, true)
                                    .unwrap()
                                    .sites
                                    .is_empty()
                            );
                        }
                        count += 1;
                    }
                }
            }
        }
    }
    assert_eq!(count, 72);
    eprintln!("byte BETWEEN grammar: {count} cases");
}
#[test]
fn byte_between_mixed_boundaries_and_unrelated_parameters_keep_their_domains() {
    for text_null in [false, true] {
        let mut a = MySqlArguments::default();
        a.add(vec![255_u8, 0]).unwrap();
        a.add((!text_null).then_some("中?")).unwrap();
        a.add(-7_i64).unwrap();
        assert_eq!(
            adapt_parameters("SELECT b BETWEEN ? AND (?) FROM t WHERE id=?", &mut a)
                .unwrap()
                .unwrap(),
            "SELECT HEX(b) BETWEEN ? AND HEX((?)) FROM t WHERE id=?"
        );
        let mut wanted = MySqlArguments::default();
        wanted.add("FF00").unwrap();
        wanted.add((!text_null).then_some("中?")).unwrap();
        wanted.add(-7_i64).unwrap();
        unchanged(&a, &wanted);
    }
    for (low, high, expected) in [
        ("?", "'A'", "? AND HEX('A')"),
        ("'A'", "?", "HEX('A') AND ?"),
        ("?", "NULL", "? AND NULL"),
        ("NULL", "?", "NULL AND ?"),
    ] {
        let mut a = args(true, false);
        let sql = format!("SELECT b NOT BETWEEN {low} AND {high} FROM t");
        assert_eq!(
            adapt_parameters(&sql, &mut a).unwrap().unwrap(),
            format!("SELECT HEX(b) NOT BETWEEN {expected} FROM t")
        );
    }
    let mut a = args(true, false);
    a.add(None::<i64>).unwrap();
    assert_eq!(
        adapt_parameters("SELECT b BETWEEN ? AND ? FROM t", &mut a)
            .unwrap()
            .unwrap(),
        "SELECT HEX(b) BETWEEN ? AND ? FROM t"
    );
    for sql in [
        "SELECT ? FROM t WHERE id BETWEEN 0 AND 2",
        "SELECT ?,(id NOT BETWEEN 0 AND 2) FROM t",
        "SELECT HEX(b) BETWEEN HEX(?) AND 'FF' FROM t",
    ] {
        let mut a = args(true, false);
        assert!(adapt_parameters(sql, &mut a).is_ok(), "{sql}");
    }
    let mut a = MySqlArguments::default();
    a.add("A").unwrap();
    a.add("a").unwrap();
    let before = a.clone();
    assert!(adapt_parameters("SELECT b BETWEEN ? AND ? FROM t", &mut a)
        .unwrap()
        .is_none());
    unchanged(&a, &before);
}
#[test]
fn byte_between_opaque_missing_or_mixed_domains_are_rejected_atomically() {
    for sql in [
        "SELECT b BETWEEN ?+1 AND ? FROM t",
        "SELECT b BETWEEN ? AND ABS(?) FROM t",
        "SELECT CONCAT(b,'x') BETWEEN ? AND ? FROM t",
        "SELECT b+1 BETWEEN ? AND ? FROM t",
        "SELECT b BETWEEN ? AND ?=id FROM t",
        "SELECT b BETWEEN ? AND ? COLLATE utf8mb4_bin FROM t",
        "SELECT b BETWEEN (? AND 1) AND ? FROM t",
        "SELECT b BETWEEN (SELECT ?) AND ? FROM t",
        "SELECT b BETWEEN ? AND CASE WHEN id=1 THEN ? END FROM t",
        "SELECT b BETWEEN ? AND ? IS FALSE FROM t",
        "SELECT CASE WHEN b BETWEEN ? AND ? THEN 1 END FROM t",
        "SELECT ? BETWEEN b AND ? FROM t",
        "SELECT CONCAT(?,'x') BETWEEN 'a' AND ? FROM t",
        "SELECT ?+1 BETWEEN 'a' AND ? FROM t",
        "SELECT b BETWEEN ? AND (? IN(1,2)) FROM t",
    ] {
        let mut a = args(true, false);
        a.add(vec![255_u8]).unwrap();
        let before = a.clone();
        assert!(adapt_parameters(sql, &mut a).is_err(), "{sql}");
        unchanged(&a, &before);
    }
    for sql in [
        "SELECT b BETWEEN ? FROM t",
        "SELECT b BETWEEN ? AND FROM t",
        "SELECT b BETWEEN AND ? FROM t",
        "SELECT b BETWEEN ? OR id=1 FROM t",
    ] {
        let mut a = args(true, false);
        let before = a.clone();
        assert!(adapt_parameters(sql, &mut a).is_err(), "{sql}");
        unchanged(&a, &before);
    }
    for null in [false, true] {
        let mut a = args(true, false);
        a.add((!null).then_some(time())).unwrap();
        let before = a.clone();
        assert!(adapt_parameters("SELECT b BETWEEN ? AND ? FROM t", &mut a).is_err());
        unchanged(&a, &before);
    }
    let mut a = args(true, false);
    a.add(7_i64).unwrap();
    let before = a.clone();
    assert!(adapt_parameters("SELECT b BETWEEN ? AND ? FROM t", &mut a).is_err());
    unchanged(&a, &before);
}
#[test]
fn multiple_byte_between_scopes_keep_assignments_and_large_null_bitmaps() {
    for count in [1, 8, 9, 17, 65] {
        let mut a = MySqlArguments::default();
        let mut want = MySqlArguments::default();
        let mut predicates = Vec::new();
        let mut rewritten = Vec::new();
        for i in 0..count {
            a.add((i % 3 != 0).then_some(vec![255_u8, i as u8]))
                .unwrap();
            want.add((i % 3 != 0).then_some(format!("FF{i:02X}")))
                .unwrap();
            a.add((i % 2 != 0).then_some("A")).unwrap();
            want.add((i % 2 != 0).then_some("A")).unwrap();
            predicates.push("b BETWEEN ? AND ?");
            rewritten.push("HEX(b) BETWEEN ? AND HEX(?)");
        }
        let sql = format!("SELECT id FROM t WHERE {}", predicates.join(" OR "));
        let expected = format!("SELECT id FROM t WHERE {}", rewritten.join(" OR "));
        assert_eq!(adapt_parameters(&sql, &mut a).unwrap().unwrap(), expected);
        unchanged(&a, &want);
    }
    let mut a = args(true, false);
    a.add(vec![0_u8]).unwrap();
    a.add(vec![255_u8]).unwrap();
    a.add(vec![255_u8]).unwrap();
    assert_eq!(adapt_parameters("UPDATE t SET b=? WHERE b BETWEEN ? AND ? AND b IN(?)",&mut a).unwrap().unwrap(),"UPDATE t SET b=UNHEX(CONCAT(?,SUBSTRING(UUID(),1,0))) WHERE HEX(b) BETWEEN ? AND ? AND HEX(b) IN(?)");
}

#[test]
fn between_utf8_keyword_parenthesis_and_separator_mutations_are_atomic() {
    let mut corpus = std::collections::BTreeSet::new();
    for seed in [
        "SELECT b BETWEEN ? AND ? FROM t",
        "UPDATE t SET n=1 WHERE (`中`.`b?`) NOT BETWEEN (?) AND ((?))",
    ] {
        let positions: Vec<_> = (0..=seed.len())
            .filter(|&i| seed.is_char_boundary(i))
            .collect();
        for (at, &i) in positions.iter().enumerate() {
            for part in [
                "", "(", ")", ",", "?", "AND", "OR", "BETWEEN", "NOT", "'", "`", "/*", "*/", "--x",
                "中", "\0",
            ] {
                corpus.insert(format!("{}{}{}", &seed[..i], part, &seed[i..]));
                if let Some(&end) = positions.get(at + 1) {
                    corpus.insert(format!("{}{}{}", &seed[..i], part, &seed[end..]));
                }
            }
        }
    }
    let (mut accepted, mut rejected) = (0, 0);
    for sql in &corpus {
        let mut a = args(true, false);
        a.add(vec![255_u8, 32]).unwrap();
        let before = a.clone();
        match adapt_parameters(sql, &mut a) {
            Err(_) => {
                unchanged(&a, &before);
                rejected += 1;
            }
            Ok(Some(out)) => {
                assert_eq!(sites(&out).unwrap().len(), 2, "{sql}");
                let mut want = MySqlArguments::default();
                want.add("FF0080275C3F").unwrap();
                want.add("FF20").unwrap();
                unchanged(&a, &want);
                for mode in [false, true] {
                    let bound = compile_mode(&out, &a, mode).unwrap();
                    assert!(scan(&bound, !mode, true).unwrap().sites.is_empty(), "{sql}");
                }
                accepted += 1;
            }
            Ok(None) => panic!("byte arguments must be adapted: {sql}"),
        }
    }
    assert!(accepted > 100 && rejected > 100);
    eprintln!(
        "BETWEEN UTF-8 mutations: accepted={accepted}, rejected={rejected}, total={}",
        corpus.len()
    );
}

#[test]
fn between_null_test_suffix_wraps_the_predicate_without_reordering_bounds() {
    for op in ["BETWEEN", "NOT BETWEEN"] {
        for suffix in ["IS NULL", "IS NOT NULL"] {
            let mut a = args(true, false);
            a.add(None::<String>).unwrap();
            let sql = format!("SELECT b {op} ? AND ? {suffix} FROM t");
            let expected = format!("SELECT (HEX(b) {op} ? AND HEX(?)) {suffix} FROM t");
            assert_eq!(adapt_parameters(&sql, &mut a).unwrap().unwrap(), expected);
            assert_eq!(sites(&expected).unwrap().len(), 2);
            assert!(compile(&expected, &a).is_ok());
        }
    }
}

#[test]
fn nonempty_all_default_values_keep_server_owned_slots_and_row_widths() {
    let mut count = 0;
    for width in [1, 2, 6, 17] {
        for rows in [1, 2, 9] {
            for gap in [" ", "\n", " /* ? VALUES */ "] {
                for listed in [false, true] {
                    let header = if listed {
                        format!(
                            "INSERT INTO t({})",
                            (0..width)
                                .map(|i| format!("c{i}"))
                                .collect::<Vec<_>>()
                                .join(",")
                        )
                    } else {
                        "INSERT INTO t".into()
                    };
                    let row = format!("({})", vec!["DEFAULT"; width].join(","));
                    let sql = format!("{header}{gap}VALUES{}; -- end", vec![row; rows].join(","));
                    for mode in [false, true] {
                        assert_eq!(route_values(&sql, mode).unwrap().unwrap(), sql);
                    }
                    count += 1;
                }
            }
        }
    }
    assert_eq!(count, 72);
    eprintln!("all-DEFAULT VALUES grammar: {count} nonempty batches");
}
#[test]
fn insert_value_alias_only_changes_the_checked_body_token() {
    let mut count = 0;
    for table in ["t", "db.`value`", "`VALUE`.`values`"] {
        for columns in ["", "(id,`default`)"] {
            for word in ["VALUE", "value", "VaLuE"] {
                for gap in [" ", " /* ? VALUE */ ", "\n"] {
                    let sql=format!("/* VALUE */ INSERT INTO {table}{columns}{gap}{word}(?,DEFAULT),(?,'VALUE(?)'); -- VALUE");
                    let expected=format!("/* VALUE */ INSERT INTO {table}{columns}{gap}VALUES(?,DEFAULT),(?,'VALUE(?)'); -- VALUE");
                    assert_eq!(normalize_insert_value(&sql).unwrap().unwrap(), expected);
                    assert_eq!(sites(&expected).unwrap().len(), 2);
                    count += 1;
                }
            }
        }
    }
    assert_eq!(count, 54);
    eprintln!("VALUE alias grammar: {count} templates");
    for sql in [
        "SELECT 'VALUE(?)'",
        "INSERT INTO t VALUES(?)",
        "INSERT INTO `value` SELECT ?",
        "INSERT INTO t WITH LABEL value (id) VALUES(?)",
        "INSERT INTO t PARTITION(p) VALUES(?)",
        "INSERT INTO t(a,b) SELECT ?,?",
        "UPDATE t SET value=?",
        "INSERT INTO t VALUE(?,?) --x",
    ] {
        assert!(normalize_insert_value(sql).unwrap().is_none(), "{sql}");
    }
}
#[test]
fn alias_parameter_adaptation_preserves_native_arguments_and_typed_bytes() {
    let mut a = MySqlArguments::default();
    a.add(7_i64).unwrap();
    a.add("DEFAULT?'\\\0😀").unwrap();
    let before = a.clone();
    assert_eq!(
        adapt_query("INSERT INTO t(id,title) VALUE(?,?)", Some(&mut a))
            .unwrap()
            .unwrap(),
        "INSERT INTO t(id,title) VALUES(?,?)"
    );
    unchanged(&a, &before);
    assert!(
        adapt_mysql_time_memberships("INSERT INTO t(id,title) VALUE(?,?)", &a)
            .unwrap()
            .is_none()
    );
    unchanged(&a, &before);
    let mut a = args(true, false);
    a.add(9_i64).unwrap();
    let sql = "INSERT INTO t(payload,id) VALUE(?,?)";
    let q = adapt_query(sql, Some(&mut a)).unwrap().unwrap();
    assert_eq!(
        q,
        "INSERT INTO t(payload,id) VALUES(UNHEX(CONCAT(?,SUBSTRING(UUID(),1,0))),?)"
    );
    for mode in [false, true] {
        let bound = compile_mode(&q, &a, mode).unwrap();
        let routed = route_values(&bound, mode).unwrap().unwrap();
        assert!(!routed.contains("VALUE("));
        assert!(scan(&routed, !mode, true).unwrap().sites.is_empty());
    }
    assert_eq!(
        adapt_query("INSERT INTO t VALUE(DEFAULT)", None)
            .unwrap()
            .unwrap(),
        "INSERT INTO t VALUES(DEFAULT)"
    );
}
#[test]
fn default_and_literal_words_are_not_interchangeable_or_schema_expanded() {
    for sql in [
        "INSERT INTO t(a,b) VALUES(DEFAULT,'DEFAULT')",
        "INSERT INTO t(a,b) VALUES(NULL,DEFAULT)",
        "INSERT INTO t(a,b) VALUES(DEFAULT,1),(2,DEFAULT)",
        "INSERT INTO t(a,b) VALUES(DEFAULT,CONCAT('a,b','x'))",
    ] {
        let routed = route_values(sql, false).unwrap().unwrap();
        assert!(routed.contains("IF(@@sql_mode IS NULL,"));
        assert!(routed.contains("DEFAULT"));
        assert!(!routed.contains("UNION"));
    }
    for sql in [
        "INSERT INTO t VALUES()",
        "INSERT INTO t VALUES(DEFAULT,)",
        "INSERT INTO t VALUES(DEFAULT),(DEFAULT,DEFAULT)",
        "INSERT INTO t VALUES(DEFAULT);DELETE FROM t",
    ] {
        assert!(route_values(sql, false).is_err(), "{sql}");
    }
}
