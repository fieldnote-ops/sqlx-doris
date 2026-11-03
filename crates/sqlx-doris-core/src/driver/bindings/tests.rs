//! Binding regression contracts.

use super::*;
use crate::DorisTypeInfo;

fn raw(ty: ColumnType, bytes: Vec<u8>) -> DorisArguments {
    let mut a = DorisArguments::default();
    a.add(0_i64).unwrap();
    a.types[0] = DorisTypeInfo::binary(ty);
    a.values = bytes;
    a
}
fn text(ty: ColumnType, value: &str) -> DorisArguments {
    let mut a = DorisArguments::default();
    a.add(value).unwrap();
    a.types[0] = DorisTypeInfo::binary(ty);
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
        let mut a = DorisArguments::default();
        a.add(payload.as_str()).unwrap();
        assert_eq!(
            compile_mode("INSERT INTO t VALUES(?)", &a, mode).unwrap(),
            format!("INSERT INTO t VALUES(({literal}))")
        );
    }
}
#[test]
fn mode_independent_bindings_include_exact_time_null_numbers_and_plain_text() {
    use crate::types::{WireTime, WireTimeSign};
    let mut a = DorisArguments::default();
    a.add(i64::MIN).unwrap();
    a.add(u64::MAX).unwrap();
    a.add(WireTime::new(WireTimeSign::Negative, 27, 2, 3, 123456).unwrap())
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
        let mut a = DorisArguments::default();
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
    let mut a = DorisArguments::default();
    a.add("ordinary").unwrap();
    assert!(compile_modes("SELECT ?", &a).unwrap().independent());
    assert!(compile_modes("SELECT ?,?", &a).is_err());
    assert!(compile_modes("SELECT 'x\\' ?, ?", &a).is_err());
}
#[test]
fn empty_and_null_text_and_binary_are_distinct() {
    let mut a = DorisArguments::default();
    a.add("").unwrap();
    a.add(None::<String>).unwrap();
    assert_eq!(
        compile("INSERT INTO t VALUES(?,?)", &a).unwrap(),
        "INSERT INTO t VALUES((''),(NULL))"
    );
    let mut a = DorisArguments::default();
    a.add(Vec::<u8>::new()).unwrap();
    a.add(None::<Vec<u8>>).unwrap();
    assert_eq!(
        compile("INSERT INTO t VALUES(?,?)", &a).unwrap(),
        "INSERT INTO t VALUES((''),(NULL))"
    );
}
#[test]
fn signed_unsigned_boolean_and_adjacency() {
    let mut a = DorisArguments::default();
    a.add(i8::MIN).unwrap();
    a.add(i16::MIN).unwrap();
    a.add(i32::MIN).unwrap();
    a.add(i64::MIN).unwrap();
    a.add(u64::MAX).unwrap();
    a.add(true).unwrap();
    assert_eq!(
        compile("INSERT INTO t VALUES(?,?,?,?,?,?)", &a).unwrap(),
        "INSERT INTO t VALUES((-128),(-32768),(-2147483648),(-9223372036854775808),(18446744073709551615),(1))"
    );
    let mut a = DorisArguments::default();
    a.add(-1_i64).unwrap();
    a.add(-2_i64).unwrap();
    assert_eq!(compile("SELECT ?-?", &a).unwrap(), "SELECT (-1)-(-2)");
}
#[test]
fn floats_preserve_negative_zero_and_reject_non_finite() {
    let mut a = DorisArguments::default();
    a.add(-0.0_f64).unwrap();
    a.add(-0.0_f32).unwrap();
    a.add(f32::MAX).unwrap();
    let sql = compile("INSERT INTO t VALUES(?,?,?)", &a).unwrap();
    // Doris parses numeric -0.0 as positive zero; string casts retain its sign.
    assert!(sql.contains("CAST('-0.0' AS DOUBLE)"));
    assert!(sql.contains("CAST('-0.0' AS FLOAT)"));
    for v in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
        let mut a = DorisArguments::default();
        assert!(a.add(v).is_err());
        let a = raw(ColumnType::Double, v.to_le_bytes().to_vec());
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
    assert!(
        compile(
            "INSERT INTO t VALUES(?)",
            &raw(ColumnType::Date, vec![4, 0xe9, 7, 2, 29])
        )
        .is_err()
    );
    assert!(
        compile(
            "INSERT INTO t VALUES(?)",
            &raw(ColumnType::Date, vec![4, 0, 0, 1, 1])
        )
        .is_ok()
    );
    assert!(
        compile(
            "INSERT INTO t VALUES(?)",
            &raw(ColumnType::Datetime, vec![7, 0xea, 7, 10, 27, 24, 0, 0])
        )
        .is_err()
    );
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
    assert!(
        compile(
            "INSERT INTO t VALUES(?)",
            &raw(ColumnType::LongLong, vec![0; 7])
        )
        .is_err()
    );
    assert!(compile("INSERT INTO t VALUES(?)", &raw(ColumnType::Bit, vec![0])).is_err());
    assert!(
        compile(
            "INSERT INTO t VALUES(?)",
            &raw(ColumnType::VarString, vec![1, 255])
        )
        .is_err()
    );
}
#[test]
fn mismatched_count_is_preflight_error() {
    let a = DorisArguments::default();
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
            let mut a = DorisArguments::default();
            a.add(payload.as_bytes()).unwrap();
            a.types[0] = DorisTypeInfo::binary(ty);
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
    let mut a = DorisArguments::default();
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
        let mut a = DorisArguments::default();
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
    use crate::types::{WireTime, WireTimeSign};
    for value in [
        WireTime::ZERO,
        WireTime::MIN,
        WireTime::MAX,
        WireTime::new(WireTimeSign::Negative, 0, 0, 0, 1).unwrap(),
        WireTime::new(WireTimeSign::Positive, 25, 2, 3, 123456).unwrap(),
        WireTime::new(WireTimeSign::Negative, 838, 59, 58, 999999).unwrap(),
    ] {
        let mut a = DorisArguments::default();
        a.add(value).unwrap();
        assert_eq!(
            compile("INSERT INTO t VALUES(?)", &a).unwrap(),
            format!("INSERT INTO t VALUES((CAST('{value:.6}' AS TIME(6))))")
        );
    }
    let mut a = DorisArguments::default();
    a.add(None::<WireTime>).unwrap();
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
    let sql = "/* VALUES */ INSERT INTO `t VALUES` PARTITION(p) WITH LABEL l (`a`,`b`) VALUES(/*x*/concat('a,b',')'),[1,2]), (42,'x;?') ; -- trailing";
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
            let mut a = DorisArguments::default();
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
fn signed_time_fixture() -> crate::types::WireTime {
    crate::types::WireTime::new(crate::types::WireTimeSign::Negative, 0, 0, 0, 1).unwrap()
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
        let mut a = DorisArguments::default();
        a.add(vec![0_u8, 255, 128]).unwrap();
        assert_eq!(adapt_parameters(sql, &mut a).unwrap().unwrap(), want);
        let mut expected = DorisArguments::default();
        expected.add("00FF80").unwrap();
        assert_eq!(a.values, expected.values);
        assert_eq!(a.types, expected.types);
        assert_eq!(sites(want).unwrap().len(), 1);
    }
}

#[test]
fn blob_set_assignments_remain_values_while_where_becomes_byte_comparison() {
    let mut a = DorisArguments::default();
    a.add(vec![255_u8, 0]).unwrap();
    a.add(7_i64).unwrap();
    a.add(vec![0_u8, 128]).unwrap();
    let sql = "UPDATE t SET b=?,n=? WHERE t.b=?";
    assert_eq!(
        adapt_parameters(sql, &mut a).unwrap().unwrap(),
        "UPDATE t SET b=UNHEX(CONCAT(?,SUBSTRING(UUID(),1,0))),n=? WHERE HEX(t.b)=?"
    );
    let mut want = DorisArguments::default();
    want.add("FF00").unwrap();
    want.add(7_i64).unwrap();
    want.add("0080").unwrap();
    assert_eq!(a.values, want.values);
    assert_eq!(a.types, want.types);
    let mut nested = DorisArguments::default();
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
        adapt_parameters("SELECT 1", &mut DorisArguments::default()).unwrap(),
        None
    );
}

#[test]
fn blob_compound_conditions_subquery_scopes_and_existing_hex_work() {
    let mut a = DorisArguments::default();
    a.add(vec![0_u8, 255]).unwrap();
    a.add(vec![128_u8]).unwrap();
    assert_eq!(
        adapt_parameters("SELECT a.id FROM t a JOIN t c ON a.b=? WHERE c.b=?", &mut a)
            .unwrap()
            .unwrap(),
        "SELECT a.id FROM t a JOIN t c ON HEX(a.b)=? WHERE HEX(c.b)=?"
    );
    let mut a = DorisArguments::default();
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
    let mut a = DorisArguments::default();
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
        let mut a = DorisArguments::default();
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
        let mut want = DorisArguments::default();
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
    let mut a = DorisArguments::default();
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
    let mut want = DorisArguments::default();
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
        let mut a = DorisArguments::default();
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
    let mut a = DorisArguments::default();
    a.add(vec![255_u8]).unwrap();
    assert!(adapt_parameters("SELECT 1", &mut a).is_err());
}

#[test]
fn binary_rewrite_treats_payload_and_comment_question_marks_as_data() {
    let mut value: Vec<u8> = (0..=255).collect();
    value.extend_from_slice(b"';DROP TABLE t;--?");
    let mut a = DorisArguments::default();
    a.add(value.clone()).unwrap();
    let q = adapt_parameters("SELECT '?' FROM `t?` WHERE `b?`= ? /* ? */", &mut a)
        .unwrap()
        .unwrap();
    assert_eq!(q, "SELECT '?' FROM `t?` WHERE HEX(`b?`)= ? /* ? */");
    assert!(!q.contains("DROP"));
    let mut expected = DorisArguments::default();
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
        let mut args = DorisArguments::default();
        args.add(vec![255_u8, 0]).unwrap();
        args.add(Vec::<u8>::new()).unwrap();
        assert_eq!(adapt_parameters(sql, &mut args).unwrap().unwrap(), want);
        let mut expected = DorisArguments::default();
        expected.add("FF00").unwrap();
        expected.add("").unwrap();
        assert_eq!(args.values, expected.values);
        assert_eq!(args.types, expected.types);
        assert_eq!(sites(want).unwrap().len(), 2);
    }
}

#[test]
fn blob_membership_hexes_mixed_strings_but_never_filters_nulls() {
    let mut args = DorisArguments::default();
    args.add(None::<Vec<u8>>).unwrap();
    args.add("a").unwrap();
    args.add(vec![255_u8]).unwrap();
    args.add(None::<i64>).unwrap();
    let mut expected = DorisArguments::default();
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
    let mut args = DorisArguments::default();
    for _ in 0..5 {
        args.add(vec![255_u8]).unwrap();
    }
    let sql =
        "UPDATE t SET b=? WHERE id IN(SELECT id FROM t WHERE b IN(?,?)) AND (b=? OR b NOT IN(?))";
    assert_eq!(
        adapt_parameters(sql, &mut args).unwrap().unwrap(),
        "UPDATE t SET b=UNHEX(CONCAT(?,SUBSTRING(UUID(),1,0))) WHERE id IN(SELECT id FROM t WHERE HEX(b) IN(?,?)) AND (HEX(b)=? OR HEX(b) NOT IN(?))"
    );
    let mut args = DorisArguments::default();
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
        let mut args = DorisArguments::default();
        args.add(vec![255_u8]).unwrap();
        let before = args.clone();
        assert!(adapt_parameters(sql, &mut args).is_err(), "{sql}");
        assert_eq!(args.values, before.values);
        assert_eq!(args.types, before.types);
    }
    let mut args = DorisArguments::default();
    args.add(vec![255_u8]).unwrap();
    args.add(1_i64).unwrap();
    assert!(adapt_parameters("SELECT id FROM t WHERE b IN(?,?)", &mut args).is_err());
}

#[test]
fn blob_membership_many_parameters_keep_all_null_bitmap_bits_and_order() {
    let mut args = DorisArguments::default();
    let mut expected = DorisArguments::default();
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
        let mut args = DorisArguments::default();
        args.add(b"\xff');DROP TABLE t;--?".as_slice()).unwrap();
        args.types[0].r#type = kind;
        assert_eq!(
            adapt_parameters("SELECT '?' FROM `t?` WHERE b IN(?,'?') /* ? */", &mut args)
                .unwrap()
                .unwrap(),
            "SELECT '?' FROM `t?` WHERE HEX(b) IN(?,HEX('?')) /* ? */"
        );
        let q = compile_mode("SELECT b FROM t WHERE HEX(b) IN(?,HEX('?'))", &args, true).unwrap();
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
            let mut args = DorisArguments::default();
            args.add(vec![255_u8, 0, 128]).unwrap();
            assert_eq!(adapt_parameters(&sql, &mut args).unwrap().unwrap(), want);
            let mut expected = DorisArguments::default();
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
        let mut args = DorisArguments::default();
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
        let mut expected = DorisArguments::default();
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
    let mut args = DorisArguments::default();
    args.add(vec![255_u8]).unwrap();
    args.add(7_i64).unwrap();
    for _ in 0..4 {
        args.add(vec![128_u8]).unwrap();
    }
    let sql = "UPDATE t SET b=?,n=? WHERE b<>? AND ?!=b AND (b<=>? OR b IN(?))";
    assert_eq!(
        adapt_parameters(sql, &mut args).unwrap().unwrap(),
        "UPDATE t SET b=UNHEX(CONCAT(?,SUBSTRING(UUID(),1,0))),n=? WHERE HEX(b)<>? AND ?!=HEX(b) AND (HEX(b)<=>? OR HEX(b) IN(?))"
    );
    let mut expected = DorisArguments::default();
    expected.add("FF").unwrap();
    expected.add(7_i64).unwrap();
    for _ in 0..4 {
        expected.add("80").unwrap();
    }
    assert_eq!(args.values, expected.values);
    assert_eq!(args.types, expected.types);
    let mut args = DorisArguments::default();
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
        let mut args = DorisArguments::default();
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
    let mut args = DorisArguments::default();
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
    let mut args = DorisArguments::default();
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
    let mut args = DorisArguments::default();
    let mut expected = DorisArguments::default();
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
        let mut a = DorisArguments::default();
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
        assert!(
            compile(&q, &a)
                .unwrap()
                .contains("CAST('-00:00:00.000001' AS TIME(6))")
        );
    }
}
#[test]
fn time_membership_null_and_mixed_parameter_domains_are_checked() {
    let mut a = DorisArguments::default();
    a.add(signed_time_fixture()).unwrap();
    a.add(None::<crate::types::WireTime>).unwrap();
    let q = adapt_parameters("SELECT t IN(?,NULL,?,'25:00:00') FROM t", &mut a)
        .unwrap()
        .unwrap();
    assert!(q.starts_with("SELECT REPLACE(CAST(CAST(t AS TIME(6)) AS CHAR)"));
    assert_eq!(q.matches("REPLACE(").count(), 4);
    assert_eq!(sites(&q).unwrap().len(), 2);
    assert!(compile(&q, &a).unwrap().contains("(NULL)"));
    let mut a = DorisArguments::default();
    a.add(signed_time_fixture()).unwrap();
    a.add(vec![255_u8]).unwrap();
    let bytes = a.values.clone();
    assert!(adapt_parameters("SELECT t IN(?,?) FROM t", &mut a).is_err());
    assert_eq!(a.values, bytes);
}
