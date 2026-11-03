//! Seeded generators exercise independent wire encoders and malformed inputs.
//! No runtime dependency, network, persistent corpus or generated repository files.
use super::*;
use codec::{LenEnc, put_len};
use sqlx_core::row::Row;

struct Generator(u64);
impl Generator {
    fn new() -> Self {
        let seed = std::env::var("DORIS_RANDOM_SEED")
            .map_or(Ok(0xa9d6_417b_028c_e35fu64), |v| v.parse())
            .expect("DORIS_RANDOM_SEED must be u64");
        eprintln!("randomized driver seed={seed}");
        Self(seed)
    }
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut x = self.0;
        x = (x ^ (x >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        x = (x ^ (x >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        x ^ (x >> 31)
    }
    fn bytes(&mut self, n: usize) -> Vec<u8> {
        (0..n).map(|_| self.next() as u8).collect()
    }
}
fn cases() -> usize {
    let n = std::env::var("DORIS_RANDOM_CASES")
        .map_or(Ok(4096), |v| v.parse())
        .expect("DORIS_RANDOM_CASES must be integer");
    assert!((64..=100_000).contains(&n));
    n
}
fn column(index: usize, kind: ColumnType) -> DorisColumn {
    DorisColumn {
        ordinal: index,
        name: format!("c{index}"),
        info: DorisTypeInfo::binary(kind),
        origin: sqlx_core::column::ColumnOrigin::Expression,
        source: None,
    }
}
#[test]
fn randomized_lengths_round_trip_and_reject_every_truncated_encoding() {
    let mut g = Generator::new();
    for n in [0, 250, 251, 65535, 65536, 16777215, 16777216, u64::MAX]
        .into_iter()
        .chain((0..cases()).map(|_| g.next()))
    {
        let mut encoded = Vec::new();
        put_len(&mut encoded, n);
        let mut c = Cursor(&encoded);
        assert_eq!(c.len().unwrap(), n);
        assert!(c.0.is_empty());
        for cut in 0..encoded.len() {
            assert!(
                Cursor(&encoded[..cut]).len().is_err(),
                "length {n}, cut {cut}"
            );
        }
    }
    for marker in [251, 255] {
        assert!(Cursor(&[marker]).len().is_err());
    }
}
#[test]
fn randomized_rows_round_trip_nulls_numbers_octets_and_reject_truncation() {
    let mut g = Generator::new();
    for case in 0..cases() {
        let count = 1 + (g.next() % 12) as usize;
        let mut columns = Vec::new();
        let mut values = Vec::new();
        let mut binary = vec![0; 1 + (count + 2).div_ceil(8)];
        let mut text = Vec::new();
        for index in 0..count {
            let kind = [
                ColumnType::VarString,
                ColumnType::Blob,
                ColumnType::Tiny,
                ColumnType::LongLong,
            ][(g.next() % 4) as usize];
            columns.push(column(index, kind));
            if g.next().is_multiple_of(4) {
                binary[1 + (index + 2) / 8] |= 1 << ((index + 2) % 8);
                text.push(251);
                values.push(None);
                continue;
            }
            let n = (g.next() % 96) as usize;
            let b = match kind {
                ColumnType::Tiny => vec![g.next() as u8],
                ColumnType::LongLong => g.next().to_le_bytes().to_vec(),
                _ => g.bytes(n),
            };
            if matches!(kind, ColumnType::Tiny | ColumnType::LongLong) {
                binary.extend_from_slice(&b);
            } else {
                binary.put_bytes_lenenc(&b);
            }
            text.put_bytes_lenenc(&b);
            values.push(Some(b));
        }
        let columns = Arc::new(columns);
        for (bytes, binary) in [(&binary, true), (&text, false)] {
            let row = DorisConnection::row(bytes.clone().into(), columns.clone(), binary).unwrap();
            for (index, expected) in values.iter().enumerate() {
                let actual = row.ranges[index]
                    .as_ref()
                    .map(|r| row.packet[r.clone()].to_vec());
                assert_eq!(&actual, expected, "case {case}, column {index}");
                if binary && let Some(value) = expected {
                    match columns[index].info.r#type {
                        ColumnType::Tiny => assert_eq!(
                            row.try_get::<i64, _>(index).unwrap(),
                            i64::from(value[0] as i8)
                        ),
                        ColumnType::LongLong => assert_eq!(
                            row.try_get::<i64, _>(index).unwrap(),
                            i64::from_le_bytes(value.as_slice().try_into().unwrap())
                        ),
                        _ => {}
                    }
                }
            }
            for cut in 0..bytes.len() {
                assert!(
                    DorisConnection::row(bytes[..cut].to_vec().into(), columns.clone(), binary)
                        .is_err(),
                    "case {case}, cut {cut}"
                );
            }
            let mut trailing = bytes.clone();
            trailing.push(0);
            assert!(DorisConnection::row(trailing.into(), columns.clone(), binary).is_err());
        }
    }
}
#[test]
fn randomized_raw_packets_and_binding_buffers_are_bounded_and_panic_free() {
    let mut g = Generator::new();
    let columns = Arc::new(vec![
        column(0, ColumnType::VarString),
        column(1, ColumnType::LongLong),
    ]);
    for _ in 0..cases() {
        let n = (g.next() % 160) as usize;
        let b = g.bytes(n);
        let _ = Cursor(&b).bytes();
        let _ = Cursor(&b).nul();
        let _ = ok_reply(&b);
        let _ = wire::database_error(&b);
        let _ = DorisConnection::row(b.clone().into(), columns.clone(), true);
        let _ = DorisConnection::row(b.clone().into(), columns.clone(), false);
        let kind = ColumnType::try_from(g.next() as u8).unwrap_or(ColumnType::VarString);
        let args = DorisArguments {
            values: b,
            types: vec![DorisTypeInfo::binary(kind)],
            null_bitmap: vec![(g.next() as u8) & 1],
        };
        for mode in [false, true] {
            let _ = bindings::compile_mode("INSERT INTO t VALUES(?)", &args, mode);
        }
    }
}
#[test]
fn randomized_unicode_sql_and_typed_pagination_preserve_marker_identity() {
    let mut g = Generator::new();
    let alphabet = [
        '中', '🙂', '\0', '\n', '\r', '\t', '\u{1a}', '\'', '"', '`', '?', '\\', ';', '/', '*',
        '-', ' ', 'a',
    ];
    for case in 0..cases() {
        let n = (g.next() % 64) as usize;
        let value: String = (0..n)
            .map(|_| alphabet[(g.next() % alphabet.len() as u64) as usize])
            .collect();
        let mut args = DorisArguments::default();
        args.add(value.as_str()).unwrap();
        args.add((g.next() % 1000) as i64).unwrap();
        let sql = "SELECT CAST(? AS STRING) /* ? ; ' */ LIMIT ?";
        assert_eq!(bindings::sites(sql).unwrap().len(), 2);
        for mode in [false, true] {
            let compiled = bindings::compile_mode(sql, &args, mode).unwrap();
            // Generated literals follow the selected session mode; the public
            // input lexer deliberately requires mode-independent marker sites.
            // Cross-check payload semantics with the real FE in driver_live.
            assert!(compiled.contains("/* ? ; ' */"), "case {case}");
            assert!(compiled.ends_with(&format!(
                "LIMIT {}",
                i64::from_le_bytes(args.values[args.values.len() - 8..].try_into().unwrap())
            )));
        }
        // Mutate SQL only at UTF-8 character boundaries. Rejection is permitted;
        // no parser may panic or access a byte inside a Unicode code point.
        let raw = format!("WITH `中?` AS (SELECT '{value}') SELECT ? /*🙂*/ LIMIT ?");
        let mut mutable = args.clone();
        let _ = bindings::adapt_query(&raw, Some(&mut mutable));
        for mode in [false, true] {
            let _ = bindings::compile_mode(&raw, &args, mode);
        }
    }
}
