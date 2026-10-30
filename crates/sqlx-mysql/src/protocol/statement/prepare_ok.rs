use bytes::{Buf, Bytes};

use crate::error::Error;
use crate::io::ProtocolDecode;
use crate::protocol::Capabilities;

// https://dev.mysql.com/doc/internals/en/com-stmt-prepare-response.html#packet-COM_STMT_PREPARE_OK

#[derive(Debug)]
pub(crate) struct PrepareOk {
    pub(crate) statement_id: u32,
    pub(crate) columns: u16,
    pub(crate) params: u16,
    #[allow(unused)]
    pub(crate) warnings: u16,
}

impl ProtocolDecode<'_, Capabilities> for PrepareOk {
    fn decode_with(buf: Bytes, _: Capabilities) -> Result<Self, Error> {
        // The mandatory prefix is 10 bytes. MySQL permits warning_count to be
        // omitted; Doris uses this form for statements with no params/columns.
        // See COM_STMT_PREPARE_OK's packet_length >= 12 condition.
        const SIZE: usize = 10;

        let mut slice = buf.get(..SIZE).ok_or_else(|| {
            err_protocol!(
                "PrepareOk expected at least 10 bytes but got {} bytes",
                buf.len()
            )
        })?;

        let status = slice.get_u8();
        if status != 0x00 {
            return Err(err_protocol!(
                "expected 0x00 (COM_STMT_PREPARE_OK) but found 0x{:02x}",
                status
            ));
        }

        let statement_id = slice.get_u32_le();
        let columns = slice.get_u16_le();
        let params = slice.get_u16_le();

        slice.advance(1); // reserved: string<1>

        let warnings = match buf.len() - SIZE {
            0 => 0,
            1 => {
                return Err(err_protocol!(
                    "PrepareOk contains a truncated warning_count"
                ))
            }
            _ => u16::from_le_bytes([buf[SIZE], buf[SIZE + 1]]),
        };

        Ok(Self {
            statement_id,
            columns,
            params,
            warnings,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn doris_ten_byte_prepare_ok_defaults_warnings_without_losing_statement_id() {
        let packet = Bytes::from_static(&[0, 0x12, 0x34, 0x56, 0x80, 0, 0, 0, 0, 0]);
        let decoded = PrepareOk::decode_with(packet, Capabilities::empty()).unwrap();
        assert_eq!(decoded.statement_id, 0x8056_3412);
        assert_eq!(
            (decoded.columns, decoded.params, decoded.warnings),
            (0, 0, 0)
        );
    }

    #[test]
    fn mysql_twelve_byte_prepare_ok_retains_fields_and_nonzero_warnings() {
        let packet = Bytes::from_static(&[0, 42, 0, 0, 0, 3, 0, 2, 0, 0, 0x34, 0x12]);
        let decoded = PrepareOk::decode_with(packet, Capabilities::empty()).unwrap();
        assert_eq!(decoded.statement_id, 42);
        assert_eq!(
            (decoded.columns, decoded.params, decoded.warnings),
            (3, 2, 0x1234)
        );
    }

    #[test]
    fn metadata_following_byte_keeps_existing_thirteen_byte_behavior() {
        let packet = Bytes::from_static(&[0, 7, 0, 0, 0, 1, 0, 2, 0, 0, 9, 0, 1]);
        let decoded = PrepareOk::decode_with(packet, Capabilities::empty()).unwrap();
        assert_eq!(decoded.statement_id, 7);
        assert_eq!(
            (decoded.columns, decoded.params, decoded.warnings),
            (1, 2, 9)
        );
    }

    #[test]
    fn truncated_mandatory_header_and_half_warning_count_are_protocol_errors() {
        let complete = [0; 12];
        for length in (0..10).chain([11]) {
            let error = PrepareOk::decode_with(
                Bytes::copy_from_slice(&complete[..length]),
                Capabilities::empty(),
            )
            .unwrap_err();
            assert!(
                matches!(error, Error::Protocol(_)),
                "length={length}: {error:?}"
            );
        }
    }

    #[test]
    fn invalid_status_is_not_accepted_as_a_short_success() {
        let error = PrepareOk::decode_with(
            Bytes::from_static(&[1, 0, 0, 0, 0, 0, 0, 0, 0, 0]),
            Capabilities::empty(),
        )
        .unwrap_err();
        assert!(matches!(error, Error::Protocol(_)));
    }
}
