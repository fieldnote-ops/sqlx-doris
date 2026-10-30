use std::{fmt, vec};

use serde::{
    Deserializer, Serialize,
    de::{
        self, DeserializeOwned, DeserializeSeed, IntoDeserializer, MapAccess, SeqAccess, Visitor,
    },
};
use serde_json::Number;

/// A malformed or incompatible Doris complex-value representation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TextError(String);
impl fmt::Display for TextError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}
impl std::error::Error for TextError {}
impl de::Error for TextError {
    fn custom<T: fmt::Display>(message: T) -> Self {
        Self(message.to_string())
    }
}

pub(crate) fn from_str<T: DeserializeOwned>(input: &str) -> Result<T, TextError> {
    // Standard JSON handles most ARRAY/STRUCT/VARIANT results, including its
    // arbitrary-precision number representation. Doris MAP also permits keys
    // such as bare integers; preserve those with a small dedicated value parser.
    if let Ok(value) = serde_json::from_str(input) {
        return Ok(value);
    }
    let mut parser = Parser {
        source: input,
        position: 0,
    };
    let value = parser.value()?;
    parser.space();
    if parser.position != input.len() {
        return Err(parser.error("trailing data"));
    }
    T::deserialize(value)
}

// Doris CAST's nested string reader removes a backslash before the next byte;
// it does not interpret JSON escapes such as \n or \u0000. Keep control bytes
// literal inside the length-encoded parameter and escape quotes/backslashes.
pub(crate) fn to_doris<T: Serialize>(value: &T) -> Result<String, TextError> {
    let value = to_value(value)?;
    let mut output = String::new();
    value.write_doris(&mut output);
    Ok(output)
}

pub(crate) fn to_value<T: Serialize>(value: &T) -> Result<Value, TextError> {
    let json = super::json::to_string(value).map_err(|e| TextError(e.to_string()))?;
    let mut parser = Parser {
        source: &json,
        position: 0,
    };
    parser.value()
}

#[derive(Debug)]
pub(crate) enum Value {
    Null,
    Bool(bool),
    Number(Number),
    String(String),
    Array(Vec<Value>),
    Map(Vec<(Value, Value)>),
}

struct Parser<'a> {
    source: &'a str,
    position: usize,
}
impl Parser<'_> {
    fn error(&self, message: &str) -> TextError {
        TextError(format!("{message} at byte {}", self.position))
    }
    fn space(&mut self) {
        while self
            .source
            .as_bytes()
            .get(self.position)
            .is_some_and(u8::is_ascii_whitespace)
        {
            self.position += 1;
        }
    }
    fn take(&mut self, b: u8) -> bool {
        self.space();
        if self.source.as_bytes().get(self.position) == Some(&b) {
            self.position += 1;
            true
        } else {
            false
        }
    }
    fn value(&mut self) -> Result<Value, TextError> {
        self.space();
        match self.source.as_bytes().get(self.position) {
            Some(b'"') => {
                let start = self.position;
                self.position += 1;
                let mut escaped = false;
                while let Some(&b) = self.source.as_bytes().get(self.position) {
                    self.position += 1;
                    if escaped {
                        escaped = false;
                    } else if b == b'\\' {
                        escaped = true;
                    } else if b == b'"' {
                        return serde_json::from_str(&self.source[start..self.position])
                            .map(Value::String)
                            .map_err(|e| TextError(e.to_string()));
                    }
                }
                Err(self.error("unterminated string"))
            }
            Some(b'[') => {
                self.position += 1;
                let mut values = vec![];
                if self.take(b']') {
                    return Ok(Value::Array(values));
                }
                loop {
                    values.push(self.value()?);
                    if self.take(b']') {
                        break;
                    }
                    if !self.take(b',') {
                        return Err(self.error("expected ',' or ']'"));
                    }
                }
                Ok(Value::Array(values))
            }
            Some(b'{') => {
                self.position += 1;
                let mut entries = vec![];
                if self.take(b'}') {
                    return Ok(Value::Map(entries));
                }
                loop {
                    let key = self.value()?;
                    if !self.take(b':') {
                        return Err(self.error("expected ':'"));
                    }
                    entries.push((key, self.value()?));
                    if self.take(b'}') {
                        break;
                    }
                    if !self.take(b',') {
                        return Err(self.error("expected ',' or '}'"));
                    }
                }
                Ok(Value::Map(entries))
            }
            Some(_) => {
                let start = self.position;
                while self
                    .source
                    .as_bytes()
                    .get(self.position)
                    .is_some_and(|b| !b.is_ascii_whitespace() && !b",:]}".contains(b))
                {
                    self.position += 1;
                }
                let atom = &self.source[start..self.position];
                match atom {
                    "null" | "NULL" => Ok(Value::Null),
                    "true" => Ok(Value::Bool(true)),
                    "false" => Ok(Value::Bool(false)),
                    _ => serde_json::from_str::<Number>(atom)
                        .map(Value::Number)
                        .map_err(|_| self.error("invalid value")),
                }
            }
            None => Err(self.error("expected a value")),
        }
    }
}

impl Value {
    fn write_doris(self, output: &mut String) {
        match self {
            Self::Null => output.push_str("null"),
            Self::Bool(b) => output.push_str(if b { "true" } else { "false" }),
            Self::Number(n) => output.push_str(&n.to_string()),
            Self::String(s) => {
                output.push('"');
                for c in s.chars() {
                    if matches!(c, '"' | '\\') {
                        output.push('\\');
                    }
                    output.push(c);
                }
                output.push('"');
            }
            Self::Array(values) => {
                output.push('[');
                for (i, value) in values.into_iter().enumerate() {
                    if i > 0 {
                        output.push(',');
                    }
                    value.write_doris(output);
                }
                output.push(']');
            }
            Self::Map(entries) => {
                output.push('{');
                for (i, (key, value)) in entries.into_iter().enumerate() {
                    if i > 0 {
                        output.push(',');
                    }
                    key.write_doris(output);
                    output.push(':');
                    value.write_doris(output);
                }
                output.push('}');
            }
        }
    }
    fn number(self) -> Result<Number, TextError> {
        match self {
            Self::Number(n) => Ok(n),
            Self::String(s) => {
                serde_json::from_str(&s).map_err(|_| TextError("expected a number".into()))
            }
            _ => Err(TextError("expected a number".into())),
        }
    }
}

struct Sequence(vec::IntoIter<Value>);
impl<'de> SeqAccess<'de> for Sequence {
    type Error = TextError;
    fn next_element_seed<T: DeserializeSeed<'de>>(
        &mut self,
        seed: T,
    ) -> Result<Option<T::Value>, TextError> {
        self.0.next().map(|v| seed.deserialize(v)).transpose()
    }
    fn size_hint(&self) -> Option<usize> {
        Some(self.0.len())
    }
}
struct Mapping {
    entries: vec::IntoIter<(Value, Value)>,
    pending: Option<Value>,
}
impl<'de> MapAccess<'de> for Mapping {
    type Error = TextError;
    fn next_key_seed<K: DeserializeSeed<'de>>(
        &mut self,
        seed: K,
    ) -> Result<Option<K::Value>, TextError> {
        match self.entries.next() {
            Some((key, value)) => {
                self.pending = Some(value);
                seed.deserialize(key).map(Some)
            }
            None => Ok(None),
        }
    }
    fn next_value_seed<V: DeserializeSeed<'de>>(&mut self, seed: V) -> Result<V::Value, TextError> {
        seed.deserialize(
            self.pending
                .take()
                .ok_or_else(|| TextError("map value missing".into()))?,
        )
    }
    fn size_hint(&self) -> Option<usize> {
        Some(self.entries.len())
    }
}

macro_rules! number_method {
    ($method:ident) => {
        fn $method<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, TextError> {
            self.number()?
                .$method(visitor)
                .map_err(|e| TextError(e.to_string()))
        }
    };
}
impl<'de> Deserializer<'de> for Value {
    type Error = TextError;
    fn deserialize_any<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, TextError> {
        match self {
            Self::Null => visitor.visit_unit(),
            Self::Bool(b) => visitor.visit_bool(b),
            Self::String(s) => visitor.visit_string(s),
            Self::Number(n) => n
                .deserialize_any(visitor)
                .map_err(|e| TextError(e.to_string())),
            Self::Array(values) => visitor.visit_seq(Sequence(values.into_iter())),
            Self::Map(entries) => visitor.visit_map(Mapping {
                entries: entries.into_iter(),
                pending: None,
            }),
        }
    }
    fn deserialize_option<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, TextError> {
        match self {
            Self::Null => visitor.visit_none(),
            value => visitor.visit_some(value),
        }
    }
    fn deserialize_newtype_struct<V: Visitor<'de>>(
        self,
        _name: &'static str,
        visitor: V,
    ) -> Result<V::Value, TextError> {
        visitor.visit_newtype_struct(self)
    }
    fn deserialize_enum<V: Visitor<'de>>(
        self,
        _name: &'static str,
        _variants: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, TextError> {
        match self {
            Self::String(s) => visitor.visit_enum(s.into_deserializer()),
            _ => Err(TextError("expected an enum string".into())),
        }
    }
    number_method!(deserialize_i8);
    number_method!(deserialize_i16);
    number_method!(deserialize_i32);
    number_method!(deserialize_i64);
    number_method!(deserialize_i128);
    number_method!(deserialize_u8);
    number_method!(deserialize_u16);
    number_method!(deserialize_u32);
    number_method!(deserialize_u64);
    number_method!(deserialize_u128);
    number_method!(deserialize_f32);
    number_method!(deserialize_f64);
    serde::forward_to_deserialize_any! {
        bool char str string bytes byte_buf unit unit_struct
        seq tuple tuple_struct map struct identifier ignored_any
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{Array, LargeInt, Map, Struct};
    use serde::Deserialize;

    #[derive(Debug, Deserialize, PartialEq)]
    struct Record {
        name: String,
        data: Map<i32, Array<Option<LargeInt>>>,
    }

    #[test]
    fn nested_map_keeps_key_types_duplicates_nulls_and_128_bit_integers() {
        let raw = r#"{"name":"中文🙂 \"quoted\"\\\n","data":{1:[170141183460469231731687303715884105727,null],1:[]}}"#;
        let value: Struct<Record> = from_str(raw).unwrap();
        assert_eq!(value.name, "中文🙂 \"quoted\"\\\n");
        assert_eq!(
            value.data.0,
            vec![
                (1, Array(vec![Some(LargeInt(i128::MAX)), None])),
                (1, Array(vec![]))
            ]
        );
    }
    #[test]
    fn malformed_or_trailing_input_and_integer_overflow_are_errors() {
        for raw in ["{1:}", "[1,]", "{1:2} junk", "{1 2}", "\"unfinished"] {
            assert!(from_str::<Map<i32, i32>>(raw).is_err(), "{raw}");
        }
        assert!(from_str::<Map<i32, i8>>("{1:128}").is_err());
    }
    #[test]
    fn empty_containers_and_json_number_precision_are_preserved() {
        assert_eq!(from_str::<Map<i32, String>>("{}").unwrap().0, vec![]);
        let value: Map<i32, serde_json::Value> =
            from_str("{1:123456789012345678901234567890.123456789}").unwrap();
        assert_eq!(
            value.0[0].1.to_string(),
            "123456789012345678901234567890.123456789"
        );
    }
}
