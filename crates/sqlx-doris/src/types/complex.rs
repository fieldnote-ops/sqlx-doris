use std::{
    fmt,
    ops::{Deref, DerefMut},
};

use crate::sqlx::{
    Decode, Encode, MySql, Type,
    encode::IsNull,
    error::BoxDynError,
    mysql::{MySqlTypeInfo, MySqlValueRef},
};
use serde::{
    Deserialize, Deserializer, Serialize, Serializer,
    de::{DeserializeOwned, MapAccess, SeqAccess, Visitor},
    ser::SerializeMap,
};

use super::{json, text, text_compatible};

/// Doris ARRAY. Use `Array<Option<T>>` when elements can be SQL NULL.
/// JSON-based complex encoding rejects NaN/infinity instead of writing null.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Array<T>(pub Vec<T>);

/// Doris MAP as ordered key/value entries. Duplicate keys are not discarded.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct Map<K, V>(pub Vec<(K, V)>);

impl<K: Serialize, V: Serialize> Serialize for Map<K, V> {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let mut map = s.serialize_map(Some(self.0.len()))?;
        for (key, value) in &self.0 {
            map.serialize_entry(key, value)?;
        }
        map.end()
    }
}
impl<'de, K: Deserialize<'de>, V: Deserialize<'de>> Deserialize<'de> for Map<K, V> {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        struct Entries<K, V>(std::marker::PhantomData<(K, V)>);
        impl<'de, K: Deserialize<'de>, V: Deserialize<'de>> Visitor<'de> for Entries<K, V> {
            type Value = Map<K, V>;
            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("a map of Doris keys and values")
            }
            fn visit_map<A: MapAccess<'de>>(self, mut access: A) -> Result<Self::Value, A::Error> {
                let mut entries = Vec::with_capacity(access.size_hint().unwrap_or(0));
                while let Some(entry) = access.next_entry()? {
                    entries.push(entry);
                }
                Ok(Map(entries))
            }
            fn visit_seq<A: SeqAccess<'de>>(self, mut access: A) -> Result<Self::Value, A::Error> {
                #[derive(Deserialize)]
                struct Entry<K, V> {
                    key: K,
                    value: V,
                }
                let mut entries = Vec::with_capacity(access.size_hint().unwrap_or(0));
                while let Some(Entry { key, value }) = access.next_element()? {
                    entries.push((key, value));
                }
                Ok(Map(entries))
            }
        }
        d.deserialize_any(Entries(std::marker::PhantomData))
    }
}

/// Doris STRUCT mapped to a serde record, preserving named fields.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Struct<T>(pub T);

/// JSON representation for Doris VARIANT / JSON or MySQL JSON.
/// Arbitrary-precision JSON is enabled on the Rust side. On Doris 4.1.5 use
/// `CAST(? AS JSON)` when writing objects to a VARIANT table column. An explicit
/// `PARSE_TO_VARIANT(?)` expression parses JSON but its result's properties can
/// conflict with default VARIANT table properties on 4.1.5. `CAST(? AS VARIANT)`
/// keeps the parameter as a string.
/// The server parser can keep out-of-range integer documents as strings, and
/// raw scalar VARIANT output erases distinctions such as `"123"` versus `123`.
/// This wrapper cannot reconstruct that lost information: use explicit typed
/// SQL projections for scalar roots and verify native VARIANT semantics.
/// Native VARIANT storage also normalizes some null/empty values. Use a JSON
/// column when the original document shape must be preserved. Non-finite Rust
/// floats are rejected before JSON encoding; they never become implicit nulls.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Variant<T = serde_json::Value>(pub T);

macro_rules! value_wrapper {
    ($name:ident<$($generic:ident),+>, $inner:ty) => {
        impl<$($generic),+> From<$inner> for $name<$($generic),+> {
            fn from(value: $inner) -> Self {
                Self(value)
            }
        }

        impl<$($generic),+> Deref for $name<$($generic),+> {
            type Target = $inner;

            fn deref(&self) -> &Self::Target {
                &self.0
            }
        }

        impl<$($generic),+> DerefMut for $name<$($generic),+> {
            fn deref_mut(&mut self) -> &mut Self::Target {
                &mut self.0
            }
        }
    };
}

value_wrapper!(Array<T>, Vec<T>);
value_wrapper!(Map<K, V>, Vec<(K, V)>);
value_wrapper!(Struct<T>, T);
value_wrapper!(Variant<T>, T);

macro_rules! sqlx_complex {
    ($name:ident<$($generic:ident),+>, $encode:path) => {
        impl<$($generic),+> Type<MySql> for $name<$($generic),+> {
            fn type_info() -> MySqlTypeInfo {
                <String as Type<MySql>>::type_info()
            }

            fn compatible(info: &MySqlTypeInfo) -> bool {
                text_compatible(info)
            }
        }

        impl<$($generic: DeserializeOwned),+> Decode<'_, MySql> for $name<$($generic),+> {
            fn decode(value: MySqlValueRef<'_>) -> Result<Self, BoxDynError> {
                let raw = <String as Decode<MySql>>::decode(value)?;
                text::from_str(&raw).map_err(Into::into)
            }
        }

        impl<$($generic: Serialize),+> Encode<'_, MySql> for $name<$($generic),+> {
            fn encode_by_ref(&self, buf: &mut Vec<u8>) -> Result<IsNull, BoxDynError> {
                <String as Encode<MySql>>::encode_by_ref(&$encode(self)?, buf)
            }
        }
    };
}

sqlx_complex!(Array<T>, text::to_doris);
sqlx_complex!(Map<K, V>, text::to_doris);
sqlx_complex!(Struct<T>, text::to_doris);
sqlx_complex!(Variant<T>, json::to_string);
