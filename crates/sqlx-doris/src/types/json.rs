//! JSON serialization that rejects non-finite floats instead of converting them
//! to null. The adapter keeps serde_json's precision and map-key behavior.
use serde::{
    Serialize, Serializer,
    ser::{
        self, SerializeMap, SerializeSeq, SerializeStruct, SerializeStructVariant, SerializeTuple,
        SerializeTupleStruct, SerializeTupleVariant,
    },
};
use std::fmt;

pub(crate) fn to_string<T: Serialize + ?Sized>(value: &T) -> serde_json::Result<String> {
    serde_json::to_string(&Checked(value))
}
#[cfg(any(feature = "stream-load", test))]
pub(crate) fn to_vec<T: Serialize + ?Sized>(value: &T) -> serde_json::Result<Vec<u8>> {
    serde_json::to_vec(&Checked(value))
}
#[cfg(any(feature = "stream-load", test))]
pub(crate) fn to_value<T: Serialize + ?Sized>(value: &T) -> serde_json::Result<serde_json::Value> {
    serde_json::to_value(Checked(value))
}

struct Checked<'a, T: ?Sized>(&'a T);
impl<T: Serialize + ?Sized> Serialize for Checked<'_, T> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.0.serialize(Finite(serializer))
    }
}
struct Finite<S>(S);
macro_rules! primitive {
    ($($method:ident($ty:ty)),+ $(,)?) => {$(
        fn $method(self, value: $ty) -> Result<Self::Ok, Self::Error> { self.0.$method(value) }
    )+};
}
impl<S: Serializer> Serializer for Finite<S> {
    type Ok = S::Ok;
    type Error = S::Error;
    type SerializeSeq = Finite<S::SerializeSeq>;
    type SerializeTuple = Finite<S::SerializeTuple>;
    type SerializeTupleStruct = Finite<S::SerializeTupleStruct>;
    type SerializeTupleVariant = Finite<S::SerializeTupleVariant>;
    type SerializeMap = Finite<S::SerializeMap>;
    type SerializeStruct = Finite<S::SerializeStruct>;
    type SerializeStructVariant = Finite<S::SerializeStructVariant>;
    primitive!(
        serialize_bool(bool),
        serialize_i8(i8),
        serialize_i16(i16),
        serialize_i32(i32),
        serialize_i64(i64),
        serialize_i128(i128),
        serialize_u8(u8),
        serialize_u16(u16),
        serialize_u32(u32),
        serialize_u64(u64),
        serialize_u128(u128),
        serialize_char(char),
        serialize_str(&str),
        serialize_bytes(&[u8])
    );
    fn serialize_f32(self, value: f32) -> Result<Self::Ok, Self::Error> {
        if !value.is_finite() {
            return Err(ser::Error::custom(
                "non-finite float cannot be encoded as JSON or a Doris complex value",
            ));
        }
        self.0.serialize_f32(value)
    }
    fn serialize_f64(self, value: f64) -> Result<Self::Ok, Self::Error> {
        if !value.is_finite() {
            return Err(ser::Error::custom(
                "non-finite float cannot be encoded as JSON or a Doris complex value",
            ));
        }
        self.0.serialize_f64(value)
    }
    fn serialize_none(self) -> Result<Self::Ok, Self::Error> {
        self.0.serialize_none()
    }
    fn serialize_some<T: Serialize + ?Sized>(self, value: &T) -> Result<Self::Ok, Self::Error> {
        self.0.serialize_some(&Checked(value))
    }
    fn serialize_unit(self) -> Result<Self::Ok, Self::Error> {
        self.0.serialize_unit()
    }
    fn serialize_unit_struct(self, name: &'static str) -> Result<Self::Ok, Self::Error> {
        self.0.serialize_unit_struct(name)
    }
    fn serialize_unit_variant(
        self,
        name: &'static str,
        index: u32,
        variant: &'static str,
    ) -> Result<Self::Ok, Self::Error> {
        self.0.serialize_unit_variant(name, index, variant)
    }
    fn serialize_newtype_struct<T: Serialize + ?Sized>(
        self,
        name: &'static str,
        value: &T,
    ) -> Result<Self::Ok, Self::Error> {
        self.0.serialize_newtype_struct(name, &Checked(value))
    }
    fn serialize_newtype_variant<T: Serialize + ?Sized>(
        self,
        name: &'static str,
        index: u32,
        variant: &'static str,
        value: &T,
    ) -> Result<Self::Ok, Self::Error> {
        self.0
            .serialize_newtype_variant(name, index, variant, &Checked(value))
    }
    fn serialize_seq(self, len: Option<usize>) -> Result<Self::SerializeSeq, Self::Error> {
        self.0.serialize_seq(len).map(Finite)
    }
    fn serialize_tuple(self, len: usize) -> Result<Self::SerializeTuple, Self::Error> {
        self.0.serialize_tuple(len).map(Finite)
    }
    fn serialize_tuple_struct(
        self,
        name: &'static str,
        len: usize,
    ) -> Result<Self::SerializeTupleStruct, Self::Error> {
        self.0.serialize_tuple_struct(name, len).map(Finite)
    }
    fn serialize_tuple_variant(
        self,
        name: &'static str,
        index: u32,
        variant: &'static str,
        len: usize,
    ) -> Result<Self::SerializeTupleVariant, Self::Error> {
        self.0
            .serialize_tuple_variant(name, index, variant, len)
            .map(Finite)
    }
    fn serialize_map(self, len: Option<usize>) -> Result<Self::SerializeMap, Self::Error> {
        self.0.serialize_map(len).map(Finite)
    }
    fn serialize_struct(
        self,
        name: &'static str,
        len: usize,
    ) -> Result<Self::SerializeStruct, Self::Error> {
        self.0.serialize_struct(name, len).map(Finite)
    }
    fn serialize_struct_variant(
        self,
        name: &'static str,
        index: u32,
        variant: &'static str,
        len: usize,
    ) -> Result<Self::SerializeStructVariant, Self::Error> {
        self.0
            .serialize_struct_variant(name, index, variant, len)
            .map(Finite)
    }
    fn collect_str<T: fmt::Display + ?Sized>(self, value: &T) -> Result<Self::Ok, Self::Error> {
        self.0.collect_str(value)
    }
    fn is_human_readable(&self) -> bool {
        self.0.is_human_readable()
    }
}
macro_rules! sequence {
    ($trait:ident,$method:ident) => {
        impl<S: $trait> $trait for Finite<S> {
            type Ok = S::Ok;
            type Error = S::Error;
            fn $method<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<(), Self::Error> {
                self.0.$method(&Checked(value))
            }
            fn end(self) -> Result<Self::Ok, Self::Error> {
                self.0.end()
            }
        }
    };
}
sequence!(SerializeSeq, serialize_element);
sequence!(SerializeTuple, serialize_element);
sequence!(SerializeTupleStruct, serialize_field);
sequence!(SerializeTupleVariant, serialize_field);
impl<S: SerializeMap> SerializeMap for Finite<S> {
    type Ok = S::Ok;
    type Error = S::Error;
    fn serialize_key<T: Serialize + ?Sized>(&mut self, key: &T) -> Result<(), Self::Error> {
        self.0.serialize_key(&Checked(key))
    }
    fn serialize_value<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<(), Self::Error> {
        self.0.serialize_value(&Checked(value))
    }
    fn end(self) -> Result<Self::Ok, Self::Error> {
        self.0.end()
    }
}
macro_rules! record {
    ($trait:ident) => {
        impl<S: $trait> $trait for Finite<S> {
            type Ok = S::Ok;
            type Error = S::Error;
            fn serialize_field<T: Serialize + ?Sized>(
                &mut self,
                key: &'static str,
                value: &T,
            ) -> Result<(), Self::Error> {
                self.0.serialize_field(key, &Checked(value))
            }
            fn skip_field(&mut self, key: &'static str) -> Result<(), Self::Error> {
                self.0.skip_field(key)
            }
            fn end(self) -> Result<Self::Ok, Self::Error> {
                self.0.end()
            }
        }
    };
}
record!(SerializeStruct);
record!(SerializeStructVariant);

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{Array, Map, Struct, Variant};
    use sqlx::{Encode, MySql};
    #[derive(Serialize)]
    struct Reading {
        nested: Option<Array<f64>>,
    }
    #[test]
    fn non_finite_values_fail_in_nested_wrappers_instead_of_becoming_null() {
        for number in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
            let row = Reading {
                nested: Some(Array(vec![number])),
            };
            assert!(to_vec(&row).unwrap_err().to_string().contains("non-finite"));
            assert!(to_value(&row).is_err());
            assert!(
                <Struct<_> as Encode<MySql>>::encode_by_ref(&Struct(&row), &mut vec![]).is_err()
            );
            assert!(
                <Map<_, _> as Encode<MySql>>::encode_by_ref(
                    &Map(vec![(1, Some(number))]),
                    &mut vec![]
                )
                .is_err()
            );
            assert!(
                <Variant<_> as Encode<MySql>>::encode_by_ref(&Variant(&row), &mut vec![]).is_err()
            );
        }
        assert!(to_string(&[f32::NAN]).is_err());
        assert_eq!(to_string(&[Some(1.25), None]).unwrap(), "[1.25,null]");
    }
    #[test]
    fn wide_numbers_map_entries_and_json_precision_survive_the_adapter() {
        let wide: serde_json::Value =
            serde_json::from_str("123456789012345678901234567890.123456789").unwrap();
        assert_eq!(to_string(&wide).unwrap(), wide.to_string());
        assert_eq!(to_value(&wide).unwrap(), wide);
        let entries = Map(vec![(1, i128::MIN), (1, i128::MAX)]);
        let json = to_string(&entries).unwrap();
        assert_eq!(json, format!("{{\"1\":{},\"1\":{}}}", i128::MIN, i128::MAX));
    }
}
