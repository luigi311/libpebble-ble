//! AppMessage wire codec: Rust types ↔ D-Bus `a{i(sv)}` wire shape.
//!
//! Each AppMessage value is encoded as a `(tag, payload)` pair so the integer
//! width survives the D-Bus hop. Tag is one of:
//! `u8 u16 u32 i8 i16 i32 uint int str bytes`.

use std::collections::HashMap;

use libpebble_ble::AppMessageValue;
use zbus::zvariant::{Array, OwnedValue, Str};

/// `(tag, payload)` pair as it appears on the wire.
pub type WireValue = (String, OwnedValue);
pub type WireDict = HashMap<i32, WireValue>;

pub fn decode_wire_dict(wire: WireDict) -> HashMap<u32, AppMessageValue> {
    wire.into_iter().filter_map(|(k, (tag, val))| {
        let k = u32::try_from(k).ok()?; // reject negative keys
        let v = decode_wire_value(&tag, val)?;
        Some((k, v))
    }).collect()
}

pub fn decode_wire_value(tag: &str, val: OwnedValue) -> Option<AppMessageValue> {
    match tag {
        "u8"   => Some(AppMessageValue::U8(u32::try_from(val).ok()? as u8)),
        "u16"  => Some(AppMessageValue::U16(u32::try_from(val).ok()? as u16)),
        "u32"  => Some(AppMessageValue::U32(u32::try_from(val).ok()?)),
        "i8"   => Some(AppMessageValue::I8(i32::try_from(val).ok()? as i8)),
        "i16"  => Some(AppMessageValue::I16(i32::try_from(val).ok()? as i16)),
        "i32"  => Some(AppMessageValue::I32(i32::try_from(val).ok()?)),
        // Pebble AppMessage caps integers at 32 bits; uint/int widen to u64/i64
        // on the Rust side but the wire value is always a 32-bit D-Bus integer.
        "uint" => Some(AppMessageValue::Uint(u32::try_from(val).ok()? as u64)),
        "int"  => Some(AppMessageValue::Int(i32::try_from(val).ok()? as i64)),
        "str"  => Some(AppMessageValue::Str(String::try_from(val).ok()?)),
        "bytes" => {
            let arr = Array::try_from(val).ok()?;
            let b: Vec<u8> = Vec::try_from(arr).ok()?;
            Some(AppMessageValue::Bytes(b))
        }
        _ => None,
    }
}

pub fn encode_wire_dict(data: &HashMap<u32, AppMessageValue>) -> WireDict {
    data.iter().filter_map(|(k, v)| {
        let k = i32::try_from(*k).ok()?; // reject keys >= 2^31
        let (tag, owned) = encode_wire_value(v);
        Some((k, (tag, owned)))
    }).collect()
}

pub fn encode_wire_value(value: &AppMessageValue) -> (String, OwnedValue) {
    match value {
        AppMessageValue::U8(v)    => ("u8".into(),    OwnedValue::from(*v as u32)),
        AppMessageValue::U16(v)   => ("u16".into(),   OwnedValue::from(*v as u32)),
        AppMessageValue::U32(v)   => ("u32".into(),   OwnedValue::from(*v)),
        AppMessageValue::I8(v)    => ("i8".into(),    OwnedValue::from(*v as i32)),
        AppMessageValue::I16(v)   => ("i16".into(),   OwnedValue::from(*v as i32)),
        AppMessageValue::I32(v)   => ("i32".into(),   OwnedValue::from(*v)),
        // Pebble AppMessage caps integers at 32 bits; upper bits are dropped here.
        AppMessageValue::Uint(v)  => ("uint".into(),  OwnedValue::from(*v as u32)),
        AppMessageValue::Int(v)   => ("int".into(),   OwnedValue::from(*v as i32)),
        AppMessageValue::Str(s)   => ("str".into(),   OwnedValue::from(Str::from(s.as_str()))),
        AppMessageValue::Bytes(b) => {
            let arr = Array::from(b.as_slice());
            ("bytes".into(), OwnedValue::try_from(arr).expect("bytes encode"))
        }
    }
}
