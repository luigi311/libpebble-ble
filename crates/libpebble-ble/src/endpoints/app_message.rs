//! AppMessage — the key/value dict protocol Pebble watchapps use.

use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum AppMessageCmd {
    Push = 0x01,
    Ack = 0xFF,
    Nack = 0x7F,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
enum TupleType {
    Bytes = 0,
    CString = 1,
    Uint = 2,
    Int = 3,
}

/// An AppMessage value. Width-pinned integer variants survive the D-Bus hop
/// to the client with the exact type the watchapp expects to read.
#[derive(Debug, Clone, PartialEq)]
pub enum AppMessageValue {
    U8(u8),
    U16(u16),
    U32(u32),
    I8(i8),
    I16(i16),
    I32(i32),
    /// Auto-width unsigned (smallest that fits).
    Uint(u64),
    /// Auto-width signed (smallest that fits).
    Int(i64),
    Str(String),
    Bytes(Vec<u8>),
}

impl AppMessageValue {
    pub fn u8(v: u8) -> Self {
        Self::U8(v)
    }
    pub fn u16(v: u16) -> Self {
        Self::U16(v)
    }
    pub fn u32(v: u32) -> Self {
        Self::U32(v)
    }
    pub fn i8(v: i8) -> Self {
        Self::I8(v)
    }
    pub fn i16(v: i16) -> Self {
        Self::I16(v)
    }
    pub fn i32(v: i32) -> Self {
        Self::I32(v)
    }
}

// encode_tuple_correct matches Python's struct.pack("<IBH", key, ttype, length):
// <IBH = little-endian: I(4 bytes key) B(1 byte type) H(2 bytes length)
// Total header = 7 bytes
fn encode_tuple_correct(key: u32, value: &AppMessageValue) -> Vec<u8> {
    let (ttype, raw): (u8, Vec<u8>) = match value {
        AppMessageValue::U8(v) => (TupleType::Uint as u8, vec![*v]),
        AppMessageValue::U16(v) => (TupleType::Uint as u8, v.to_le_bytes().to_vec()),
        AppMessageValue::U32(v) => (TupleType::Uint as u8, v.to_le_bytes().to_vec()),
        AppMessageValue::I8(v) => (TupleType::Int as u8, v.to_le_bytes().to_vec()),
        AppMessageValue::I16(v) => (TupleType::Int as u8, v.to_le_bytes().to_vec()),
        AppMessageValue::I32(v) => (TupleType::Int as u8, v.to_le_bytes().to_vec()),
        AppMessageValue::Uint(v) => {
            let raw = if *v <= u8::MAX as u64 {
                vec![*v as u8]
            } else if *v <= u16::MAX as u64 {
                (*v as u16).to_le_bytes().to_vec()
            } else {
                (*v as u32).to_le_bytes().to_vec()
            };
            (TupleType::Uint as u8, raw)
        }
        AppMessageValue::Int(v) => {
            let raw = if *v >= i8::MIN as i64 && *v <= i8::MAX as i64 {
                (*v as i8).to_le_bytes().to_vec()
            } else if *v >= i16::MIN as i64 && *v <= i16::MAX as i64 {
                (*v as i16).to_le_bytes().to_vec()
            } else {
                (*v as i32).to_le_bytes().to_vec()
            };
            (TupleType::Int as u8, raw)
        }
        AppMessageValue::Str(s) => {
            let mut raw = s.as_bytes().to_vec();
            raw.push(0);
            (TupleType::CString as u8, raw)
        }
        AppMessageValue::Bytes(b) => (TupleType::Bytes as u8, b.clone()),
    };

    let length = raw.len() as u16;
    let mut out = Vec::with_capacity(7 + raw.len());
    out.extend_from_slice(&key.to_le_bytes()); // u32 LE
    out.push(ttype); // u8
    out.extend_from_slice(&length.to_le_bytes()); // u16 LE
    out.extend_from_slice(&raw);
    out
}

fn decode_tuples(payload: &[u8]) -> std::collections::HashMap<u32, AppMessageValue> {
    let mut out = std::collections::HashMap::new();
    let mut off = 0usize;
    while off + 7 <= payload.len() {
        let key = u32::from_le_bytes([payload[off], payload[off+1], payload[off+2], payload[off+3]]);
        let ttype = payload[off+4];
        let length = u16::from_le_bytes([payload[off+5], payload[off+6]]) as usize;
        off += 7;
        if off + length > payload.len() {
            break;
        }
        let raw = &payload[off..off + length];
        off += length;

        let value = match ttype {
            t if t == TupleType::CString as u8 => {
                let s = raw.strip_suffix(&[0]).unwrap_or(raw);
                AppMessageValue::Str(String::from_utf8_lossy(s).into_owned())
            }
            t if t == TupleType::Uint as u8 => {
                let v = match raw.len() {
                    1 => raw[0] as u64,
                    2 => u16::from_le_bytes([raw[0], raw[1]]) as u64,
                    4 => u32::from_le_bytes([raw[0], raw[1], raw[2], raw[3]]) as u64,
                    _ => continue,
                };
                AppMessageValue::Uint(v)
            }
            t if t == TupleType::Int as u8 => {
                let v = match raw.len() {
                    1 => raw[0] as i8 as i64,
                    2 => i16::from_le_bytes([raw[0], raw[1]]) as i64,
                    4 => i32::from_le_bytes([raw[0], raw[1], raw[2], raw[3]]) as i64,
                    _ => continue,
                };
                AppMessageValue::Int(v)
            }
            _ => AppMessageValue::Bytes(raw.to_vec()),
        };
        out.insert(key, value);
    }
    out
}

pub struct ParsedAppMessage {
    pub cmd: AppMessageCmd,
    pub txn: u8,
    pub app_uuid: Option<String>,
    pub data: Option<std::collections::HashMap<u32, AppMessageValue>>,
}

pub fn parse_app_message(payload: &[u8]) -> Option<ParsedAppMessage> {
    if payload.len() < 2 {
        return None;
    }
    let raw_cmd = payload[0];
    let txn = payload[1];

    let cmd = match raw_cmd {
        0x01 => AppMessageCmd::Push,
        0xFF => AppMessageCmd::Ack,
        0x7F => AppMessageCmd::Nack,
        _ => return None,
    };

    if cmd == AppMessageCmd::Push && payload.len() >= 19 {
        let uuid_bytes: [u8; 16] = payload[2..18].try_into().ok()?;
        let app_uuid = Uuid::from_bytes(uuid_bytes).to_string();
        // payload[18] = count of tuples; tuples start at byte 19
        let data = decode_tuples(&payload[19..]);
        return Some(ParsedAppMessage { cmd, txn, app_uuid: Some(app_uuid), data: Some(data) });
    }

    Some(ParsedAppMessage { cmd, txn, app_uuid: None, data: None })
}

pub fn build_app_message_push(
    transaction_id: u8,
    app_uuid: &str,
    data: &std::collections::HashMap<u32, AppMessageValue>,
) -> Option<Vec<u8>> {
    let uuid_bytes: [u8; 16] = Uuid::parse_str(app_uuid).ok()?.into_bytes();
    let mut body = vec![AppMessageCmd::Push as u8, transaction_id];
    body.extend_from_slice(&uuid_bytes);
    body.push(data.len() as u8);
    for (key, value) in data {
        body.extend_from_slice(&encode_tuple_correct(*key, value));
    }
    Some(body)
}

pub fn build_app_message_ack(transaction_id: u8) -> Vec<u8> {
    vec![AppMessageCmd::Ack as u8, transaction_id]
}
