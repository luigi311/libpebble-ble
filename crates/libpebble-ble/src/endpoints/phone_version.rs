//! PhoneVersion endpoint (17) — capability advertisement to the watch.

const PHONEVERSION_REMOTE_OS_ANDROID: u32 = 0x00000002;
const PHONEVERSION_REMOTE_CAPS_TELEPHONY: u32 = 0x00000010;
const PHONEVERSION_REMOTE_CAPS_SMS: u32 = 0x00000020;
const PHONEVERSION_REMOTE_CAPS_GPS: u32 = 0x00000040;
const PHONEVERSION_REMOTE_CAPS_BTLE: u32 = 0x00000080;
const PROTOCOL_CAPS_APP_RUN_STATE: u64 = 0x0000000000000001;

pub fn build_phone_version_response() -> Vec<u8> {
    let platform_flags = PHONEVERSION_REMOTE_OS_ANDROID
        | PHONEVERSION_REMOTE_CAPS_TELEPHONY
        | PHONEVERSION_REMOTE_CAPS_SMS
        | PHONEVERSION_REMOTE_CAPS_GPS
        | PHONEVERSION_REMOTE_CAPS_BTLE;

    let mut out = vec![0x01u8];
    out.extend_from_slice(&0xFFFF_FFFFu32.to_be_bytes());
    out.extend_from_slice(&0x8000_0000u32.to_be_bytes());
    out.extend_from_slice(&platform_flags.to_be_bytes());
    out.extend_from_slice(&[2u8, 4, 4, 2]); // response_version=2, major=4, minor=4, bugfix=2
    out.extend_from_slice(&PROTOCOL_CAPS_APP_RUN_STATE.to_be_bytes());
    out
}
