//! AppRunState endpoint (52) — launch/stop watchapps.

use super::uuid_to_bytes;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum AppRunStateCmd {
    Start = 0x01,
    Stop = 0x02,
    Request = 0x03,
}

pub fn build_app_run_state(cmd: AppRunStateCmd, app_uuid: &str) -> Option<Vec<u8>> {
    let uuid_bytes = uuid_to_bytes(app_uuid)?;
    let mut out = vec![cmd as u8];
    out.extend_from_slice(&uuid_bytes);
    Some(out)
}
