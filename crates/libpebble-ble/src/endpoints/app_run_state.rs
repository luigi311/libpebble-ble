//! AppRunState endpoint (52) — launch/stop watchapps, and inbound run-state
//! events the watch pushes when an app opens or closes.

use uuid::Uuid;

use super::uuid_to_bytes;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum AppRunStateCmd {
    Start = 0x01,
    Stop = 0x02,
    Request = 0x03,
}

impl AppRunStateCmd {
    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            0x01 => Some(Self::Start),
            0x02 => Some(Self::Stop),
            0x03 => Some(Self::Request),
            _ => None,
        }
    }
}

pub fn build_app_run_state(cmd: AppRunStateCmd, app_uuid: &str) -> Option<Vec<u8>> {
    let uuid_bytes = uuid_to_bytes(app_uuid)?;
    let mut out = vec![cmd as u8];
    out.extend_from_slice(&uuid_bytes);
    Some(out)
}

/// Parse an inbound AppRunState message (`[cmd][uuid 16B]`) into the command and
/// the app UUID string. The watch sends `Start`/`Stop` as apps open/close.
pub fn parse_app_run_state(payload: &[u8]) -> Option<(AppRunStateCmd, String)> {
    // The frame is exactly [cmd][uuid 16B]; reject anything with trailing bytes.
    if payload.len() != 17 {
        return None;
    }
    let cmd = AppRunStateCmd::from_u8(payload[0])?;
    let uuid_bytes: [u8; 16] = payload[1..17].try_into().ok()?;
    Some((cmd, Uuid::from_bytes(uuid_bytes).to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn app_run_state_round_trips() {
        let uuid = "61b22bc8-1e29-460d-a236-3fe409a439ff";
        let bytes = build_app_run_state(AppRunStateCmd::Start, uuid).expect("builds");
        let (cmd, parsed) = parse_app_run_state(&bytes).expect("parses");
        assert_eq!(cmd, AppRunStateCmd::Start);
        assert_eq!(parsed, uuid);
    }

    #[test]
    fn app_run_state_rejects_malformed() {
        assert!(parse_app_run_state(&[0x01, 0x00]).is_none()); // too short
        assert!(parse_app_run_state(&[0x09; 17]).is_none()); // unknown command
        assert!(parse_app_run_state(&[0x01; 18]).is_none()); // trailing byte
    }
}
