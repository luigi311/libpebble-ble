//! Reset endpoint (2003) — reboot, factory reset, recovery, core dump.
//!
//! Mirrors libpebble3's `packets/Reset.kt`. Each message is a single command
//! byte; the watch acts and (for reboots) drops the connection — no reply.

/// Reset command bytes (libpebble3 `ResetMessage.ResetType`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum ResetCommand {
    /// Reboot the watch.
    Reset = 0x00,
    /// Trigger a core dump.
    CoreDump = 0x01,
    /// Factory reset — wipes the watch. Destructive.
    FactoryReset = 0xfe,
    /// Reboot into recovery (PRF) firmware.
    ResetIntoPrf = 0xff,
}

/// Build a Reset message (endpoint 2003): just the command byte.
pub fn build_reset(command: ResetCommand) -> Vec<u8> {
    vec![command as u8]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reset_command_bytes() {
        assert_eq!(build_reset(ResetCommand::Reset), vec![0x00]);
        assert_eq!(build_reset(ResetCommand::CoreDump), vec![0x01]);
        assert_eq!(build_reset(ResetCommand::FactoryReset), vec![0xfe]);
        assert_eq!(build_reset(ResetCommand::ResetIntoPrf), vec![0xff]);
    }
}
