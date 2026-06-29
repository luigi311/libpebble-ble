//! Music control endpoint (32) — mirrors libpebble3's `packets/Music.kt`.
//!
//! Outbound (phone → watch) "Update*" messages push the now-playing player,
//! track, playback state and volume. Inbound single-byte commands (play/pause/
//! next/…) are the watch's media-key presses, surfaced as [`MusicAction`].

// Phone → watch update commands.
const CMD_UPDATE_CURRENT_TRACK: u8 = 0x10;
const CMD_UPDATE_PLAY_STATE: u8 = 0x11;
const CMD_UPDATE_VOLUME: u8 = 0x12;
const CMD_UPDATE_PLAYER_INFO: u8 = 0x13;

/// Playback state (libpebble3 `MusicControl.PlaybackState`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum MusicPlaybackState {
    Paused = 0,
    Playing = 1,
    Rewinding = 2,
    FastForwarding = 3,
    Unknown = 4,
}

impl MusicPlaybackState {
    pub fn from_u8(v: u8) -> Self {
        match v {
            0 => Self::Paused,
            1 => Self::Playing,
            2 => Self::Rewinding,
            3 => Self::FastForwarding,
            _ => Self::Unknown,
        }
    }
}

/// Shuffle state (libpebble3 `MusicControl.ShuffleState`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum MusicShuffle {
    Unknown = 0,
    Off = 1,
    On = 2,
}

impl MusicShuffle {
    pub fn from_u8(v: u8) -> Self {
        match v {
            1 => Self::Off,
            2 => Self::On,
            _ => Self::Unknown,
        }
    }
}

/// Repeat state (libpebble3 `MusicControl.RepeatState`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum MusicRepeat {
    Unknown = 0,
    Off = 1,
    One = 2,
    All = 3,
}

impl MusicRepeat {
    pub fn from_u8(v: u8) -> Self {
        match v {
            1 => Self::Off,
            2 => Self::One,
            3 => Self::All,
            _ => Self::Unknown,
        }
    }
}

/// A media-control action the watch sent (libpebble3 `MusicAction`, plus
/// `GetCurrentTrack` — the watch asking the phone to re-send the now-playing).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MusicAction {
    PlayPause,
    Pause,
    Play,
    NextTrack,
    PreviousTrack,
    VolumeUp,
    VolumeDown,
    GetCurrentTrack,
}

impl MusicAction {
    /// Stable lowercase name, e.g. for surfacing over D-Bus.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::PlayPause => "play_pause",
            Self::Pause => "pause",
            Self::Play => "play",
            Self::NextTrack => "next_track",
            Self::PreviousTrack => "previous_track",
            Self::VolumeUp => "volume_up",
            Self::VolumeDown => "volume_down",
            Self::GetCurrentTrack => "get_current_track",
        }
    }
}

/// Parse an inbound music-control message into the action the watch requested.
/// Returns `None` for the phone→watch `Update*` commands or unknown bytes.
pub fn parse_music_command(payload: &[u8]) -> Option<MusicAction> {
    Some(match *payload.first()? {
        0x01 => MusicAction::PlayPause,
        0x02 => MusicAction::Pause,
        0x03 => MusicAction::Play,
        0x04 => MusicAction::NextTrack,
        0x05 => MusicAction::PreviousTrack,
        0x06 => MusicAction::VolumeUp,
        0x07 => MusicAction::VolumeDown,
        0x08 => MusicAction::GetCurrentTrack,
        _ => return None,
    })
}

/// Append a Pascal string (`[len u8][utf-8 bytes]`), truncated to 255 bytes on
/// a char boundary.
fn push_sstring(out: &mut Vec<u8>, s: &str) {
    let bytes = if s.len() > 255 {
        let mut end = 255;
        while !s.is_char_boundary(end) {
            end -= 1;
        }
        &s.as_bytes()[..end]
    } else {
        s.as_bytes()
    };
    out.push(bytes.len() as u8);
    out.extend_from_slice(bytes);
}

/// Build an `UpdatePlayerInfo` message (which media app is playing).
pub fn build_update_player_info(pkg: &str, name: &str) -> Vec<u8> {
    let mut out = vec![CMD_UPDATE_PLAYER_INFO];
    push_sstring(&mut out, pkg);
    push_sstring(&mut out, name);
    out
}

/// Build an `UpdateCurrentTrack` message. The three extended fields are written
/// as a contiguous prefix (decode is by remaining length), so a `None` drops it
/// and everything after — pass all three or none.
pub fn build_update_current_track(
    artist: &str,
    album: &str,
    title: &str,
    track_length_ms: Option<u32>,
    track_count: Option<u32>,
    current_track: Option<u32>,
) -> Vec<u8> {
    let mut out = vec![CMD_UPDATE_CURRENT_TRACK];
    push_sstring(&mut out, artist);
    push_sstring(&mut out, album);
    push_sstring(&mut out, title);
    for field in [track_length_ms, track_count, current_track] {
        match field {
            Some(v) => out.extend_from_slice(&v.to_le_bytes()),
            None => break,
        }
    }
    out
}

/// Build an `UpdatePlayStateInfo` message.
pub fn build_update_play_state(
    state: MusicPlaybackState,
    track_position_ms: u32,
    play_rate_pct: u32,
    shuffle: MusicShuffle,
    repeat: MusicRepeat,
) -> Vec<u8> {
    let mut out = vec![CMD_UPDATE_PLAY_STATE, state as u8];
    out.extend_from_slice(&track_position_ms.to_le_bytes());
    out.extend_from_slice(&play_rate_pct.to_le_bytes());
    out.push(shuffle as u8);
    out.push(repeat as u8);
    out
}

/// Build an `UpdateVolumeInfo` message (0–100).
pub fn build_update_volume(volume_percent: u8) -> Vec<u8> {
    vec![CMD_UPDATE_VOLUME, volume_percent]
}

#[cfg(test)]
mod tests {
    use super::*;

    // Byte fixtures from libpebble3 MusicTest.kt (payload only, i.e. after the
    // 4-byte Pebble Protocol header).
    #[test]
    fn update_current_track_with_optionals() {
        let p = build_update_current_track("A", "B", "C", Some(10), Some(20), Some(30));
        assert_eq!(
            p,
            vec![0x10, 1, 65, 1, 66, 1, 67, 10, 0, 0, 0, 20, 0, 0, 0, 30, 0, 0, 0],
        );
    }

    #[test]
    fn update_current_track_without_optionals() {
        let p = build_update_current_track("A", "B", "C", None, None, None);
        assert_eq!(p, vec![0x10, 1, 65, 1, 66, 1, 67]);
    }

    #[test]
    fn play_state_layout() {
        let p = build_update_play_state(
            MusicPlaybackState::Playing,
            30_000,
            100,
            MusicShuffle::Off,
            MusicRepeat::Off,
        );
        // cmd, state=1, pos LE, rate LE, shuffle=1, repeat=1
        assert_eq!(p, vec![0x11, 1, 0x30, 0x75, 0, 0, 100, 0, 0, 0, 1, 1]);
    }

    #[test]
    fn player_info_and_volume() {
        assert_eq!(
            build_update_player_info("a", "Name"),
            vec![0x13, 1, b'a', 4, b'N', b'a', b'm', b'e'],
        );
        assert_eq!(build_update_volume(75), vec![0x12, 75]);
    }

    #[test]
    fn parse_inbound_actions() {
        assert_eq!(parse_music_command(&[0x03]), Some(MusicAction::Play));
        assert_eq!(parse_music_command(&[0x08]), Some(MusicAction::GetCurrentTrack));
        assert_eq!(parse_music_command(&[0x10]), None); // an Update* command
        assert_eq!(parse_music_command(&[]), None);
        assert_eq!(MusicAction::PreviousTrack.as_str(), "previous_track");
    }

    #[test]
    fn long_string_is_truncated() {
        let p = build_update_player_info(&"x".repeat(300), "");
        assert_eq!(p[1], 255); // length byte capped
        assert_eq!(p.len(), 1 + 1 + 255 + 1); // cmd + len + 255 + name len(0)
    }
}
