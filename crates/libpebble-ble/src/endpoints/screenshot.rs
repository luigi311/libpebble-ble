//! Screenshot endpoint (8000) — capture the watch framebuffer.
//!
//! Mirrors libpebble3's `packets/Screenshot.kt` + `services/ScreenshotService.kt`.
//! Request is a single command byte; the watch replies with a header
//! `[code][version u32 BE][width u32 BE][height u32 BE]` followed by the raw
//! framebuffer, split across multiple endpoint messages. `version` selects the
//! pixel format: 1 = 1-bit black/white, 2 = 8-bit Pebble color (2-bit per
//! channel). Bytes accumulate until `width * height * bits_per_pixel / 8`.

/// Build a screenshot request (command 0 = take screenshot — the only one).
pub fn build_screenshot_request() -> Vec<u8> {
    vec![0x00]
}

/// Screenshot response status (libpebble3 `ScreenshotResponseCode`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScreenshotResponseCode {
    Ok,
    MalformedCommand,
    OutOfMemory,
    AlreadyInProgress,
}

impl ScreenshotResponseCode {
    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            0 => Some(Self::Ok),
            1 => Some(Self::MalformedCommand),
            2 => Some(Self::OutOfMemory),
            3 => Some(Self::AlreadyInProgress),
            _ => None,
        }
    }
}

/// Framebuffer pixel format (libpebble3 `ScreenshotVersion`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScreenshotVersion {
    BlackWhite1Bit,
    Color8Bit,
}

impl ScreenshotVersion {
    pub fn from_u32(v: u32) -> Option<Self> {
        match v {
            1 => Some(Self::BlackWhite1Bit),
            2 => Some(Self::Color8Bit),
            _ => None,
        }
    }
    pub fn bits_per_pixel(self) -> usize {
        match self {
            Self::BlackWhite1Bit => 1,
            Self::Color8Bit => 8,
        }
    }
}

/// Parsed screenshot response header (first 13 bytes of the first message).
#[derive(Debug, Clone, Copy)]
pub struct ScreenshotHeader {
    pub response_code: ScreenshotResponseCode,
    /// `None` if the watch reported an unrecognised version (only meaningful
    /// when `response_code` is `Ok`).
    pub version: Option<ScreenshotVersion>,
    pub width: u32,
    pub height: u32,
}

/// Parse the screenshot header from the first response message, returning the
/// header and the image bytes that follow it. `None` if too short or the
/// response code is unrecognised.
pub fn parse_screenshot_header(payload: &[u8]) -> Option<(ScreenshotHeader, &[u8])> {
    if payload.len() < 13 {
        return None;
    }
    let response_code = ScreenshotResponseCode::from_u8(payload[0])?;
    let version = ScreenshotVersion::from_u32(u32::from_be_bytes(payload[1..5].try_into().ok()?));
    let width = u32::from_be_bytes(payload[5..9].try_into().ok()?);
    let height = u32::from_be_bytes(payload[9..13].try_into().ok()?);
    Some((
        ScreenshotHeader { response_code, version, width, height },
        &payload[13..],
    ))
}

/// Largest watch dimension we accept. Real Pebbles top out around 200×228;
/// this leaves margin while rejecting garbage/oversized headers that would
/// otherwise drive a huge allocation.
pub const MAX_SCREEN_DIM: u32 = 1024;

/// Validate dimensions and return the expected framebuffer byte count (rows are
/// byte-aligned). `None` if a dimension is zero or exceeds [`MAX_SCREEN_DIM`].
pub fn expected_size(version: ScreenshotVersion, width: u32, height: u32) -> Option<usize> {
    if width == 0 || height == 0 || width > MAX_SCREEN_DIM || height > MAX_SCREEN_DIM {
        return None;
    }
    let row_bytes = ((width as usize) * version.bits_per_pixel()).div_ceil(8);
    row_bytes.checked_mul(height as usize)
}

/// Decode a complete framebuffer into row-major RGBA8888 pixels
/// (`width * height * 4` bytes). Missing trailing bytes decode as black.
/// Returns an empty vec if the dimensions fail [`expected_size`] validation.
pub fn decode_to_rgba(version: ScreenshotVersion, width: u32, height: u32, data: &[u8]) -> Vec<u8> {
    if expected_size(version, width, height).is_none() {
        return Vec::new();
    }
    let w = width as usize;
    let h = height as usize;
    // Safe: dimensions validated above (each ≤ MAX_SCREEN_DIM).
    let mut rgba = vec![0u8; w * h * 4];
    match version {
        ScreenshotVersion::BlackWhite1Bit => {
            let stride = w.div_ceil(8); // byte-aligned 1-bit rows
            for y in 0..h {
                for x in 0..w {
                    let bit = data
                        .get(y * stride + x / 8)
                        .map(|b| (b >> (x % 8)) & 1)
                        .unwrap_or(0);
                    let v = if bit == 0 { 0 } else { 255 };
                    let p = (y * w + x) * 4;
                    rgba[p] = v;
                    rgba[p + 1] = v;
                    rgba[p + 2] = v;
                    rgba[p + 3] = 255;
                }
            }
        }
        ScreenshotVersion::Color8Bit => {
            // Each byte is a Pebble color: bits [5:4]=R, [3:2]=G, [1:0]=B
            // (top two bits are alpha, ignored — screenshots are opaque).
            // Each 2-bit channel scales to 8-bit by ×85 (3 → 255).
            for i in 0..(w * h) {
                let cb = data.get(i).copied().unwrap_or(0);
                let p = i * 4;
                rgba[p] = ((cb >> 4) & 0x03) * 85;
                rgba[p + 1] = ((cb >> 2) & 0x03) * 85;
                rgba[p + 2] = (cb & 0x03) * 85;
                rgba[p + 3] = 255;
            }
        }
    }
    rgba
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn request_is_command_zero() {
        assert_eq!(build_screenshot_request(), vec![0x00]);
    }

    #[test]
    fn parse_header_and_trailing_data() {
        let mut p = vec![0u8]; // response code OK
        p.extend_from_slice(&2u32.to_be_bytes()); // version = color
        p.extend_from_slice(&144u32.to_be_bytes()); // width
        p.extend_from_slice(&168u32.to_be_bytes()); // height
        p.extend_from_slice(&[0xAA, 0xBB]); // first image bytes
        let (h, data) = parse_screenshot_header(&p).expect("parses");
        assert_eq!(h.response_code, ScreenshotResponseCode::Ok);
        assert_eq!(h.version, Some(ScreenshotVersion::Color8Bit));
        assert_eq!((h.width, h.height), (144, 168));
        assert_eq!(data, &[0xAA, 0xBB]);
        assert_eq!(expected_size(ScreenshotVersion::Color8Bit, 144, 168), Some(144 * 168));
        assert!(parse_screenshot_header(&[0, 0, 0]).is_none());
    }

    #[test]
    fn rejects_invalid_geometry() {
        assert_eq!(expected_size(ScreenshotVersion::Color8Bit, 0, 168), None);
        assert_eq!(expected_size(ScreenshotVersion::Color8Bit, 5000, 168), None);
        assert_eq!(expected_size(ScreenshotVersion::BlackWhite1Bit, 144, 99999), None);
        // 1-bit row is byte-aligned: 18 bytes/row * 168 rows.
        assert_eq!(expected_size(ScreenshotVersion::BlackWhite1Bit, 144, 168), Some(18 * 168));
        assert!(decode_to_rgba(ScreenshotVersion::Color8Bit, 5000, 5000, &[]).is_empty());
    }

    #[test]
    fn decode_1bit() {
        // 8x1: only bit 0 set -> first pixel white, rest black.
        let rgba = decode_to_rgba(ScreenshotVersion::BlackWhite1Bit, 8, 1, &[0b0000_0001]);
        assert_eq!(&rgba[0..4], &[255, 255, 255, 255]); // x=0 white
        assert_eq!(&rgba[4..8], &[0, 0, 0, 255]); // x=1 black
    }

    #[test]
    fn decode_8bit_color() {
        // 0x3F -> R=G=B=3 -> white; 0xC0 -> all channels 0 (alpha bits ignored) -> black.
        let rgba = decode_to_rgba(ScreenshotVersion::Color8Bit, 2, 1, &[0x3F, 0xC0]);
        assert_eq!(&rgba[0..4], &[255, 255, 255, 255]);
        assert_eq!(&rgba[4..8], &[0, 0, 0, 255]);
    }

    #[test]
    fn decode_tolerates_truncation() {
        // Missing data -> black pixels, no panic.
        let rgba = decode_to_rgba(ScreenshotVersion::Color8Bit, 4, 4, &[]);
        assert_eq!(rgba.len(), 4 * 4 * 4);
        assert!(rgba.chunks(4).all(|px| px == [0, 0, 0, 255]));
    }
}
