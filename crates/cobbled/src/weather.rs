//! Weather provider: location → Open-Meteo forecast → Pebble watch.
//!
//! * **Open-Meteo** (free HTTPS API, no key): current + tomorrow forecast.
//! * **Connection-gated**: only fetches when the watch is connected.
//! * **Refresh timer**: fetches immediately on connect, then every 3 hours.

use std::time::Duration;

use tracing::{info, warn};

use crate::{http, location, service::CobbleDaemon};

/// Deterministic 16-byte location key so the watch weather app can identify
/// and update the same location entry across restarts.
fn location_key() -> [u8; 16] {
    let host = hostname::get()
        .ok()
        .and_then(|h| h.into_string().ok())
        .unwrap_or_default();
    let mut key = [0u8; 16];
    let mut hash: u64 = 0xcbf29ce484222325;
    for b in host.bytes() {
        hash ^= b as u64;
        hash = hash.wrapping_mul(0x100000001b3);
    }
    key[..8].copy_from_slice(&hash.to_le_bytes());
    key[8..].copy_from_slice(b"weather\0");
    key
}

pub async fn run_weather(daemon: CobbleDaemon) {
    let key = location_key();
    let mut rx = daemon.watch_connection();
    loop {
        // Wait until connected.
        while !*rx.borrow() {
            let _ = rx.changed().await;
        }
        if let Err(e) = refresh(&daemon, key).await {
            warn!("weather: refresh failed: {e}");
        }
        // Wait for either the 3-hour timer or a disconnect.
        tokio::select! {
            _ = tokio::time::sleep(Duration::from_secs(3 * 3600)) => {}
            _ = rx.changed() => {}
        }
        // Loop back: the `while` above re-verifies connection state
        // before calling refresh again.
    }
}

async fn refresh(daemon: &CobbleDaemon, key: [u8; 16]) -> anyhow::Result<()> {
    let (lat, lon, location_name) = location::get_location(daemon.db()).await?;
    info!("weather: {lat:.4},{lon:.4} ({location_name})");

    let forecast = fetch_forecast(lat, lon).await?;

    let forecast_short = format!(
        "{} {}{}",
        forecast.current_condition,
        forecast.current_temp,
        forecast.temp_unit()
    );

    daemon
        .push_weather(
            key.to_vec(),
            location_name,
            forecast_short,
            forecast.current_temp,
            forecast.current_weather_code,
            forecast.today_high,
            forecast.today_low,
            forecast.tomorrow_weather_code,
            forecast.tomorrow_high,
            forecast.tomorrow_low,
            true,
        )
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;

    Ok(())
}

// ── Open-Meteo ────────────────────────────────────────────────────────────

struct Forecast {
    current_temp: i16,
    current_condition: String,
    current_weather_code: u8,
    today_high: i16,
    today_low: i16,
    tomorrow_weather_code: u8,
    tomorrow_high: i16,
    tomorrow_low: i16,
}

impl Forecast {
    fn temp_unit(&self) -> &'static str { "°F" }
}

async fn fetch_forecast(lat: f64, lon: f64) -> anyhow::Result<Forecast> {
    let url = format!(
        "https://api.open-meteo.com/v1/forecast\
         ?latitude={lat:.4}&longitude={lon:.4}\
         &current=temperature_2m,weather_code\
         &daily=temperature_2m_max,temperature_2m_min,weather_code\
         &forecast_days=2\
         &temperature_unit=fahrenheit"
    );

    let body = http::http_get(&url).await?;
    let json: serde_json::Value = serde_json::from_str(&body)
        .map_err(|e| anyhow::anyhow!("Open-Meteo parse: {e}"))?;

    let current = &json["current"];
    let daily = &json["daily"];

    // Validate response shape before extracting values.
    if current.is_null() || daily.is_null() {
        return Err(anyhow::anyhow!("Open-Meteo: missing current or daily section"));
    }
    if daily["temperature_2m_max"].as_array().map_or(0, |a| a.len()) < 2 {
        return Err(anyhow::anyhow!("Open-Meteo: expected 2 forecast days"));
    }

    let current_temp = current["temperature_2m"].as_f64().map_or(0, |v| v.round() as i16);
    let current_wmo: u16 = current["weather_code"].as_u64().unwrap_or(0) as u16;
    let current_condition = wmo_description(current_wmo).to_string();

    let today_high = daily["temperature_2m_max"][0].as_f64().map_or(0, |v| v.round() as i16);
    let today_low = daily["temperature_2m_min"][0].as_f64().map_or(0, |v| v.round() as i16);

    let tomorrow_high = daily["temperature_2m_max"][1].as_f64().map_or(0, |v| v.round() as i16);
    let tomorrow_low = daily["temperature_2m_min"][1].as_f64().map_or(0, |v| v.round() as i16);
    let tomorrow_wmo: u16 = daily["weather_code"][1].as_u64().unwrap_or(0) as u16;

    Ok(Forecast {
        current_temp,
        current_condition,
        current_weather_code: wmo_to_pebble(current_wmo),
        today_high,
        today_low,
        tomorrow_weather_code: wmo_to_pebble(tomorrow_wmo),
        tomorrow_high,
        tomorrow_low,
    })
}

fn wmo_description(code: u16) -> &'static str {
    match code {
        0 => "Clear",
        1..=3 => "Partly Cloudy",
        45 | 48 => "Fog",
        51 | 53 | 55 => "Drizzle",
        56 | 57 => "Freezing Drizzle",
        61 | 63 | 65 => "Rain",
        66 | 67 => "Freezing Rain",
        71 | 73 | 75 => "Snow",
        77 => "Snow Grains",
        80..=82 => "Rain Showers",
        85 | 86 => "Snow Showers",
        95 => "Thunderstorm",
        96 | 99 => "Hail",
        _ => "Overcast",
    }
}

fn wmo_to_pebble(code: u16) -> u8 {
    match code {
        0 => 7,   // Sun
        1 => 0,   // PartlyCloudy
        2 => 1,   // CloudyDay
        3 => 1,   // CloudyDay
        45 | 48 => 6, // Generic (fog)
        51 | 53 | 55 | 56 | 57 => 3, // LightRain
        61 => 3,  // LightRain
        63 | 65 | 66 | 67 | 80 | 81 | 82 => 4, // HeavyRain
        71 | 73 | 77 | 85 => 2, // LightSnow
        75 | 86 => 5, // HeavySnow
        95 | 96 | 99 => 6, // Generic (thunderstorm)
        8 | 9 => 8, // RainAndSnow (sleet)
        _ => 255, // Unknown
    }
}
