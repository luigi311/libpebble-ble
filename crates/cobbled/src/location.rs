//! Location acquisition: GeoClue2 (GPS) → IP geolocation with DB cache.
//!
//! * **GeoClue2** (session D-Bus): accurate GPS when the daemon is installed.
//! * **ifconfig.me**: gets the current public IP address.
//! * **ipapi.co** (free HTTPS API): city-level IP geolocation, cached in the
//!   database to avoid rate limits.
//! * **Nominatim** (OpenStreetMap): reverse geocodes coordinates to a city name.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use tracing::debug;

use crate::db::{AppDb, IpLocation};
use crate::http;

/// Get coordinates and a human-readable city name.
///
/// Tries GeoClue2 first.  Falls back to IP-based geolocation (cached in the
/// database, fetched from ipapi.co only if not already cached for the current
/// IP).
pub async fn get_location(db: Option<Arc<Mutex<AppDb>>>) -> anyhow::Result<(f64, f64, String)> {
    if let Ok((lat, lon, name)) = try_geoclue().await {
        return Ok((lat, lon, name));
    }
    debug!("GeoClue2 unavailable; falling back to IP geolocation");
    try_ip_geolocation(db).await
}

async fn try_geoclue() -> anyhow::Result<(f64, f64, String)> {
    let conn = zbus::Connection::session().await?;

    let reply = conn
        .call_method(
            Some("org.freedesktop.GeoClue2"),
            "/org/freedesktop/GeoClue2/Manager",
            Some("org.freedesktop.GeoClue2.Manager"),
            "GetClient",
            &(),
        )
        .await
        .map_err(|e| anyhow::anyhow!("GeoClue2 GetClient: {e}"))?;
    let client_path: zbus::zvariant::OwnedObjectPath = reply.body().deserialize()?;

    conn
        .call_method(
            Some("org.freedesktop.GeoClue2"),
            client_path.as_str(),
            Some("org.freedesktop.GeoClue2.Client"),
            "Start",
            &(),
        )
        .await
        .map_err(|e| anyhow::anyhow!("GeoClue2 Start: {e}"))?;

    let lat = get_prop_f64(&conn, client_path.as_str(), "org.freedesktop.GeoClue2.Client", "Latitude", Duration::from_secs(5)).await?;
    let lon = get_prop_f64(&conn, client_path.as_str(), "org.freedesktop.GeoClue2.Client", "Longitude", Duration::from_secs(5)).await?;

    let _ = conn
        .call_method(
            Some("org.freedesktop.GeoClue2"),
            client_path.as_str(),
            Some("org.freedesktop.GeoClue2.Client"),
            "Stop",
            &(),
        )
        .await;

    let name = reverse_geocode(lat, lon).await?;
    Ok((lat, lon, name))
}

async fn get_prop_f64(
    conn: &zbus::Connection,
    path: &str,
    iface: &str,
    prop: &str,
    timeout: Duration,
) -> anyhow::Result<f64> {
    let reply = tokio::time::timeout(timeout, conn.call_method(
        Some("org.freedesktop.GeoClue2"),
        path,
        Some("org.freedesktop.DBus.Properties"),
        "Get",
        &(iface, prop),
    ))
    .await
    .map_err(|_| anyhow::anyhow!("GeoClue2 {prop} read timed out"))?
    .map_err(|e| anyhow::anyhow!("GeoClue2 {prop}: {e}"))?;

    let body = reply.body();
    let v: zbus::zvariant::Value<'_> = body.deserialize()?;
    let ov = zbus::zvariant::OwnedValue::try_from(v)?;
    let val: f64 = ov.try_into()?;
    Ok(val)
}

// ── IP geolocation (with DB cache) ──────────────────────────────────────

async fn try_ip_geolocation(db: Option<Arc<Mutex<AppDb>>>) -> anyhow::Result<(f64, f64, String)> {
    // 1. Get current public IP.  ifconfig.me/ip returns bare-IP plaintext;
    // the root path now returns an HTML page since mid-2026.
    let ip = match http::http_get_text("https://ifconfig.me/ip").await {
        Ok(ip) if looks_like_ip(&ip) => ip,
        Ok(raw) => {
            debug!("ifconfig.me returned non-IP response ({raw:?}); using ipapi directly");
            return fetch_ipapi_and_build().await;
        }
        Err(_) => {
            // Can't get IP — fall back to uncached ipapi if DB is available,
            // or just call ipapi directly.
            return fetch_ipapi_and_build().await;
        }
    };

    // 2. Check the database cache.
    if let Some(ref db) = db
        && let Some(loc) = db.lock().unwrap().lookup_ip_location(&ip)
    {
        let name = location_name(&loc.city);
        debug!("weather: cached IP location ({name})");
        return Ok((loc.latitude, loc.longitude, name));
    }

    // 3. Not cached — fetch from ipapi.co.
    debug!("weather: IP location not cached; querying ipapi.co");
    let (lat, lon, city, region) = fetch_ipapi_raw().await?;
    let name = location_name(&city);

    // 4. Store in cache if DB is available.
    if let Some(ref db) = db {
        let loc = IpLocation { latitude: lat, longitude: lon, city, region };
        if let Err(e) = db.lock().unwrap().store_ip_location(&ip, &loc) {
            tracing::warn!("weather: failed to cache IP location: {e}");
        }
    }

    Ok((lat, lon, name))
}

/// Fetch raw data from ipapi.co.  Returns (lat, lon, city, region).
async fn fetch_ipapi_raw() -> anyhow::Result<(f64, f64, String, String)> {
    let body = http::http_get("https://ipapi.co/json/").await?;
    let json: serde_json::Value = serde_json::from_str(&body)
        .map_err(|e| anyhow::anyhow!("ipapi parse: {e}"))?;

    let lat = json["latitude"].as_f64().ok_or_else(|| anyhow::anyhow!("ipapi: missing latitude"))?;
    let lon = json["longitude"].as_f64().ok_or_else(|| anyhow::anyhow!("ipapi: missing longitude"))?;
    let city = json["city"].as_str().unwrap_or("").to_string();
    let region = json["region"].as_str().unwrap_or("").to_string();

    Ok((lat, lon, city, region))
}

/// Fetch from ipapi and build the location result directly (fallback when
/// we can't get our current IP).
async fn fetch_ipapi_and_build() -> anyhow::Result<(f64, f64, String)> {
    let (lat, lon, city, _) = fetch_ipapi_raw().await?;
    Ok((lat, lon, location_name(&city)))
}

// ── Helpers ─────────────────────────────────────────────────────────────

fn location_name(city: &str) -> String {
    if city.is_empty() { "Current Location".into() } else { city.to_string() }
}

/// Quick validation: the response should look like an IP address, not an HTML page.
fn looks_like_ip(s: &str) -> bool {
    !s.is_empty() && !s.contains('<') && s.chars().all(|c| c.is_ascii_digit() || c == '.' || c == ':')
}

// ── Nominatim reverse geocoding ─────────────────────────────────────────

async fn reverse_geocode(lat: f64, lon: f64) -> anyhow::Result<String> {
    let url = format!(
        "https://nominatim.openstreetmap.org/reverse?lat={lat:.6}&lon={lon:.6}&format=json&zoom=10"
    );
    let body = http::http_get(&url).await?;
    let json: serde_json::Value = serde_json::from_str(&body)?;
    let address = &json["address"];

    let city = address["city"].as_str()
        .or_else(|| address["town"].as_str())
        .or_else(|| address["village"].as_str())
        .unwrap_or("");

    if !city.is_empty() {
        Ok(city.to_string())
    } else {
        Ok("Current Location".to_string())
    }
}
