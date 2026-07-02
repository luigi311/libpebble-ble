//! Location acquisition: GeoClue2 (GPS) → IP geolocation fallback.
//!
//! * **GeoClue2** (session D-Bus): accurate GPS when the daemon is installed
//!   and the user has granted location permission.
//! * **ipapi.co** (free HTTPS API, no key): city-level IP geolocation.
//! * **Nominatim** (OpenStreetMap): reverse geocodes coordinates to a city name.

use std::time::Duration;

use tracing::info;

use crate::http;

/// Get coordinates and a human-readable city/region name.
///
/// Tries GeoClue2 first; falls back to IP-based geolocation if GeoClue2 is
/// not available.
pub async fn get_location() -> anyhow::Result<(f64, f64, String)> {
    if let Ok((lat, lon, name)) = try_geoclue().await {
        return Ok((lat, lon, name));
    }
    try_ip_geolocation().await
}

async fn try_geoclue() -> anyhow::Result<(f64, f64, String)> {
    let conn = zbus::Connection::session().await?;

    // Create a GeoClue2 client.
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

    // Start the client so it begins acquiring location.
    let _ = conn
        .call_method(
            Some("org.freedesktop.GeoClue2"),
            client_path.as_str(),
            Some("org.freedesktop.GeoClue2.Client"),
            "Start",
            &(),
        )
        .await;

    // Wait up to 5 seconds for a location fix.
    let lat = get_prop_f64(&conn, client_path.as_str(), "org.freedesktop.GeoClue2.Client", "Latitude", Duration::from_secs(5)).await?;
    let lon = get_prop_f64(&conn, client_path.as_str(), "org.freedesktop.GeoClue2.Client", "Longitude", Duration::from_secs(5)).await?;

    // Stop the client — we only need a one-shot fix.
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

/// Read a floating-point D-Bus property with a timeout.
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

// ── IP Geolocation fallback ───────────────────────────────────────────────

/// Use ipapi.co (free, no API key) to get a rough location from the public IP.
async fn try_ip_geolocation() -> anyhow::Result<(f64, f64, String)> {
    let body = http::http_get("https://ipapi.co/json/").await?;
    let json: serde_json::Value = serde_json::from_str(&body)
        .map_err(|e| anyhow::anyhow!("ipapi parse: {e}"))?;

    let lat = json["latitude"].as_f64().ok_or_else(|| anyhow::anyhow!("ipapi: missing latitude"))?;
    let lon = json["longitude"].as_f64().ok_or_else(|| anyhow::anyhow!("ipapi: missing longitude"))?;

    let city = json["city"].as_str().unwrap_or("");
    let name = if !city.is_empty() {
        city.to_string()
    } else {
        "Current Location".to_string()
    };

    info!("weather: IP-based location {lat:.4},{lon:.4} ({name})");
    Ok((lat, lon, name))
}

// ── Nominatim reverse geocoding ───────────────────────────────────────────

/// Turn coordinates into a city/region name via OpenStreetMap Nominatim.
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
