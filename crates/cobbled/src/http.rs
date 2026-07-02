//! Async HTTP GET helper backed by [`reqwest`] with 429 retry handling.

use std::time::Duration;

use tracing::debug;

/// GET a URL and return the response body as a string.
/// On HTTP 429 (rate limit), extracts the `Retry-After` header and retries
/// after the specified delay (up to 3 retries).
pub async fn http_get(url: &str) -> anyhow::Result<String> {
    let mut retries = 0;
    loop {
        let resp = reqwest::get(url)
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?;
        if resp.status().is_success() {
            return resp.text().await.map_err(|e| anyhow::anyhow!("{e}"));
        }
        if resp.status().as_u16() == 429 && retries < 3 {
            retries += 1;
            let delay = retry_after_secs(resp.headers().get("retry-after"));
            debug!("http: 429 rate limited; retrying in {delay}s (attempt {retries}/3)");
            tokio::time::sleep(Duration::from_secs(delay)).await;
            continue;
        }
        return Err(anyhow::anyhow!(
            "HTTP {}: {}",
            resp.status().as_u16(),
            resp.text().await.unwrap_or_default().lines().next().unwrap_or("")
        ));
    }
}

/// Parse a `Retry-After` header value into seconds (delta-seconds format).
fn retry_after_secs(header: Option<&reqwest::header::HeaderValue>) -> u64 {
    let val = match header.and_then(|v| v.to_str().ok()) {
        Some(s) => s,
        None => return 30,
    };
    val.trim().parse::<u64>().unwrap_or(30).min(300)
}
