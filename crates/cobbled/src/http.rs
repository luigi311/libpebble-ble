//! Async HTTP GET helper backed by [`reqwest`] with 429 retry handling.

use std::sync::LazyLock;
use std::time::Duration;

use tracing::{debug, trace, warn};

static CLIENT: LazyLock<reqwest::Client> = LazyLock::new(|| {
    reqwest::Client::builder()
        .user_agent("cobbled/0.5")
        .timeout(Duration::from_secs(15))
        .build()
        .expect("reqwest client")
});

/// GET a URL and return the response body as a string.
/// On HTTP 429 (rate limit), extracts the `Retry-After` header and retries
/// after the specified delay (up to 3 retries).
pub async fn http_get(url: &str) -> anyhow::Result<String> {
    let mut retries = 0;
    loop {
        let resp = CLIENT
            .get(url)
            .send()
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?;
        if resp.status().is_success() {
            let body = resp.text().await.map_err(|e| anyhow::anyhow!("{e}"))?;
            trace!("http: GET {} -> {} bytes", url, body.len());
            return Ok(body);
        }
        if resp.status().as_u16() == 429 && retries < 3 {
            retries += 1;
            let delay = retry_after_secs(resp.headers().get("retry-after"));
            debug!("http: 429 rate limited; retrying in {delay}s (attempt {retries}/3)");
            tokio::time::sleep(Duration::from_secs(delay)).await;
            continue;
        }
        let status = resp.status().as_u16();
        let body_preview = resp.text().await.unwrap_or_default();
        let preview = body_preview.lines().next().unwrap_or("");
        warn!("http: GET {url} -> HTTP {status}: {preview}");
        return Err(anyhow::anyhow!("HTTP {status}: {preview}"));
    }
}

/// GET a URL and return the response body trimmed (for plain-text endpoints).
pub async fn http_get_text(url: &str) -> anyhow::Result<String> {
    Ok(http_get(url).await?.trim().to_string())
}

/// Parse a `Retry-After` header value into seconds (delta-seconds format).
fn retry_after_secs(header: Option<&reqwest::header::HeaderValue>) -> u64 {
    let val = match header.and_then(|v| v.to_str().ok()) {
        Some(s) => s,
        None => return 30,
    };
    val.trim().parse::<u64>().unwrap_or(30).min(300)
}
