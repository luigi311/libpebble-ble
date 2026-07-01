use std::path::{Path, PathBuf};

use anyhow::Context;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct Config {
    /// Watch Bluetooth address, e.g. E6:94:0A:D4:D5:DC
    #[serde(default)]
    pub address: String,
    /// HCI adapter name
    #[serde(default = "default_adapter")]
    pub adapter: String,
    /// Enable verbose (TRACE-level) logging
    #[serde(default)]
    pub verbose: bool,
    /// Path to the health data SQLite database
    pub db: Option<PathBuf>,
}

fn default_adapter() -> String {
    "hci0".to_string()
}

/// Returns `$XDG_CONFIG_HOME/cobbled/config.toml` or
/// `~/.config/cobbled/config.toml` as a fallback.
pub fn default_config_path() -> anyhow::Result<PathBuf> {
    let base = if let Some(p) = std::env::var_os("XDG_CONFIG_HOME").filter(|v| !v.is_empty()) {
        PathBuf::from(p)
    } else if let Some(p) = std::env::var_os("HOME").filter(|v| !v.is_empty()) {
        PathBuf::from(p).join(".config")
    } else {
        anyhow::bail!(
            "neither XDG_CONFIG_HOME nor HOME is set; \
             use --config to specify the config file path explicitly"
        );
    };
    Ok(base.join("cobbled/config.toml"))
}

pub fn load(path: &Path) -> anyhow::Result<Config> {
    match std::fs::read_to_string(path) {
        Ok(text) => toml::from_str(&text)
            .with_context(|| format!("parse config file {}", path.display())),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            // Return a default config so the daemon can start without a
            // pre-existing config file. The GUI (or manual editing) can
            // supply a watch address later; reload_config will pick it up.
            Ok(Config {
                address: String::new(),
                adapter: default_adapter(),
                verbose: false,
                db: None,
            })
        }
        Err(e) => Err(e).with_context(|| format!("read config file {}", path.display())),
    }
}
