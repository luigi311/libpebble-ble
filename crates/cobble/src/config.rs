use std::path::{Path, PathBuf};

use anyhow::Context;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct Config {
    #[serde(default)]
    pub address: String,
    #[serde(default = "default_adapter")]
    pub adapter: String,
    #[serde(default)]
    pub verbose: bool,
    pub db: Option<String>,
}

impl Default for Config {
    fn default() -> Self {
        // Match the serde defaults so Config::default() == deserializing "{}".
        Self {
            address: String::new(),
            adapter: default_adapter(),
            verbose: false,
            db: None,
        }
    }
}

fn default_adapter() -> String {
    "hci0".to_string()
}

pub fn default_config_path() -> anyhow::Result<PathBuf> {
    let base = if let Some(p) = std::env::var_os("XDG_CONFIG_HOME").filter(|v| !v.is_empty()) {
        PathBuf::from(p)
    } else if let Some(p) = std::env::var_os("HOME").filter(|v| !v.is_empty()) {
        PathBuf::from(p).join(".config")
    } else {
        anyhow::bail!("neither XDG_CONFIG_HOME nor HOME is set");
    };
    Ok(base.join("cobbled/config.toml"))
}

pub fn default_db_path() -> anyhow::Result<PathBuf> {
    let base = if let Some(p) = std::env::var_os("XDG_DATA_HOME").filter(|v| !v.is_empty()) {
        PathBuf::from(p)
    } else if let Some(p) = std::env::var_os("HOME").filter(|v| !v.is_empty()) {
        PathBuf::from(p).join(".local/share")
    } else {
        anyhow::bail!("neither XDG_DATA_HOME nor HOME is set");
    };
    Ok(base.join("cobbled/cobbled.db"))
}

pub fn load(path: &Path) -> anyhow::Result<Config> {
    let text = std::fs::read_to_string(path)
        .with_context(|| format!("read config {}", path.display()))?;
    toml::from_str(&text).with_context(|| format!("parse config {}", path.display()))
}

pub fn save(path: &Path, cfg: &Config) -> anyhow::Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("create config dir {}", parent.display()))?;
    }
    let text = toml::to_string_pretty(cfg).context("serialise config")?;
    std::fs::write(path, text).with_context(|| format!("write config {}", path.display()))
}
