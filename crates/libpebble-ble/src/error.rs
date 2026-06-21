use thiserror::Error;

#[derive(Error, Debug)]
pub enum PebbleError {
    #[error("BLE error: {0}")]
    Ble(#[from] bluer::Error),
    #[error("not connected")]
    NotConnected,
    #[error("watch NACKed transaction {0}")]
    Nack(u8),
    #[error("timeout: {0}")]
    Timeout(String),
    #[error("pairing failed: {0}")]
    PairingFailed(String),
    #[error("{0}")]
    Other(String),
}
