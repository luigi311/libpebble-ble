//! BlueZ pairing agent that auto-accepts the Pebble's bonding requests.
//!
//! Registers as the default agent for the duration of pairing so BlueZ routes
//! watch-initiated bonding to us. Only accepts requests from the configured
//! watch address; rejects everything else.

use bluer::{
    agent::{Agent, ReqError, RequestConfirmation},
    Address,
};
use tracing::{debug, info, warn};

/// Build a bluer `Agent` that auto-confirms pairing for `watch_address` only.
pub fn build_pairing_agent(watch_address: Address) -> Agent {
    Agent {
        request_default: true,

        request_confirmation: Some(Box::new(move |req: RequestConfirmation| {
            let addr = watch_address;
            Box::pin(async move {
                if req.device == addr {
                    debug!("agent auto-confirming passkey {:06} for {}", req.passkey, req.device);
                    Ok(())
                } else {
                    warn!("agent rejecting confirmation for unexpected device {}", req.device);
                    Err(ReqError::Rejected)
                }
            })
        })),

        request_authorization: Some(Box::new(move |req| {
            let addr = watch_address;
            Box::pin(async move {
                if req.device == addr {
                    debug!("agent auto-authorizing {}", req.device);
                    Ok(())
                } else {
                    Err(ReqError::Rejected)
                }
            })
        })),

        authorize_service: Some(Box::new(move |req| {
            let addr = watch_address;
            Box::pin(async move {
                if req.device == addr {
                    debug!("agent auto-authorizing service {} for {}", req.service, req.device);
                    Ok(())
                } else {
                    Err(ReqError::Rejected)
                }
            })
        })),

        display_passkey: Some(Box::new(|req| {
            Box::pin(async move {
                info!("pairing passkey for {}: {:06}", req.device, req.passkey);
                Ok(())
            })
        })),

        request_passkey: Some(Box::new(move |req| {
            let addr = watch_address;
            Box::pin(async move {
                if req.device == addr {
                    Ok(0u32)
                } else {
                    Err(ReqError::Rejected)
                }
            })
        })),

        ..Default::default()
    }
}
