//! Desktop app-name → Pebble notification category mapping.
//!
//! Add new entries here when a new app needs a specific icon or background
//! colour on the watch. Category → icon/colour mapping lives in
//! `libpebble_ble::endpoints::blob_db`.

use libpebble_ble::NotificationCategory;

pub fn app_name_to_category(app_name: &str) -> NotificationCategory {
    let lower = app_name.to_lowercase();
    let lower = lower.trim();

    if matches!(lower, "thunderbird" | "evolution" | "kmail" | "geary"
        | "mutt" | "neomutt" | "protonmail" | "gmail" | "outlook"
        | "apple mail" | "mail" | "fastmail" | "tutanota")
    {
        return NotificationCategory::Email;
    }
    if lower == "whatsapp" {
        return NotificationCategory::WhatsApp;
    }
    if lower.contains("facebook messenger") || lower == "messenger" {
        return NotificationCategory::FacebookMessenger;
    }
    if lower == "facebook" {
        return NotificationCategory::Facebook;
    }
    if matches!(lower, "twitter" | "tweetbot" | "tweetdeck" | "birdsite") {
        return NotificationCategory::Twitter;
    }
    if lower == "instagram" {
        return NotificationCategory::Instagram;
    }
    if matches!(lower, "hangouts" | "google hangouts") {
        return NotificationCategory::Hangouts;
    }
    if matches!(lower, "signal" | "telegram" | "discord" | "slack"
        | "element" | "fractal" | "nheko" | "fluffychat" | "mattermost"
        | "rocketchat" | "zulip" | "wire" | "viber" | "line"
        | "skype" | "teams" | "microsoft teams" | "google chat"
        | "messages" | "sms" | "kde connect" | "kdeconnect")
    {
        return NotificationCategory::Messaging;
    }
    NotificationCategory::Generic
}
