//! AutoFuelPrice — Bangchak fuel price watcher.
//!
//! Pulls retail fuel prices from Bangchak's public API twice daily
//! (18:00 and 20:00 Asia/Bangkok), compares each fuel type's yesterday
//! vs today price, and pushes a Thai LINE notification when any price
//! changed. Duplicate notifications for the same `OilPriceDate` are
//! suppressed via a small on-disk state file.

use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use chrono_tz::Asia::Bangkok;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use tokio_cron_scheduler::{Job, JobScheduler};
use tracing::{error, info, warn};
use tracing_subscriber::EnvFilter;

// ---- Constants ---------------------------------------------------------------

/// Bangchak's public JSON endpoint (English oil price list).
const BANGCHAK_API_URL: &str = "https://oil-price.bangchak.co.th/apioilprice2/en";

/// HTTP client user-agent identifier sent to Bangchak.
const HTTP_USER_AGENT: &str = "auto-fuel-price-bot/0.1 (+https://github.com/)";

/// Per-request HTTP timeout for upstream calls.
const HTTP_TIMEOUT: Duration = Duration::from_secs(15);

/// Line Messaging API push endpoint.
const LINE_PUSH_URL: &str = "https://api.line.me/v2/bot/message/push";

/// Cron schedule: 18:00 and 20:00 Asia/Bangkok, every day.
///
/// `tokio_cron_scheduler` evaluates cron in UTC. Bangkok is UTC+7 with no
/// DST, so 18:00/20:00 local map to 11:00/13:00 UTC.
const SCHEDULE_UTC_CRON: &str = "0 0 11,13 * * *";

/// Filesystem path used to persist the most recent price snapshot.
const STATE_FILE_NAME: &str = "last_fuel_prices.json";

/// Minimum delta (in Baht) to treat as a real price change.
/// Fuel prices are quoted to 2 decimals (satang); anything below this
/// threshold is floating-point noise.
const PRICE_CHANGE_THRESHOLD: f64 = 0.01;

/// Thai-language LINE message header.
const MESSAGE_HEADER: &str = "📰 แจ้งข่าวราคาน้ำมัน!!";

/// Thai word for a price decrease.
const DIRECTION_DECREASE: &str = "ลด";

/// Thai word for a price increase.
const DIRECTION_INCREASE: &str = "เพิ่ม";

// ---- Domain types ------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct FuelEntry {
    name: String,
    price_yesterday: f64,
    price_today: f64,
    price_tomorrow: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PriceSnapshot {
    /// ISO-8601 timestamp of when the snapshot was written.
    captured_at: String,
    /// `OilPriceDate` returned by Bangchak (dd/MM/yyyy). Used to detect
    /// whether the API has published a new dataset since the last run.
    source_date: String,
    entries: Vec<FuelEntry>,
}

/// A single fuel type whose yesterday-vs-today price changed.
#[derive(Debug, Clone, PartialEq)]
struct PriceChange {
    name: String,
    old_price: f64,
    new_price: f64,
}

impl PriceChange {
    fn difference(&self) -> f64 {
        (self.new_price - self.old_price).abs()
    }

    fn is_increase(&self) -> bool {
        self.new_price > self.old_price
    }
}

// ---- Bangchak wire format (only the fields we use) ---------------------------

/// Raw top-level element returned by the Bangchak API.
#[derive(Debug, Deserialize)]
struct BangchakWire {
    #[serde(rename = "OilPriceDate")]
    oil_price_date: String,
    #[serde(rename = "OilList")]
    oil_list: String,
}

#[derive(Debug, Deserialize)]
struct BangchakOilItem {
    #[serde(rename = "OilName")]
    oil_name: String,
    #[serde(rename = "PriceYesterday")]
    price_yesterday: f64,
    #[serde(rename = "PriceToday")]
    price_today: f64,
    #[serde(rename = "PriceTomorrow")]
    price_tomorrow: f64,
}

/// Result of fetching + parsing the Bangchak payload.
struct BangchakResponse {
    oil_price_date: String,
    items: Vec<FuelEntry>,
}

// ---- Line push payload -------------------------------------------------------

#[derive(Debug, Serialize)]
struct LinePushRequest<'a> {
    to: &'a str,
    messages: [LineMessage; 1],
}

#[derive(Debug, Serialize)]
struct LineMessage {
    #[serde(rename = "type")]
    message_type: &'static str,
    text: String,
}

// ---- Application -------------------------------------------------------------

struct AppConfig {
    state_file: PathBuf,
    line_target: Option<String>,
    line_token: Option<String>,
    http: Client,
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();

    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .with_target(true)
        .json()
        .init();

    let config = Arc::new(build_config()?);

    if config.line_target.is_none() || config.line_token.is_none() {
        warn!("LINE_CHANNEL_TOKEN or LINE_TARGET_ID not set — notifications disabled");
    }

    // Manual one-shot mode for ops/debugging: run a single tick and exit.
    if std::env::var_os("RUN_ONCE").is_some() {
        info!("RUN_ONCE set — executing a single tick and exiting");
        return run_once(&config).await;
    }

    info!(schedule = %SCHEDULE_UTC_CRON, "scheduling fuel price job");

    let mut scheduler = JobScheduler::new()
        .await
        .context("failed to create scheduler")?;

    let config_for_job = config.clone();
    let job = Job::new_async(SCHEDULE_UTC_CRON, move |_uuid, _l| {
        let config = config_for_job.clone();
        Box::pin(async move {
            if let Err(error) = run_once(&config).await {
                error!(error = %error, "scheduled run failed");
            }
        })
    })
    .context("failed to build job")?;

    scheduler.add(job).await.context("failed to add job")?;
    scheduler
        .start()
        .await
        .context("failed to start scheduler")?;

    // Keep the process alive until interrupted.
    tokio::signal::ctrl_c()
        .await
        .context("failed to listen for Ctrl+C")?;
    info!("shutdown signal received, exiting");
    scheduler
        .shutdown()
        .await
        .context("scheduler shutdown failed")?;
    Ok(())
}

fn build_config() -> Result<AppConfig> {
    let http = Client::builder()
        .user_agent(HTTP_USER_AGENT)
        .timeout(HTTP_TIMEOUT)
        .build()
        .context("failed to build HTTP client")?;

    let state_file = std::env::var("STATE_FILE")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from(STATE_FILE_NAME));

    Ok(AppConfig {
        state_file,
        line_target: std::env::var("LINE_TARGET_ID")
            .ok()
            .filter(|s| !s.is_empty()),
        line_token: std::env::var("LINE_CHANNEL_TOKEN")
            .ok()
            .filter(|s| !s.is_empty()),
        http,
    })
}

/// Single scheduled tick: fetch → compute changes → deduplicate → notify → persist.
async fn run_once(config: &AppConfig) -> Result<()> {
    let now = chrono::Utc::now().with_timezone(&Bangkok);
    info!(%now, "scheduled tick starting");

    let response = fetch_prices(&config.http).await?;
    let current = PriceSnapshot {
        captured_at: now.to_rfc3339(),
        source_date: response.oil_price_date.clone(),
        entries: response.items,
    };

    let changes = compute_changes(&current);

    let already_notified = match read_state(&config.state_file)? {
        Some(previous) => previous.source_date == current.source_date,
        None => false,
    };

    if changes.is_empty() {
        info!("no price changes detected — skipping notification");
    } else if already_notified {
        info!(
            source_date = %current.source_date,
            "already notified for this price date — skipping"
        );
    } else {
        info!(changed_count = changes.len(), "price changes detected");
        if let Err(error) = notify_line(config, &changes).await {
            warn!(error = %error, "LINE push failed — continuing to persist snapshot");
        }
    }

    write_state(&config.state_file, &current)?;
    info!(path = %config.state_file.display(), "snapshot persisted");
    Ok(())
}

// ---- Fetch -------------------------------------------------------------------

async fn fetch_prices(http: &Client) -> Result<BangchakResponse> {
    info!(url = BANGCHAK_API_URL, "fetching prices from Bangchak");
    let raw: Vec<BangchakWire> = http
        .get(BANGCHAK_API_URL)
        .send()
        .await
        .context("failed to send request to Bangchak")?
        .error_for_status()
        .context("Bangchak API returned non-success status")?
        .json()
        .await
        .context("failed to decode Bangchak JSON response")?;

    // API returns an array; we take the first (current) entry.
    let payload = raw
        .into_iter()
        .next()
        .context("Bangchak API returned empty payload")?;

    let items: Vec<BangchakOilItem> = serde_json::from_str(&payload.oil_list)
        .with_context(|| format!("failed to parse OilList JSON: {}", payload.oil_list))?;

    let entries = items
        .into_iter()
        .map(|item| FuelEntry {
            name: item.oil_name,
            price_yesterday: item.price_yesterday,
            price_today: item.price_today,
            price_tomorrow: item.price_tomorrow,
        })
        .collect::<Vec<_>>();

    Ok(BangchakResponse {
        oil_price_date: payload.oil_price_date,
        items: entries,
    })
}

// ---- Change detection --------------------------------------------------------

/// Compares each entry's `price_yesterday` against `price_today` and returns
/// those whose difference exceeds [`PRICE_CHANGE_THRESHOLD`].
///
/// The Bangchak API already exposes yesterday's price, so the comparison
/// is self-contained within the current snapshot — no historical file needed.
fn compute_changes(snapshot: &PriceSnapshot) -> Vec<PriceChange> {
    snapshot
        .entries
        .iter()
        .filter_map(|entry| {
            let delta = entry.price_today - entry.price_yesterday;
            if delta.abs() > PRICE_CHANGE_THRESHOLD {
                Some(PriceChange {
                    name: entry.name.clone(),
                    old_price: entry.price_yesterday,
                    new_price: entry.price_today,
                })
            } else {
                None
            }
        })
        .collect()
}

// ---- Notify ------------------------------------------------------------------

async fn notify_line(config: &AppConfig, changes: &[PriceChange]) -> Result<()> {
    let text = format_message(changes);

    // Dry-run mode: render the message that *would* be sent and stop.
    // Useful for verifying format without burning a LINE push.
    if std::env::var_os("DRY_RUN").is_some() {
        info!(
            dry_run = true,
            "skipping LINE push — message preview follows"
        );
        println!("{text}");
        return Ok(());
    }

    let (target, token) = match (&config.line_target, &config.line_token) {
        (Some(target), Some(token)) => (target.clone(), token.clone()),
        _ => {
            warn!("LINE credentials missing — skipping notification");
            return Ok(());
        }
    };

    let payload = LinePushRequest {
        to: &target,
        messages: [LineMessage {
            message_type: "text",
            text,
        }],
    };

    let response = config
        .http
        .post(LINE_PUSH_URL)
        .bearer_auth(token)
        .json(&payload)
        .send()
        .await
        .context("failed to send LINE push request")?;

    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        anyhow::bail!("LINE push failed: {status} — {body}");
    }

    info!(target = %target, "LINE notification sent");
    Ok(())
}

/// Builds the Thai-language notification body.
///
/// Layout:
/// ```text
/// 📰 แจ้งข่าวราคาน้ำมัน!!
///
/// <name> ปรับลด/เพิ่ม <diff> บาท
/// จากราคา <old> บาท เป็น <new> บาท
///
/// <name> ปรับลด/เพิ่ม <diff> บาท
/// จากราคา <old> บาท เป็น <new> บาท
/// ```
fn format_message(changes: &[PriceChange]) -> String {
    let mut body = Vec::with_capacity(changes.len());
    for change in changes {
        let direction = if change.is_increase() {
            DIRECTION_INCREASE
        } else {
            DIRECTION_DECREASE
        };
        body.push(format!(
            "{} ปรับ{} {:.2} บาท\nจากราคา {:.2} บาท เป็น {:.2} บาท",
            change.name,
            direction,
            change.difference(),
            change.old_price,
            change.new_price,
        ));
    }
    format!("{}\n\n{}", MESSAGE_HEADER, body.join("\n\n"))
}

// ---- State persistence -------------------------------------------------------

fn read_state(path: &PathBuf) -> Result<Option<PriceSnapshot>> {
    match fs::read_to_string(path) {
        Ok(contents) => {
            let snapshot: PriceSnapshot = serde_json::from_str(&contents)
                .with_context(|| format!("failed to parse state file at {}", path.display()))?;
            Ok(Some(snapshot))
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => {
            Err(error).with_context(|| format!("failed to read state file at {}", path.display()))
        }
    }
}

fn write_state(path: &PathBuf, snapshot: &PriceSnapshot) -> Result<()> {
    let serialized =
        serde_json::to_string_pretty(snapshot).context("failed to serialize snapshot")?;
    fs::write(path, serialized)
        .with_context(|| format!("failed to write state file at {}", path.display()))?;
    Ok(())
}

// ---- Tests -------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(name: &str, yesterday: f64, today: f64, tomorrow: f64) -> FuelEntry {
        FuelEntry {
            name: name.to_string(),
            price_yesterday: yesterday,
            price_today: today,
            price_tomorrow: tomorrow,
        }
    }

    fn snapshot(entries: Vec<FuelEntry>) -> PriceSnapshot {
        PriceSnapshot {
            captured_at: "2026-06-21T18:00:00+07:00".to_string(),
            source_date: "21/06/2026".to_string(),
            entries,
        }
    }

    #[test]
    fn compute_changes_detects_decrease() {
        let snap = snapshot(vec![entry("Gasohol 95", 31.00, 30.50, 30.50)]);
        let changes = compute_changes(&snap);
        assert_eq!(changes.len(), 1);
        assert_eq!(changes[0].name, "Gasohol 95");
        assert!(!changes[0].is_increase());
        assert!((changes[0].difference() - 0.50).abs() < f64::EPSILON);
    }

    #[test]
    fn compute_changes_detects_increase() {
        let snap = snapshot(vec![entry("Diesel B20", 32.50, 33.00, 33.00)]);
        let changes = compute_changes(&snap);
        assert_eq!(changes.len(), 1);
        assert!(changes[0].is_increase());
    }

    #[test]
    fn compute_changes_returns_empty_when_no_change() {
        let snap = snapshot(vec![entry("Diesel B20", 33.00, 33.00, 33.00)]);
        assert!(compute_changes(&snap).is_empty());
    }

    #[test]
    fn compute_changes_ignores_subthreshold_noise() {
        let snap = snapshot(vec![entry("Diesel B20", 33.0001, 33.0005, 33.0005)]);
        assert!(compute_changes(&snap).is_empty());
    }

    #[test]
    fn format_message_decrease_uses_lod() {
        let changes = vec![PriceChange {
            name: "Gasohol 95".to_string(),
            old_price: 31.00,
            new_price: 30.50,
        }];
        let message = format_message(&changes);
        assert!(message.starts_with("📰 แจ้งข่าวราคาน้ำมัน!!"));
        assert!(message.contains("Gasohol 95 ปรับลด 0.50 บาท"));
        assert!(message.contains("จากราคา 31.00 บาท เป็น 30.50 บาท"));
    }

    #[test]
    fn format_message_increase_uses_perm() {
        let changes = vec![PriceChange {
            name: "Diesel B20".to_string(),
            old_price: 32.50,
            new_price: 33.00,
        }];
        let message = format_message(&changes);
        assert!(message.contains("Diesel B20 ปรับเพิ่ม 0.50 บาท"));
        assert!(message.contains("จากราคา 32.50 บาท เป็น 33.00 บาท"));
    }

    #[test]
    fn format_message_separates_entries_with_blank_line() {
        let changes = vec![
            PriceChange {
                name: "Gasohol 95".to_string(),
                old_price: 31.00,
                new_price: 30.50,
            },
            PriceChange {
                name: "Diesel B20".to_string(),
                old_price: 32.50,
                new_price: 33.00,
            },
        ];
        let message = format_message(&changes);
        assert!(message.contains("\n\nDiesel B20"));
    }
}
