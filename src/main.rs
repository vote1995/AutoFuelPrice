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

/// Date line prefix shown below the header.
const DATE_LABEL: &str = "📅 ราคาปรับขึ้น-ลง วันที่";

/// Emoji bullet placed before each oil type name.
const OIL_TYPE_BULLET: &str = "⛽";

/// Arrow glyph drawn after the bullet, before the oil name.
const OIL_TYPE_ARROW: &str = "▶";

/// Thai word for a price decrease.
const DIRECTION_DECREASE: &str = "ลด";

/// Thai word for a price increase.
const DIRECTION_INCREASE: &str = "เพิ่ม";

/// English → Thai month names (index 0 = January).
const THAI_MONTHS: [&str; 12] = [
    "มกราคม",
    "กุมภาพันธ์",
    "มีนาคม",
    "เมษายน",
    "พฤษภาคม",
    "มิถุนายน",
    "กรกฎาคม",
    "สิงหาคม",
    "กันยายน",
    "ตุลาคม",
    "พฤศจิกายน",
    "ธันวาคม",
];

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
    /// Thai-formatted effective date parsed from `OilRemark2`
    /// (e.g. "19 มิถุนายน 2026"). Empty when the API omitted the remark.
    #[serde(default)]
    effective_date: String,
    /// Effective time parsed from `OilRemark2` (e.g. "05:00").
    #[serde(default)]
    effective_time: String,
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
    #[serde(rename = "OilRemark2", default)]
    oil_remark2: String,
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
    effective_date: String,
    effective_time: String,
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
///
/// State is only persisted when one of these is true:
/// - No price changes were detected (nothing to notify).
/// - We already notified for this `OilPriceDate` (idempotent re-write).
/// - The LINE push actually succeeded.
///
/// If credentials are missing or the push fails, state is NOT persisted,
/// so the next scheduled run will retry with the same `OilPriceDate`.
async fn run_once(config: &AppConfig) -> Result<()> {
    let now = chrono::Utc::now().with_timezone(&Bangkok);
    info!(%now, "scheduled tick starting");

    let response = fetch_prices(&config.http).await?;
    let current = PriceSnapshot {
        captured_at: now.to_rfc3339(),
        source_date: response.oil_price_date.clone(),
        effective_date: response.effective_date.clone(),
        effective_time: response.effective_time.clone(),
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

        // Guard the push: if credentials are missing, do NOT persist state —
        // otherwise the next run would think we already notified and skip.
        if config.line_target.is_none() || config.line_token.is_none() {
            warn!("LINE credentials missing — NOT persisting state, will retry next run");
            return Ok(());
        }

        if let Err(error) = notify_line(
            config,
            &current.effective_date,
            &current.effective_time,
            &changes,
        )
        .await
        {
            warn!(
                error = %error,
                "LINE push failed — NOT persisting state, will retry next run"
            );
            return Ok(());
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

    let (effective_date, effective_time) = parse_effective_datetime(&payload.oil_remark2);

    Ok(BangchakResponse {
        oil_price_date: payload.oil_price_date,
        effective_date,
        effective_time,
        items: entries,
    })
}

// ---- Effective date parsing ------------------------------------------------

/// Parses Bangchak's `OilRemark2` to extract the effective date/time.
///
/// Expected input shape:
/// `Are effective on 19 June 2026 from 05:00 AM`
///
/// Returns `(thai_date, time)` — e.g. `("19 มิถุนายน 2026", "05:00")`.
/// Returns `("", "")` if parsing fails so the caller can gracefully
/// omit the date line from the message.
fn parse_effective_datetime(remark: &str) -> (String, String) {
    const ENGLISH_MONTHS: [&str; 12] = [
        "January",
        "February",
        "March",
        "April",
        "May",
        "June",
        "July",
        "August",
        "September",
        "October",
        "November",
        "December",
    ];

    let words: Vec<&str> = remark.split_whitespace().collect();
    // Expected tokens:
    // ["Are", "effective", "on", "19", "June", "2026", "from", "05:00", "AM"]
    if words.len() < 9 {
        return (String::new(), String::new());
    }

    let day = match words[3].parse::<u32>() {
        Ok(value) => value,
        Err(_) => return (String::new(), String::new()),
    };
    let month_en = words[4];
    let year = match words[5].parse::<u32>() {
        Ok(value) => value,
        Err(_) => return (String::new(), String::new()),
    };
    let time = words[7];

    let month_idx = match ENGLISH_MONTHS.iter().position(|&m| m == month_en) {
        Some(idx) => idx,
        None => return (String::new(), String::new()),
    };
    let month_thai = THAI_MONTHS[month_idx];

    (format!("{} {} {}", day, month_thai, year), time.to_string())
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

async fn notify_line(
    config: &AppConfig,
    effective_date: &str,
    effective_time: &str,
    changes: &[PriceChange],
) -> Result<()> {
    let text = format_message(effective_date, effective_time, changes);

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
/// 📅 ราคาปรับขึ้น-ลง วันที่ <date> เวลา <time> น.
///
/// ⛽ ▶ <name>
/// ปรับลด/เพิ่ม <diff> บาท
/// จากราคา <old> บาท เป็น <new> บาท
///
/// ⛽ ▶ <name>
/// ปรับลด/เพิ่ม <diff> บาท
/// จากราคา <old> บาท เป็น <new> บาท
/// ```
///
/// When `effective_date` is empty (the API omitted `OilRemark2`),
/// the date line is omitted entirely.
fn format_message(effective_date: &str, effective_time: &str, changes: &[PriceChange]) -> String {
    let header = if effective_date.is_empty() {
        MESSAGE_HEADER.to_string()
    } else if effective_time.is_empty() {
        format!("{}\n{} {}", MESSAGE_HEADER, DATE_LABEL, effective_date)
    } else {
        format!(
            "{}\n{} {} เวลา {} น.",
            MESSAGE_HEADER, DATE_LABEL, effective_date, effective_time
        )
    };

    let mut body = Vec::with_capacity(changes.len());
    for change in changes {
        let direction = if change.is_increase() {
            DIRECTION_INCREASE
        } else {
            DIRECTION_DECREASE
        };
        body.push(format!(
            "{} {} {}\nปรับ{} {:.2} บาท\nจากราคา {:.2} บาท เป็น {:.2} บาท",
            OIL_TYPE_BULLET,
            OIL_TYPE_ARROW,
            change.name,
            direction,
            change.difference(),
            change.old_price,
            change.new_price,
        ));
    }
    format!("{}\n\n{}", header, body.join("\n\n"))
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
            effective_date: "19 มิถุนายน 2026".to_string(),
            effective_time: "05:00".to_string(),
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
        let message = format_message("19 มิถุนายน 2026", "05:00", &changes);
        assert!(message.starts_with("📰 แจ้งข่าวราคาน้ำมัน!!"));
        assert!(message.contains("📅 ราคาปรับขึ้น-ลง วันที่ 19 มิถุนายน 2026 เวลา 05:00 น."));
        assert!(message.contains("⛽ ▶ Gasohol 95"));
        assert!(message.contains("ปรับลด 0.50 บาท"));
        assert!(message.contains("จากราคา 31.00 บาท เป็น 30.50 บาท"));
    }

    #[test]
    fn format_message_increase_uses_perm() {
        let changes = vec![PriceChange {
            name: "Diesel B20".to_string(),
            old_price: 32.50,
            new_price: 33.00,
        }];
        let message = format_message("19 มิถุนายน 2026", "05:00", &changes);
        assert!(message.contains("⛽ ▶ Diesel B20"));
        assert!(message.contains("ปรับเพิ่ม 0.50 บาท"));
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
        let message = format_message("", "", &changes);
        assert!(message.contains("\n\n⛽ ▶ Diesel B20"));
    }

    #[test]
    fn format_message_omits_date_line_when_empty() {
        let changes = vec![PriceChange {
            name: "Gasohol 95".to_string(),
            old_price: 31.00,
            new_price: 30.50,
        }];
        let message = format_message("", "", &changes);
        assert!(message.starts_with("📰 แจ้งข่าวราคาน้ำมัน!!"));
        assert!(!message.contains("📅"));
    }

    #[test]
    fn parse_effective_datetime_extracts_thai_date_and_time() {
        let (date, time) = parse_effective_datetime("Are effective on 19 June 2026 from 05:00 AM");
        assert_eq!(date, "19 มิถุนายน 2026");
        assert_eq!(time, "05:00");
    }

    #[test]
    fn parse_effective_datetime_returns_empty_on_garbage_input() {
        let (date, time) = parse_effective_datetime("not a valid remark");
        assert!(date.is_empty());
        assert!(time.is_empty());
    }
}
